# frozen_string_literal: true

# Visual version of the demo.
#
#   bundle exec ruby ui.rb
#   open http://localhost:4567
#
# Each button in the page runs a real scenario here (real connection_pool gem, real
# sockets to mock_db_server.rb), records every step with its timestamp, and returns
# the timeline as JSON. The page replays that timeline as an animation.
#
# The HTTP server is a few lines on top of TCPServer so the demo keeps a single
# gem dependency (WEBrick is no longer part of Ruby's standard library).

require "bundler/setup"
require "connection_pool"
require "json"
require "socket"
require_relative "db_client"

UI_PORT         = Integer(ENV.fetch("UI_PORT", 4567))
HANDSHAKE_MS    = 25
MAX_CONNECTIONS = 10
WAIT_TIMEOUT_S  = 3

def start_db
  cmd = [RbConfig.ruby, File.join(__dir__, "mock_db_server.rb"), "0",
         HANDSHAKE_MS.to_s, MAX_CONNECTIONS.to_s, WAIT_TIMEOUT_S.to_s]
  io = IO.popen(cmd)
  port = io.gets[/:(\d+)/, 1].to_i
  at_exit { Process.kill("TERM", io.pid) rescue nil }
  port
end

DB_PORT = start_db

def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

# Collects timestamped events from many threads for one phase of a scenario.
class Recorder
  attr_reader :events

  def initialize
    @t0 = now
    @events = []
    @lock = Mutex.new
  end

  def ev(type, msg, **data)
    @lock.synchronize { @events << { t: ((now - @t0) * 1000).round(1), ev: type, msg: msg, **data } }
  end

  def elapsed_ms = ((now - @t0) * 1000).round(1)
end

# Runs one phase and packages its timeline with the database's own counters.
def phase(title, mode:, pool_size: nil, timeout: nil)
  DbClient.admin(DB_PORT, "RESET_STATS")
  rec = Recorder.new
  yield rec
  stats = DbClient.admin(DB_PORT, "STATS").split.to_h { |kv| k, v = kv.split("="); [k, v.to_i] }
  {
    title: title, mode: mode, pool_size: pool_size, timeout: timeout, elapsed_ms: rec.elapsed_ms,
    db: stats, events: rec.events,
    results: rec.events.map { |e| e[:ev] }.tally.slice("done", "refused", "timeout", "failed")
  }
end

def new_pool(rec, size:, timeout:)
  ConnectionPool.new(size: size, timeout: timeout) do
    req = Thread.current[:req]
    rec.ev("connect", "#{req}: no free connection in the pool and room to grow, so it opens a new one", req: req)
    conn = DbClient.new(port: DB_PORT)
    rec.ev("connected", "#{req}: connection ##{conn.id} is open (handshake done)", req: req, conn: conn.id)
    conn
  end
end

def drain(pool) = pool.shutdown(&:close)

def direct_request(rec, req, query_ms)
  rec.ev("arrive", "#{req} arrives", req: req)
  rec.ev("connect", "#{req}: dials the database itself (handshake)", req: req)
  conn = DbClient.new(port: DB_PORT)
  rec.ev("connected", "#{req}: connection ##{conn.id} is open", req: req, conn: conn.id)
  rec.ev("acquire", "#{req}: runs its query on connection ##{conn.id}", req: req, conn: conn.id)
  conn.query(query_ms)
  rec.ev("done", "#{req}: query finished", req: req, conn: conn.id)
  conn.close
  rec.ev("close", "#{req}: hangs up connection ##{conn.id}", req: req, conn: conn.id)
rescue DbClient::TooManyConnections
  rec.ev("refused", "#{req}: REFUSED by the database (too many connections)", req: req)
end

def pooled_request(rec, pool, req, query_ms, seen)
  Thread.current[:req] = req
  rec.ev("arrive", "#{req} arrives and asks the pool for a connection", req: req)
  rec.ev("checkout", "#{req}: waiting for a connection", req: req)
  pool.with do |conn|
    reused = !seen.add?(conn.id)
    rec.ev("acquire", reused ? "#{req}: reuses connection ##{conn.id} (no handshake)" : "#{req}: uses connection ##{conn.id}",
           req: req, conn: conn.id, reused: reused)
    conn.query(query_ms)
    rec.ev("done", "#{req}: query finished", req: req, conn: conn.id)
  ensure
    # Logged inside the block so it always lands before the next borrower's acquire.
    rec.ev("release", "#{req}: gives connection ##{conn.id} back to the pool", req: req, conn: conn.id)
  end
rescue ConnectionPool::TimeoutError
  rec.ev("timeout", "#{req}: gave up after waiting #{pool_timeout_label(pool)} (checkout timeout)", req: req)
end

def pool_timeout_label(pool) = "#{pool.instance_variable_get(:@timeout)}s"

def concurrently(count)
  Array.new(count) { |i| Thread.new { yield "R#{i + 1}", i } }.each(&:join)
end

SCENARIOS = {
  "reuse" => {
    title: "Reuse: direct vs pool",
    what: "20 requests run one after another. Without a pool, each one dials the database, " \
          "asks one quick question and hangs up. With a pool, the first request opens a " \
          "connection and every request after it reuses that same one.",
    watch: "Count the amber 'connecting' moments and the Opened total in the Database column.",
    run: lambda do
      direct = phase("Without a pool", mode: "direct") do |rec|
        20.times { |i| direct_request(rec, "R#{i + 1}", 2) }
      end
      pooled = phase("With a pool of 5", mode: "pool", pool_size: 5, timeout: 1) do |rec|
        pool = new_pool(rec, size: 5, timeout: 1)
        seen = Set.new
        20.times { |i| pooled_request(rec, pool, "R#{i + 1}", 2, seen) }
        drain(pool)
      end
      [direct, pooled]
    end,
    takeaway: "Same 20 queries. The pool skips the handshake on every request after the first, " \
              "so it finishes many times faster and the database opens one connection instead of 20."
  },
  "burst" => {
    title: "Burst: pool limits",
    what: "30 requests arrive at the same moment. The database accepts at most " \
          "#{MAX_CONNECTIONS} connections. Each query takes 300 ms.",
    watch: "Red refused requests without a pool. With a pool, a queue forms instead and everyone gets served.",
    run: lambda do
      direct = phase("Without a pool", mode: "direct") do |rec|
        concurrently(30) { |req| direct_request(rec, req, 300) }
      end
      pooled = phase("With a pool of 5", mode: "pool", pool_size: 5, timeout: 5) do |rec|
        pool = new_pool(rec, size: 5, timeout: 5)
        seen = Set.new
        concurrently(30) { |req| pooled_request(rec, pool, req, 300, seen) }
        drain(pool)
      end
      [direct, pooled]
    end,
    takeaway: "Without a pool the database hits its limit and turns requests away. The pool caps " \
              "the database at 5 connections and makes the rest wait their turn: slower for some, " \
              "but nobody is refused."
  },
  "timeout" => {
    title: "Timeout: waiting has a limit",
    what: "A pool of only 2 connections and a checkout timeout of 0.5 s. " \
          "5 requests arrive together and each one holds its connection for 1.5 s.",
    watch: "Two requests get a connection. The other three wait, then fail at 0.5 s instead of hanging.",
    run: lambda do
      [phase("Pool of 2, timeout 0.5 s", mode: "pool", pool_size: 2, timeout: 0.5) do |rec|
        pool = new_pool(rec, size: 2, timeout: 0.5)
        seen = Set.new
        concurrently(5) do |req, i|
          sleep(i * 0.02) # stable arrival order so the animation reads left to right
          pooled_request(rec, pool, req, 1500, seen)
        end
        drain(pool)
      end]
    end,
    takeaway: "A quick, clear error is better than requests piling up forever. The app can retry, " \
              "show a friendly message, or shed load."
  },
  "stale" => {
    title: "Stale connection: validation",
    what: "The database closes connections that sit idle longer than #{WAIT_TIMEOUT_S} s. " \
          "The pool does not notice. We use a connection, leave it idle, then use it again.",
    watch: "Without validation the second request gets a dead connection. With verify! it reconnects first.",
    run: lambda do
      stale = lambda do |title, validate|
        phase(title, mode: "pool", pool_size: 1, timeout: 1) do |rec|
          pool = new_pool(rec, size: 1, timeout: 1)
          seen = Set.new
          pooled_request(rec, pool, "R1", 5, seen)
          rec.ev("note", "The connection now sits idle for #{WAIT_TIMEOUT_S + 0.5} s, longer than the " \
                         "database's #{WAIT_TIMEOUT_S} s idle timeout...")
          sleep WAIT_TIMEOUT_S + 0.5
          rec.ev("server_closed", "The database has quietly closed idle connection ##{seen.first}. " \
                                  "The pool still thinks it is fine.", conn: seen.first)
          Thread.current[:req] = "R2"
          rec.ev("arrive", "R2 arrives and asks the pool for a connection", req: "R2")
          rec.ev("checkout", "R2: waiting for a connection", req: "R2")
          pool.with do |conn|
            old_id = conn.id
            rec.ev("acquire", "R2: gets connection ##{old_id} from the pool", req: "R2", conn: old_id)
            if validate
              conn.verify!
              if conn.id == old_id
                rec.ev("note", "R2: verify! pinged connection ##{old_id} and it answered")
              else
                rec.ev("stale", "R2: verify! pinged connection ##{old_id}: no answer, it is dead", req: "R2", conn: old_id)
                rec.ev("reconnected", "R2: verify! reconnected, now on connection ##{conn.id}",
                       req: "R2", conn: conn.id, old: old_id)
              end
            end
            conn.query(5)
            rec.ev("done", "R2: query finished", req: "R2", conn: conn.id)
            rec.ev("release", "R2: gives connection ##{conn.id} back to the pool", req: "R2", conn: conn.id)
          rescue DbClient::ConnectionLost => e
            rec.ev("stale", "R2: connection ##{old_id} is dead", req: "R2", conn: old_id)
            rec.ev("failed", "R2: FAILED with \"#{e.message}\"", req: "R2")
            pool.discard_current_connection(&:close)
          end
          drain(pool)
        end
      end
      [stale.call("Without validation", false), stale.call("With verify! on checkout", true)]
    end,
    takeaway: "Check a connection before handing it out, and reconnect if it is dead. " \
              "Rails does this with verify!."
  }
}.freeze

RUN_LOCK = Mutex.new # scenarios share the database's counters, so run one at a time

def run_scenario(name)
  s = SCENARIOS.fetch(name)
  phases = RUN_LOCK.synchronize { s[:run].call }
  { name: name, title: s[:title], what: s[:what], watch: s[:watch], takeaway: s[:takeaway],
    max_connections: MAX_CONNECTIONS, handshake_ms: HANDSHAKE_MS, phases: phases }
end

def respond(client, status, type, body)
  client.write("HTTP/1.1 #{status}\r\nContent-Type: #{type}\r\nContent-Length: #{body.bytesize}\r\n" \
               "Cache-Control: no-store\r\nConnection: close\r\n\r\n#{body}")
end

def handle(client)
  request_line = client.gets.to_s
  nil while (line = client.gets) && line != "\r\n" # skip headers
  _verb, target = request_line.split
  path, query = target.to_s.split("?", 2)
  params = URI.decode_www_form(query.to_s).to_h

  case path
  when "/", "/index.html"
    respond(client, "200 OK", "text/html; charset=utf-8", File.read(File.join(__dir__, "public", "index.html")))
  when "/api/scenarios"
    list = SCENARIOS.map { |k, v| { name: k, title: v[:title], what: v[:what], watch: v[:watch] } }
    respond(client, "200 OK", "application/json", JSON.generate(list))
  when "/api/run"
    if SCENARIOS.key?(params["name"])
      respond(client, "200 OK", "application/json", JSON.generate(run_scenario(params["name"])))
    else
      respond(client, "404 Not Found", "application/json", '{"error":"unknown scenario"}')
    end
  else
    respond(client, "404 Not Found", "text/plain", "not found")
  end
rescue StandardError => e
  respond(client, "500 Internal Server Error", "text/plain", "#{e.class}: #{e.message}") rescue nil
ensure
  client.close
end

require "uri"
require "set"

server = TCPServer.new("127.0.0.1", UI_PORT)
puts "Connection pooling UI: http://localhost:#{UI_PORT}  (mock database on port #{DB_PORT}, Ctrl+C to stop)"
trap("INT") { exit }
loop { Thread.new(server.accept) { |client| handle(client) } }
