# frozen_string_literal: true

# Connection pooling demo: direct connections versus a configured pool.
#
#   bundle install
#   bundle exec ruby demo.rb
#
# Starts mock_db_server.rb as a separate process on a random local port, then runs
# five scenarios against it. See README.md for what each scenario proves.

require "bundler/setup"
require "connection_pool"
require_relative "db_client"

HANDSHAKE_MS    = Integer(ENV.fetch("HANDSHAKE_MS", 25))   # cost of opening one connection
MAX_CONNECTIONS = Integer(ENV.fetch("MAX_CONNECTIONS", 20)) # server side hard limit
WAIT_TIMEOUT_S  = Float(ENV.fetch("WAIT_TIMEOUT_S", 2))     # server closes idle connections after this

def start_server
  cmd = [RbConfig.ruby, File.join(__dir__, "mock_db_server.rb"), "0",
         HANDSHAKE_MS.to_s, MAX_CONNECTIONS.to_s, WAIT_TIMEOUT_S.to_s]
  io = IO.popen(cmd)
  port = io.gets[/:(\d+)/, 1].to_i
  at_exit { Process.kill("TERM", io.pid) rescue nil }
  port
end

PORT = start_server

def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

def timed
  start = now
  result = yield
  [result, now - start]
end

def ms(seconds) = format("%7.1f ms", seconds * 1000)

def header(title)
  puts
  puts "=" * 78
  puts title
  puts "=" * 78
end

def server_stats = DbClient.admin(PORT, "STATS")
def reset_server_stats = DbClient.admin(PORT, "RESET_STATS")

def new_pool(size:, timeout:)
  ConnectionPool.new(size: size, timeout: timeout) { DbClient.new(port: PORT) }
end

def drain(pool) = pool.shutdown(&:close)

puts "Mock database on 127.0.0.1:#{PORT}  handshake=#{HANDSHAKE_MS}ms  " \
     "max_connections=#{MAX_CONNECTIONS}  wait_timeout=#{WAIT_TIMEOUT_S}s"

# --------------------------------------------------------------------------
header "1. Direct connections vs pool: 200 sequential queries"
# --------------------------------------------------------------------------
QUERIES = 200

reset_server_stats
ids, direct_time = timed do
  Array.new(QUERIES) do
    conn = DbClient.new(port: PORT)   # open: TCP + auth every single time
    conn.query(1)
    conn.close.then { conn.id }
  end
end
direct_stats = server_stats

reset_server_stats
pool = new_pool(size: 5, timeout: 1)
pooled_ids, pooled_time = timed do
  Array.new(QUERIES) { pool.with { |conn| conn.query(1)[/\d+/].to_i } }
end
pooled_stats = server_stats
drain(pool)

puts format("%-10s total %s   per query %s   distinct connections %4d",
            "direct", ms(direct_time), ms(direct_time / QUERIES), ids.uniq.size)
puts format("%-10s total %s   per query %s   distinct connections %4d",
            "pooled", ms(pooled_time), ms(pooled_time / QUERIES), pooled_ids.uniq.size)
puts "server (direct): #{direct_stats}"
puts "server (pooled): #{pooled_stats}"
puts format("=> pool was %.1fx faster; one connection was reused for all %d queries " \
            "(a single thread only ever needs one)", direct_time / pooled_time, QUERIES)

# --------------------------------------------------------------------------
header "2. Pool limits under a burst: 60 concurrent requests, server max #{MAX_CONNECTIONS}"
# --------------------------------------------------------------------------
BURST = 60

reset_server_stats
results = Array.new(BURST) do
  Thread.new do
    conn = DbClient.new(port: PORT)
    conn.query(100)
    conn.close
    :ok
  rescue DbClient::TooManyConnections
    :refused
  end
end.map(&:value)
puts format("direct:  ok=%2d refused=%2d   server: %s",
            results.count(:ok), results.count(:refused), server_stats)

reset_server_stats
pool = new_pool(size: 10, timeout: 5)
results, burst_time = timed do
  Array.new(BURST) do
    Thread.new do
      pool.with { |conn| conn.query(100) }
      :ok
    rescue ConnectionPool::TimeoutError
      :timeout
    end
  end.map(&:value)
end
puts format("pooled:  ok=%2d timeout=%2d  server: %s   wall %s",
            results.count(:ok), results.count(:timeout), server_stats, ms(burst_time))
puts "monitor: pool.size=#{pool.size} available=#{pool.available} idle=#{pool.idle}"
puts "=> the pool caps the database at 10 connections; the other 50 requests queue " \
     "in the app (backpressure) instead of being refused by the server"
drain(pool)

# --------------------------------------------------------------------------
header "3. Checkout timeout: pool size 2, timeout 0.3s, 5 requests each holding 1s"
# --------------------------------------------------------------------------
pool = new_pool(size: 2, timeout: 0.3)
t0 = now
log = Queue.new
threads = Array.new(5) do |i|
  Thread.new do
    sleep(i * 0.01) # stable arrival order for readable output
    pool.with do |conn|
      log << format("+%5.0f ms  request %d  acquired connection %d", (now - t0) * 1000, i, conn.id)
      conn.query(1000)
    end
    log << format("+%5.0f ms  request %d  released", (now - t0) * 1000, i)
  rescue ConnectionPool::TimeoutError => e
    log << format("+%5.0f ms  request %d  TIMEOUT  %s: %s", (now - t0) * 1000, i, e.class, e.message)
  end
end
threads.each(&:join)
lines = []
lines << log.pop until log.empty?
lines.sort_by { |l| l[/[\d.]+/].to_f }.each { |l| puts l }
puts "=> two requests got a connection; the rest failed fast after 0.3s instead of " \
     "waiting forever. That error is the signal to shed load or retry with backoff."
drain(pool)

# --------------------------------------------------------------------------
header "4. Stale connections and validation (server wait_timeout #{WAIT_TIMEOUT_S}s)"
# --------------------------------------------------------------------------
pool = new_pool(size: 1, timeout: 1)
pool.with { |conn| puts "warm up: connection #{conn.id} answered #{conn.query(1)}" }
puts "sleeping #{WAIT_TIMEOUT_S + 0.5}s so the server closes the idle pooled connection..."
sleep WAIT_TIMEOUT_S + 0.5

begin
  pool.with { |conn| conn.query(1) }
rescue DbClient::ConnectionLost => e
  puts "without validation: #{e.class}: #{e.message}"
end

pool.with do |conn|
  conn.verify! # ping on checkout, reconnect if dead
  puts "with validation:    verify! reconnected (reconnects=#{conn.reconnects}), " \
       "now on connection #{conn.id}, answered #{conn.query(1)}"
end

puts "idle reaping: idle=#{pool.idle} before reap"
sleep 0.2
pool.reap(idle_seconds: 0.1, &:close)
puts "idle reaping: idle=#{pool.idle} after reap(idle_seconds: 0.1), the next checkout opens a fresh connection"
puts "=> validate on checkout (or reap idle connections before the server's timeout) " \
     "so requests never receive a dead socket"
drain(pool)

# --------------------------------------------------------------------------
header "5. Transaction and session state leaking between borrowers (pool size 1)"
# --------------------------------------------------------------------------
def request_a(pool)
  pool.with do |conn|
    conn.call("BEGIN")
    conn.call("SET tenant_id 42")
    raise "boom: request A crashed mid transaction"
  end
rescue RuntimeError => e
  puts "request A: #{e.message}"
end

def request_b(pool)
  pool.with do |conn|
    puts "request B: connection #{conn.id}  #{conn.call('STATUS')}  tenant_id=#{conn.call('GET tenant_id').delete_prefix('VALUE ')}"
  end
end

pool = new_pool(size: 1, timeout: 1)
puts "naive pool:"
request_a(pool)
request_b(pool)
drain(pool)

# Fix: always reset the connection on release, whatever happened in the block.
module CleanCheckin
  def with(**)
    super do |conn|
      yield conn
    ensure
      conn.call("RESET") # rollback open transaction + clear session variables
    end
  end
end

pool = new_pool(size: 1, timeout: 1).extend(CleanCheckin)
puts "pool with reset on release:"
request_a(pool)
request_b(pool)
puts "=> a pool must hand out connections in a known clean state: roll back open " \
     "transactions and reset session state on release"
drain(pool)

puts
puts "Done."
