# frozen_string_literal: true

# A tiny TCP "database" server used by the connection pooling demo.
#
# Why a mock instead of MySQL or PostgreSQL: the demo must be reproducible by any
# reviewer with nothing but Ruby installed, no credentials and no database setup.
# The server models the four properties of a real database that make pooling matter:
#
#   1. Opening a connection is expensive (TCP + TLS + authentication), simulated
#      with a configurable handshake delay.
#   2. The server has a hard max_connections limit and refuses extra clients,
#      like MySQL's "Too many connections" error.
#   3. Idle connections are closed after wait_timeout seconds, like MySQL's
#      wait_timeout, which is what produces stale connections in a pool.
#   4. Every connection has session state (variables, open transaction) that
#      survives for the lifetime of that connection.
#
# Protocol (one line per request, one line per reply):
#   HELLO <user> <password>  -> OK <connection_id> | ERR too_many_connections
#   QUERY <ms>               -> ROW <connection_id>   (sleeps ms to simulate work)
#   PING                     -> PONG
#   SET <key> <value>        -> OK
#   GET <key>                -> VALUE <value> | VALUE NULL
#   BEGIN | COMMIT | ROLLBACK -> OK
#   STATUS                   -> TX open | TX none
#   RESET                    -> OK   (rolls back and clears session state)
#   QUIT                     -> BYE
# Admin lines sent as the first line instead of HELLO (never counted as clients):
#   STATS                    -> accepted=N active=N peak=N refused=N
#   RESET_STATS              -> OK
#
# Usage: ruby mock_db_server.rb [port] [handshake_ms] [max_connections] [wait_timeout_s]

require "socket"

class MockDbServer
  def initialize(port:, handshake_ms:, max_connections:, wait_timeout:)
    @server = TCPServer.new("127.0.0.1", port)
    @handshake = handshake_ms / 1000.0
    @max_connections = max_connections
    @wait_timeout = wait_timeout
    @lock = Mutex.new
    reset_stats
  end

  def port = @server.addr[1]

  def run
    loop do
      socket = @server.accept
      Thread.new(socket) { |s| handle(s) }
    end
  end

  private

  def reset_stats
    @next_id = 0
    @active = 0
    @peak = 0
    @refused = 0
  end

  def handle(socket)
    first = socket.gets&.chomp
    return admin(socket, first) unless first&.start_with?("HELLO")

    # The expensive part of every new connection: TCP setup, TLS and auth.
    sleep @handshake
    id = admit
    unless id
      socket.write("ERR too_many_connections\n")
      return
    end

    begin
      socket.write("OK #{id}\n")
      session(socket, id)
    ensure
      @lock.synchronize { @active -= 1 }
    end
  rescue IOError, SystemCallError
    nil
  ensure
    socket.close unless socket.closed?
  end

  # Returns a new connection id, or nil when the server is full.
  def admit
    @lock.synchronize do
      if @active >= @max_connections
        @refused += 1
        nil
      else
        @active += 1
        @peak = [@peak, @active].max
        @next_id += 1
      end
    end
  end

  def admin(socket, line)
    case line
    when "STATS"
      stats = @lock.synchronize { "accepted=#{@next_id} active=#{@active} peak=#{@peak} refused=#{@refused}" }
      socket.write("#{stats}\n")
    when "RESET_STATS"
      @lock.synchronize { reset_stats }
      socket.write("OK\n")
    end
  end

  def session(socket, id)
    vars = {}
    in_tx = false

    loop do
      # Server side idle timeout. A pooled connection that sits unused longer than
      # this is silently closed, and the client only finds out on its next write.
      return unless IO.select([socket], nil, nil, @wait_timeout)

      line = socket.gets&.chomp
      return if line.nil?

      cmd, *args = line.split(" ")
      reply =
        case cmd
        when "QUERY" then sleep(args[0].to_i / 1000.0); "ROW #{id}"
        when "PING" then "PONG"
        when "SET" then vars[args[0]] = args[1]; "OK"
        when "GET" then "VALUE #{vars.fetch(args[0], 'NULL')}"
        when "BEGIN" then in_tx = true; "OK"
        when "COMMIT", "ROLLBACK" then in_tx = false; "OK"
        when "STATUS" then in_tx ? "TX open" : "TX none"
        when "RESET" then in_tx = false; vars.clear; "OK"
        when "QUIT" then socket.write("BYE\n"); return
        else "ERR unknown_command"
        end
      socket.write("#{reply}\n")
    end
  end
end

if $PROGRAM_NAME == __FILE__
  port, handshake_ms, max_connections, wait_timeout = ARGV
  server = MockDbServer.new(
    port: (port || 7400).to_i,
    handshake_ms: (handshake_ms || 25).to_i,
    max_connections: (max_connections || 20).to_i,
    wait_timeout: (wait_timeout || 2).to_f
  )
  $stdout.puts "mock db listening on 127.0.0.1:#{server.port}"
  $stdout.flush
  server.run
end
