# frozen_string_literal: true

require "socket"

# Minimal client for mock_db_server.rb. Shaped like a real driver connection
# (mysql2, pg): constructing it opens a socket and authenticates, and it exposes
# ping and verify! the same way ActiveRecord adapters do.
class DbClient
  class Error < StandardError; end
  class TooManyConnections < Error; end
  class ConnectionLost < Error; end

  attr_reader :id, :reconnects

  def initialize(host: "127.0.0.1", port:)
    @host = host
    @port = port
    @reconnects = 0
    connect!
  end

  def query(ms = 1) = call("QUERY #{ms}")

  def call(line)
    @socket.write("#{line}\n")
    reply = @socket.gets
    raise ConnectionLost, "server has gone away (connection #{@id})" if reply.nil?

    reply.chomp
  rescue Errno::EPIPE, Errno::ECONNRESET, IOError
    raise ConnectionLost, "server has gone away (connection #{@id})"
  end

  def alive?
    call("PING") == "PONG"
  rescue Error
    false
  end

  # Validation on checkout: ping, and transparently reconnect if the server
  # closed the socket. This is what ActiveRecord's verify! does.
  def verify!
    return if alive?

    close
    connect!
    @reconnects += 1
  end

  def close
    @socket.write("QUIT\n")
  rescue StandardError
    nil
  ensure
    @socket.close unless @socket.closed?
  end

  def self.admin(port, line)
    socket = TCPSocket.new("127.0.0.1", port)
    socket.write("#{line}\n")
    socket.gets&.chomp
  ensure
    socket&.close
  end

  private

  def connect!
    @socket = TCPSocket.new(@host, @port)
    @socket.write("HELLO demo secret\n")
    reply = @socket.gets&.chomp
    case reply
    when /\AOK (\d+)\z/
      @id = Regexp.last_match(1).to_i
    when "ERR too_many_connections"
      @socket.close
      raise TooManyConnections, "server refused: too many connections"
    else
      @socket.close
      raise Error, "handshake failed: #{reply.inspect}"
    end
  end
end
