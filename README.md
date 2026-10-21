# Connection Pooling Demo

A small Ruby application that compares repeated direct connections with a configured connection pool, and shows pool limits, reuse, timeouts and stale connection handling.

## What is in this folder

| File | Purpose |
|---|---|
| `mock_db_server.rb` | A tiny TCP "database" server, started automatically by the demo as a separate process. It simulates an expensive connection handshake, a hard `max_connections` limit and an idle `wait_timeout`. |
| `db_client.rb` | A minimal driver for that server, shaped like `mysql2` or `pg`: constructing it opens and authenticates a socket. It exposes `ping`, `alive?` and `verify!` (ping and reconnect, like ActiveRecord). |
| `demo.rb` | Runs the four scenarios and prints the results. |
| `ui.rb`, `public/index.html` | Visual version: a local web page that runs each scenario for real and replays it as an animation. |
| `Gemfile`, `Gemfile.lock` | One dependency: [`connection_pool`](https://github.com/mperham/connection_pool), the pool used by Sidekiq, Dalli and the redis gem. |
| `evidence/demo-output.txt` | Captured output of a full run. |
| `evidence/environment.txt` | Ruby, OS and gem versions used for the captured run. |
| `evidence/ui-*.png` | Screenshots of the visual version, one per scenario, at the end of a run. |

Why a mock server instead of MySQL or PostgreSQL: anyone can reproduce the run with only Ruby installed, without credentials, Docker or a database. The mock models the exact properties that make pooling matter (expensive connect, server connection limit, idle timeout). The pool itself is the real, production grade `connection_pool` gem.

## Setup

Requirements: Ruby 3.2 or newer and Bundler.

```bash
git clone https://github.com/Shakil-Akbar/connection-pooling.git
cd connection-pooling
bundle install
```

## Run

```bash
bundle exec ruby demo.rb
```

A full run takes about 12 seconds. Optional knobs (environment variables):

```bash
HANDSHAKE_MS=50 MAX_CONNECTIONS=10 WAIT_TIMEOUT_S=1 bundle exec ruby demo.rb
```

| Variable | Default | Meaning |
|---|---|---|
| `HANDSHAKE_MS` | 25 | Simulated cost of opening one connection (TCP, TLS and authentication). 20 to 50 ms is typical for a TLS connection to a managed database in another availability zone. |
| `MAX_CONNECTIONS` | 20 | Server side connection limit. Extra clients are refused, like MySQL's "Too many connections". |
| `WAIT_TIMEOUT_S` | 2 | Server closes connections idle longer than this, like MySQL `wait_timeout`. |

## Visual version

```bash
bundle exec ruby ui.rb
```

Then open http://localhost:4567 (set `UI_PORT` to use another port). Pick a scenario and press **Run this scenario**.

Each run executes for real on the server (the same `connection_pool` gem and mock database, with a limit of 10 connections and a 3 second idle timeout) and records every step with its timestamp. The page then replays those steps in three columns:

* **Requests**: one box per request, colored by what it is doing (waiting, opening a connection, running its query, done, or refused, timed out or failed).
* **Pool**: the pool's connection slots and the queue of requests waiting for one. In the "without a pool" phases this column shows the connections each request opens for itself.
* **Database**: open connections right now against the limit, connections opened in total, and refused requests.

Below that, a plain English log explains each step, and a results table compares the phases using the database's own counters. Both phases of a scenario play on the same time scale, so a faster phase really looks faster. Two adjustments keep it readable: idle waits over 2 seconds are shortened, and scenarios with only a few steps play one step at a time.

To capture a scenario's final state without the animation (used for the screenshots in `evidence/`), open `http://localhost:4567/?scenario=burst&autorun=1&instant=1`.

## Scenarios and expected results

Exact timings vary by machine. The shape of the results should not.

### 1. Direct connections vs pool (reuse)

200 sequential 1 ms queries. Direct mode opens and closes a connection for each query; pooled mode borrows from a pool of size 5.

Expected: direct opens **200** server connections and spends most of its time in handshakes (about 28 ms per query). The pool opens **1** connection and reuses it for every query (about 2 ms per query). Roughly **15x faster** with the default 25 ms handshake. Only one connection is created because a single thread never needs more than one at a time, and the pool creates connections lazily.

### 2. Pool limits under a burst (backpressure)

60 concurrent requests, each running a 100 ms query, against a server limited to 20 connections.

Expected: direct mode gets **20 ok, 40 refused** by the server. Pooled mode (size 10) gets **60 ok, 0 refused**, the server never sees more than **10** connections (`peak=10`), and the extra requests wait in the application queue. Total wall time is about 6 waves of 100 ms plus handshakes, under one second.

### 3. Checkout timeout

Pool size 2, checkout timeout 0.3 s, five requests that each hold a connection for 1 s.

Expected: requests 0 and 1 acquire connections immediately. Requests 2, 3 and 4 raise `ConnectionPool::TimeoutError: Waited 0.3 sec, 0/2 available` at about 300 ms instead of hanging. Requests 0 and 1 release at about 1 s.

### 4. Stale connections and validation

Warm a pool of size 1, then sleep past the server's `wait_timeout` so the server closes the idle socket.

Expected: the next query without validation fails with `DbClient::ConnectionLost: server has gone away`. With `verify!` on checkout, the client detects the dead socket via ping, reconnects (`reconnects=1`, new connection id) and the query succeeds. `pool.reap(idle_seconds:)` then closes idle connections proactively (`idle=1` becomes `idle=0`).

## Mapping to real stacks

| Demo concept | Rails / ActiveRecord | Other stacks |
|---|---|---|
| `ConnectionPool.new(size:)` | `pool:` in `database.yml` | HikariCP `maximumPoolSize`, pgxpool `MaxConns` |
| `timeout:` | `checkout_timeout` (raises `ActiveRecord::ConnectionTimeoutError`) | HikariCP `connectionTimeout` |
| `verify!` | `ActiveRecord::ConnectionAdapters::AbstractAdapter#verify!` | HikariCP `keepaliveTime`, pgbouncer `server_check_query` |
| `reap(idle_seconds:)` | `idle_timeout` and the pool reaper (`reaping_frequency`) | HikariCP `idleTimeout`, `maxLifetime` |
