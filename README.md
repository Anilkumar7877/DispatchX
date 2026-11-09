# 🚨 RedAlert - Real-Time Ambulance Dispatch & Telemetry System

A high-performance emergency ambulance dispatch and live fleet tracking backend built with **FastAPI** and **Redis Stack** (`redis-py`).

RedAlert demonstrates how Redis native data structures cleanly model complex real-time distributed problems: geospatial indexing, priority queues, set-based availability, live Pub/Sub streaming via WebSockets, and self-healing keyspace notifications.

---

## 🌟 Key Architecture & Redis Data Structures

| Feature | Redis Commands | Data Structure | Purpose |
| :--- | :--- | :--- | :--- |
| **Fleet Registry** | `HSET`, `HGETALL`, `EXISTS` | **Hash** (`unit:{id}`) | Stores vehicle metadata (`type`, `status`, `driver`). |
| **Liveness Heartbeat** | `SET ... EX 10`, `TTL` | **String with TTL** (`hb:unit:{id}`) | 10-second ephemeral liveness indicator. |
| **Geospatial Tracking** | `GEOADD`, `GEOSEARCH`, `ZREM` | **Sorted Set (Geo)** (`units:geo`) | Coordinates stored as 52-bit Geohashes for proximity queries. |
| **Availability & Zones** | `SADD`, `SREM`, `SMOVE`, `SINTER` | **Set** (`units:free`, `units:busy`, `zone:{zone}:units`) | Atomic $O(1)$ availability transitions and in-memory zone intersection. |
| **Priority Queue** | `INCR`, `ZADD`, `ZPOPMAX`, `ZRANGE` | **Sorted Set** (`incidents:queue`) | Auto-incrementing IDs and priority dispatch by severity (1–10). |
| **Telemetry & Alerts** | `PUBLISH`, `SUBSCRIBE` | **Pub/Sub** (`unit:updates`, `incident:updates`) | Sub-millisecond event streaming pushed to WebSocket clients. |
| **Auto-Purge Offline** | `CONFIG SET`, `__keyevent@0__:expired` | **Keyspace Notifications** | Purges dead units from geospatial & availability sets on heartbeat expiry. |
| **Durable Dispatch** | `XADD`, `XREADGROUP`, `XACK`, `XAUTOCLAIM` | **Stream** (`incidents:stream`) | Unlike Pub/Sub, Streams guarantee delivery: each message persists in a Pending Entries List (PEL) until `XACK`; dead-worker messages are reclaimed via `XAUTOCLAIM`. |
| **Race-Free Assignment** | `SET NX PX`, `WATCH/MULTI/EXEC`, Lua | **Mutex / Lua Script** | Protects against concurrent double-assignments with switchable modes: `unsafe`, `lock`, `watch`, and `lua`. |

---

## 🖥️ Live Dashboard & Simulator

- **Interactive Leaflet Map Dashboard (`/dashboard`)**: Single-page UI with zero build steps that connects to `/ws` and streams moving vehicle pins (🟢 Free, 🔴 Busy) and incident feeds.
- **Fleet Simulator (`simulator.py`)**: Jitters 50 ambulances around Bengaluru (`12.97, 77.59`) every second, comparing `naive` vs. `pipeline` mode throughput.

---

## 🚀 Quickstart

### 1. Prerequisites
- **Python 3.10+**
- **Redis Stack** running on `localhost:6379`:
  ```bash
  docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest
  ```

### 2. Installation
```bash
# Clone the repository
git clone <your-repo-url>
cd <repo-folder>

# Create and activate virtual environment
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Run the FastAPI Application
```bash
uvicorn app.main:app --reload --port 8000
```
- **API Documentation (Swagger UI)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Live Fleet Dashboard**: [http://localhost:8000/dashboard](http://localhost:8000/dashboard)
- **WebSocket Endpoint**: `ws://localhost:8000/ws`

### 4. Run the Fleet Simulator
In a separate terminal, launch the simulator:
```bash
# High-speed pipelined mode (1 RTT per tick)
python simulator.py --mode pipeline

# Individual network call mode (for latency comparison)
python simulator.py --mode naive
```

---

## 📡 Core API Endpoints

### Units & Telemetry
- `POST /units` - Register unit hash and seed into `units:free`.
- `GET /units/{id}` - Retrieve unit hash (404 if missing).
- `PATCH /units/{id}/status` - Update status and atomically `SMOVE` between `units:free` and `units:busy`.
- `POST /units/{id}/heartbeat` - Refresh 10-second heartbeat (`SET hb:unit:{id} 1 EX 10`).
- `GET /units/{id}/online` - Check online presence and remaining TTL.
- `POST /units/{id}/location` - Update GPS coordinates (`GEOADD units:geo lon lat unit:{id}`) and publish update.
- `GET /units/nearest?lat=&lon=&radius_km=5&count=5` - Proximity search returning nearest available (`free`) ambulances.
- `DELETE /units/{id}/location` - Remove unit from geospatial index (`ZREM`).

### Zones & Availability
- `POST /zones/{zone}/units/{id}` - Add unit to regional zone set (`SADD`).
- `DELETE /zones/{zone}/units/{id}` - Remove unit from zone set (`SREM`).
- `GET /zones/{zone}/units` - List all units in a zone (`SMEMBERS`).
- `GET /zones/{zone}/units/free` - Fast server-side intersection of free units in a zone (`SINTER`).

### Emergency Incidents Queue
- `POST /incidents` - File new incident hash and enqueue by severity (`ZADD incidents:queue`).
- `GET /incidents/queue` - View active triage queue ordered from most to least critical (`ZRANGE ... REV WITHSCORES`).
- `GET /incidents/next` - Atomically pop and return the most critical emergency (`ZPOPMAX`).

### Streaming & WebSockets
- `GET /dashboard` - Serve live Leaflet tracking dashboard.
- `WebSocket /ws` - Stream live events from `unit:updates` and `incident:updates`.

### Phase 5: Redis Streams Dispatch Engine
- `GET /stream/stats` - Stream telemetry: `XLEN`, `XPENDING` summary, and `XINFO CONSUMERS`.
- `POST /dispatch/auto` - Auto-assign the nearest free, online unit to the highest-priority incident.
- `POST /dispatch/manual` - Manually assign a specific unit to an incident.
- `POST /dispatch/{incident_id}/resolve` - Mark incident resolved and release the unit back to `units:free`.
- `GET /dispatch/active` - List all currently active dispatches.

---

## 🔄 Phase 5: Running the Stream Worker System

Phase 5 uses Redis Streams for **durable, at-least-once delivery** of incidents to dispatch workers.

> **Streams vs Pub/Sub**: Pub/Sub is fire-and-forget — if no subscriber is connected when a message
> is published, it is lost forever. Streams persist messages in memory until explicitly acknowledged
> with `XACK`. The Pending Entries List (PEL) tracks unacknowledged messages; `XAUTOCLAIM` lets
> healthy workers take over messages from dead workers after a configurable idle timeout.

### Step 1 — Start the FastAPI Server
```bash
# From inside the dispatchx/ directory
uvicorn app.main:app --port 8000
# OR from the repo root:
python -m uvicorn dispatchx.app.main:app --port 8000
```

### Step 2 — Seed 50 Units with the Simulator
In a **separate terminal**, run the simulator so workers have free units to assign:
```bash
python simulator.py --mode pipeline
```
The simulator registers 50 units around Bengaluru, keeps sending `GEOADD` and heartbeat (`SET hb:unit:{id} 1 EX 10`) every second.

### Step 3 — Start 3 Dispatch Workers (3 separate terminals)
```bash
# Terminal A
python worker.py worker-1

# Terminal B
python worker.py worker-2

# Terminal C
python worker.py worker-3
```
Each worker:
1. Calls `XREADGROUP GROUP dispatchers <name> COUNT 1 BLOCK 5000 STREAMS incidents:stream >`
2. Finds the nearest free unit with `GEOSEARCH`
3. Atomically claims it with `SMOVE units:free units:busy`
4. Updates `unit:{id}` and `incident:{id}` hashes, then `PUBLISH`es to `incident:updates`
5. Sleeps 1–3 s to simulate radio confirmation, then `XACK`s
6. If no free unit found — **does not ACK** so the message remains in the PEL for retry

A background recovery thread runs every 10 s:
```
XAUTOCLAIM incidents:stream dispatchers <worker> 15000 0-0 COUNT 10
```
This takes over any message idle > 15 s (i.e., the original worker died mid-processing).

### Step 4 — Fire 30 Incidents
```bash
python load_incidents.py --count 30
```
Watch the workers process the queue in real time.

### Step 5 — Check Stream Stats
```bash
curl http://localhost:8000/stream/stats
```
Returns:
- `length` — total entries ever added (`XLEN`)
- `pending` — unacknowledged message summary (`XPENDING`)
- `consumers` — per-consumer stats: pending count, idle time (`XINFO CONSUMERS`)

---

## 🛡️ Phase 6: Race-Free Assignment & Concurrency Comparison

When multiple workers read emergency incidents from `incidents:stream` simultaneously, two workers can pick the same ambulance if assignment is not atomic. Phase 6 introduces 4 switchable assignment modes:

| Mode | Safety | Mechanism | Tradeoff |
| :--- | :--- | :--- | :--- |
| **`unsafe`** | ❌ Unsafe | Non-atomic `SISMEMBER` check followed by update | Subject to race conditions (double-assignments). |
| **`lock`** | ✅ Safe | Distributed lock (`SET lock:unit:{id} <token> NX PX 5000`) with Lua token unlock | Mutual exclusion per unit; adds lock round-trip latency. |
| **`watch`** | ✅ Safe | Optimistic concurrency using `WATCH unit:{id} units:free` + `MULTI/EXEC` | Zero lock overhead; retries on conflict (`WatchError`). |
| **`lua`** | ✅ Safe | Registered Lua script checking candidates, `SMOVE`ing, and updating in 1 atomic step | Fastest and cleanest: executes on Redis thread with zero races. |

### Running Workers with Specific Modes
You can select the mode via CLI flag or `ASSIGN_MODE` environment variable:
```bash
# Via CLI flag
python worker.py worker-1 --mode lua
python worker.py worker-2 --mode lock
python worker.py worker-3 --mode watch

# Or via environment variable
$env:ASSIGN_MODE="lua"
python worker.py worker-1
```

### Running the Concurrency Benchmark (`race_test.py`)
Spawns 10 parallel worker threads against 5 units and 20 incidents to detect double-assignments:
```bash
python race_test.py --mode all
```
Or test an individual mode:
```bash
python race_test.py --mode unsafe
python race_test.py --mode lock
python race_test.py --mode watch
python race_test.py --mode lua
```

---

## 📄 License
MIT License

