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

---

## 📄 License
MIT License

