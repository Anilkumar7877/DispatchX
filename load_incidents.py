"""
RedAlert Incident Load Generator
Phase 5: Rapidly injects 30 emergency incidents through POST /incidents into Redis Streams and priority queues.
"""

import argparse
import json
import random
import time
import urllib.request
import urllib.error

INCIDENT_TYPES = [
    "Cardiac Arrest",
    "Severe Stroke",
    "Major Highway Collision",
    "Acute Respiratory Distress",
    "Severe Burn Injury",
    "Anaphylactic Shock",
    "Pedestrian Trauma",
    "Fall from Height",
    "Diabetic Emergency",
    "Industrial Machinery Trauma",
    "Active Seizure",
    "Severe Head Injury",
]

# Bengaluru center coordinates
BENGALURU_LAT = 12.97
BENGALURU_LON = 77.59


def generate_incident(index: int):
    # Scatter incidents around Bengaluru urban area (~4-5 km radius)
    lat = round(BENGALURU_LAT + random.uniform(-0.035, 0.035), 5)
    lon = round(BENGALURU_LON + random.uniform(-0.035, 0.035), 5)
    incident_type = random.choice(INCIDENT_TYPES)
    # Severity 1 to 10 with bias towards high severity emergencies
    severity = random.choices(
        population=list(range(1, 11)),
        weights=[1, 1, 2, 2, 3, 4, 6, 8, 10, 10],
        k=1
    )[0]

    return {
        "type": incident_type,
        "lat": lat,
        "lon": lon,
        "severity": severity,
    }


def send_incident(api_url: str, payload: dict, source: str = "load-tester") -> dict:
    url = f"{api_url.rstrip('/')}/incidents"
    data_bytes = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data_bytes,
        headers={
            "Content-Type": "application/json",
            "X-Source": source,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description="Inject 30 emergency incidents into RedAlert")
    parser.add_argument("--url", default="http://localhost:8000", help="FastAPI base URL (default: http://localhost:8000)")
    parser.add_argument("--count", type=int, default=30, help="Number of incidents to generate (default: 30)")
    args = parser.parse_args()

    print("======================================================")
    print(f"  🚨 RedAlert Incident Load Generator")
    print(f"  Target: {args.url}/incidents | Count: {args.count}")
    print("======================================================")

    t0 = time.perf_counter()
    successful = 0

    for i in range(1, args.count + 1):
        payload = generate_incident(i)
        # Use rotating source IDs so bulk load testing is not blocked by single-source rate limit
        source_id = f"caller-batch-{i % 10:02d}"
        try:
            start_req = time.perf_counter()
            result = send_incident(args.url, payload, source=source_id)
            elapsed_ms = (time.perf_counter() - start_req) * 1000.0

            incident_id = result.get("id") or result.get("incident_id")
            print(
                f"[{i:02d}/{args.count:02d}] ✅ Created Incident #{incident_id:<3} | "
                f"Sev: {payload['severity']:>2}/10 | {payload['type']:<28} | {elapsed_ms:>6.2f} ms"
            )
            successful += 1
        except urllib.error.URLError as err:
            print(f"[{i:02d}/{args.count:02d}] ❌ Error posting incident: {err}")
        except Exception as exc:
            print(f"[{i:02d}/{args.count:02d}] ❌ Unexpected error: {exc}")

    total_time = time.perf_counter() - t0
    rate = successful / total_time if total_time > 0 else 0

    print("------------------------------------------------------")
    print(f"🎉 Complete! Successfully sent {successful}/{args.count} incidents in {total_time:.2f}s ({rate:.1f} req/s).")
    print(f"Check stream stats at: {args.url}/stream/stats")


if __name__ == "__main__":
    main()

