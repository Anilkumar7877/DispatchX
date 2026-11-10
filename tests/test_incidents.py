import time

def test_incident_creation_and_queue(client):
    source_caller = f"caller-{int(time.time() * 1000)}"

    payload = {
        "type": "Cardiac Arrest",
        "lat": 12.9716,
        "lon": 77.5946,
        "severity": 9,
        "zone": "central",
    }

    # 1. Create incident with X-Source header
    res_create = client.post("/incidents", json=payload, headers={"X-Source": source_caller})
    assert res_create.status_code == 201
    created_data = res_create.json()
    assert "id" in created_data
    assert created_data["status"] == "pending"

    # 2. Inspect queue
    res_queue = client.get("/incidents/queue")
    assert res_queue.status_code == 200
    queue_data = res_queue.json()
    assert any(item["id"] == created_data["id"] for item in queue_data)

    # 3. Pop highest incident
    res_pop = client.get("/incidents/next")
    assert res_pop.status_code == 200
    assert "type" in res_pop.json()


def test_incident_rate_limit(client):
    source_caller = f"ratelimit-caller-{int(time.time() * 1000)}"
    payload = {
        "type": "Minor Injury",
        "lat": 12.9716,
        "lon": 77.5946,
        "severity": 2,
        "zone": "south",
    }

    status_codes = []
    for _ in range(7):
        res = client.post("/incidents", json=payload, headers={"X-Source": source_caller})
        status_codes.append(res.status_code)

    # First 5 should succeed (201), remaining should be rate limited (429)
    assert status_codes[:5] == [201, 201, 201, 201, 201]
    assert status_codes[5] == 429

