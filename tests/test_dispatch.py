import time

def test_auto_and_manual_dispatch_flow(client):
    ts = int(time.time() * 1000)
    unit_id = f"disp-unit-{ts}"
    source_caller = f"caller-disp-{ts}"

    # Setup unit: registered, free, location set, heartbeat active
    client.post("/units", json={"id": unit_id, "type": "ALS", "status": "free", "driver": "Dave"})
    client.post(f"/units/{unit_id}/location", json={"lat": 12.9716, "lon": 77.5946})
    client.post(f"/units/{unit_id}/heartbeat")

    # Create incident near unit
    inc_payload = {
        "type": "Respiratory Distress",
        "lat": 12.9720,
        "lon": 77.5950,
        "severity": 8,
        "zone": "central",
    }
    res_inc = client.post("/incidents", json=inc_payload, headers={"X-Source": source_caller})
    assert res_inc.status_code == 201
    incident_id = res_inc.json()["id"]

    # Auto dispatch
    res_disp = client.post("/dispatch/auto?radius_km=15")
    assert res_disp.status_code == 200
    disp_data = res_disp.json()
    assert disp_data["status"] == "dispatched"

    # Verify active dispatches
    res_active = client.get("/dispatch/active")
    assert res_active.status_code == 200
    active_list = res_active.json()
    assert any(m["unit"]["id"] == unit_id for m in active_list)

    # Resolve mission
    res_resolve = client.post(f"/dispatch/{incident_id}/resolve")
    assert res_resolve.status_code == 200
    assert res_resolve.json()["status"] == "resolved"

    # Unit should be free again
    unit_res = client.get(f"/units/{unit_id}")
    assert unit_res.status_code == 200
    assert unit_res.json()["status"] == "free"

