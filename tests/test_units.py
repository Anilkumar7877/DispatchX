import time

def test_unit_lifecycle_and_geo(client):
    unique_unit_id = f"test-unit-{int(time.time() * 1000)}"

    # 1. Register unit
    reg_payload = {
        "id": unique_unit_id,
        "type": "ALS",
        "status": "free",
        "driver": "Alex Carter",
    }
    res_reg = client.post("/units", json=reg_payload)
    assert res_reg.status_code == 201
    assert res_reg.json()["id"] == unique_unit_id

    # 2. Update location
    loc_payload = {"lat": 12.9716, "lon": 77.5946}
    res_loc = client.post(f"/units/{unique_unit_id}/location", json=loc_payload)
    assert res_loc.status_code == 200
    assert res_loc.json()["lat"] == 12.9716

    # 3. Heartbeat
    res_hb = client.post(f"/units/{unique_unit_id}/heartbeat")
    assert res_hb.status_code == 200
    assert res_hb.json()["status"] == "ok"

    # 4. Check online
    res_online = client.get(f"/units/{unique_unit_id}/online")
    assert res_online.status_code == 200
    assert res_online.json()["online"] is True

    # 5. Get Unit details
    res_get = client.get(f"/units/{unique_unit_id}")
    assert res_get.status_code == 200
    assert res_get.json()["driver"] == "Alex Carter"

    # 6. Nearest units search
    res_near = client.get(f"/units/nearest?lat=12.9716&lon=77.5946&radius_km=10")
    assert res_near.status_code == 200
    assert isinstance(res_near.json(), list)

    # 7. Update status to busy
    res_status = client.patch(f"/units/{unique_unit_id}/status", json={"status": "busy"})
    assert res_status.status_code == 200
    assert res_status.json()["status"] == "busy"

    # 8. Check uptime bitmap
    today_str = time.strftime("%Y-%m-%d")
    res_uptime = client.get(f"/units/{unique_unit_id}/uptime?date={today_str}")
    assert res_uptime.status_code == 200
    assert res_uptime.json()["hours_online"] >= 1

    # 9. Clean up location
    res_del_loc = client.delete(f"/units/{unique_unit_id}/location")
    assert res_del_loc.status_code == 200

