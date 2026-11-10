import time

def test_zone_membership_and_filtering(client):
    unique_unit_id = f"zone-test-unit-{int(time.time() * 1000)}"
    zone_name = "north-koramangala"

    # Register unit as free
    client.post("/units", json={"id": unique_unit_id, "type": "BLS", "status": "free", "driver": "Sam"})

    # Add to zone
    res_add = client.post(f"/zones/{zone_name}/units/{unique_unit_id}")
    assert res_add.status_code == 200

    # List all units in zone
    res_list = client.get(f"/zones/{zone_name}/units")
    assert res_list.status_code == 200
    assert unique_unit_id in res_list.json()

    # List free units in zone
    res_free = client.get(f"/zones/{zone_name}/units/free")
    assert res_free.status_code == 200
    assert unique_unit_id in res_free.json()

    # Remove unit from zone
    res_remove = client.delete(f"/zones/{zone_name}/units/{unique_unit_id}")
    assert res_remove.status_code == 200
    assert res_remove.json()["removed"] is True

