import time

def test_daily_and_range_stats(client):
    today = time.strftime("%Y-%m-%d")

    res_daily = client.get(f"/stats/daily?date={today}")
    assert res_daily.status_code == 200
    assert "unique_callers" in res_daily.json()
    assert "unique_areas" in res_daily.json()

    res_range = client.get(f"/stats/range?from={today}&to={today}")
    assert res_range.status_code == 200
    assert "unique_callers" in res_range.json()


def test_stream_stats(client):
    res_stream = client.get("/stream/stats")
    assert res_stream.status_code == 200
    data = res_stream.json()
    assert data["stream"] == "incidents:stream"
    assert "length" in data
def test_units_overlap(client):
    today = "2026-11-10"
    res = client.get(f"/stats/units-overlap?ids=unit-1,unit-2&date={today}")
    assert res.status_code == 200
    data = res.json()
    assert "overlap_hours_count" in data
    assert "overlap_hours" in data
    assert data["date"] == today
