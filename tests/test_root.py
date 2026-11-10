def test_root_endpoint(client):
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "RedAlert"
    assert "dashboard_url" in data
    assert "websocket_url" in data


def test_dashboard_endpoint(client):
    response = client.get("/dashboard")
    assert response.status_code in (200, 404)

