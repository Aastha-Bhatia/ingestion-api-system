import pytest
from fastapi.testclient import TestClient
from main import app
import time

client = TestClient(app)

def test_ingest_api():
    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3, 4, 5], "priority": "HIGH"}
    )
    assert response.status_code == 202
    assert "ingestion_id" in response.json()
    
    response = client.post(
        "/ingest",
        json={"ids": [0, 1, 2], "priority": "HIGH"}
    )
    assert response.status_code == 400
    
    response = client.post(
        "/ingest",
        json={"ids": [10**9 + 8, 1, 2], "priority": "HIGH"}
    )
    assert response.status_code == 400

def test_status_api():
    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"}
    )
    ingestion_id = response.json()["ingestion_id"]
    
    response = client.get(f"/status/{ingestion_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["ingestion_id"] == ingestion_id
    assert data["status"] in ["yet_to_start", "triggered"]
    assert len(data["batches"]) == 2
    
    response = client.get("/status/nonexistent")
    assert response.status_code == 404

def test_priority_handling():
    response1 = client.post(
        "/ingest",
        json={"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"}
    )
    ingestion_id1 = response1.json()["ingestion_id"]

    response2 = client.post(
        "/ingest",
        json={"ids": [6, 7, 8], "priority": "HIGH"}
    )
    ingestion_id2 = response2.json()["ingestion_id"]

  
    high_status = None
    medium_status = None
    for _ in range(20):  
        time.sleep(0.5)
        high_status = client.get(f"/status/{ingestion_id2}").json()
        medium_status = client.get(f"/status/{ingestion_id1}").json()
        if high_status["batches"][0]["status"] == "triggered":
            break

    assert high_status["batches"][0]["status"] == "triggered", "High priority batch should be triggered first"
    assert medium_status["batches"][0]["status"] == "yet_to_start", "Medium priority should wait"

def test_rate_limiting():
    responses = []
    for i in range(3):
        response = client.post(
            "/ingest",
            json={"ids": [i*3+1, i*3+2, i*3+3], "priority": "LOW"}
        )
        responses.append(response)
    
    time.sleep(1)
    statuses = [client.get(f"/status/{r.json()['ingestion_id']}").json() for r in responses]
    
    triggered = sum(1 for s in statuses for b in s["batches"] if b["status"] == "triggered")
    assert triggered <= 1, "Should process only one batch every 5 seconds"
