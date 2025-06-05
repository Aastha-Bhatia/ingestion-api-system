# Data Ingestion API

A simple REST API for data ingestion with priority-based batch processing and rate limiting.

## 🚀 Live Demo

**Deployed URL:** `https://ingestion-api-system.onrender.com`

## API Endpoints

### POST /ingest
Submit data ingestion request with list of IDs and priority.

**Request:**
```json
{
  "ids": [1, 2, 3, 4, 5],
  "priority": "HIGH"
}
```

**Response:**
```json
{
  "ingestion_id": "abc123"
}
```

### GET /status/{ingestion_id}
Check processing status of ingestion request.

**Response:**
```json
{
  "ingestion_id": "abc123",
  "status": "triggered",
  "batches": [
    {"batch_id": "abc123_0", "ids": [1, 2, 3], "status": "completed"},
    {"batch_id": "abc123_1", "ids": [4, 5], "status": "triggered"}
  ]
}
```

## How to Run

### Clone Repository
```bash
git clone https://github.com/Aastha-Bhatia/ingestion-api-system
cd data-ingestion-api
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run Application
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Visit: `http://localhost:8000`

## Features

- Process max 3 IDs per batch
- Rate limit: 1 batch per 5 seconds
- Priority handling: HIGH > MEDIUM > LOW
- Real-time status tracking
- Background async processing

## Test Examples

```bash
# Submit ingestion request
curl -X POST "http://localhost:8000/ingest" \
  -H "Content-Type: application/json" \
  -d '{"ids": [1, 2, 3, 4, 5], "priority": "HIGH"}'

# Check status
curl "http://localhost:8000/status/your-ingestion-id"
```

## Tech Stack

- **Language:** Python
- **Framework:** FastAPI
- **Deployment:** Render
- **Processing:** Background threads with priority queue

## Requirements

```
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.0
```

---

**Made by:** Aastha Bhatia  
**LinkedIn:** https://linkedin.com/in/aasthabhatia-er
**Gmail:** aasthabhatia.er@gmail.com
