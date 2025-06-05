from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "🚀 Welcome to the Ingestion API!"}
    
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator
from typing import List, Dict
from enum import Enum
import uuid
import time
import heapq
import threading    

async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content={"detail": "Validation error"}
    )

ingestion_store: Dict[str, dict] = {}
batch_queue = []
queue_counter = 0
last_processed_time = 0
RATE_LIMIT_SECONDS = 5
processing_lock = threading.Lock()

class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class IngestRequest(BaseModel):
    ids: List[int]
    priority: Priority

    @field_validator('ids')
    @classmethod
    def validate_ids(cls, v):
        if not v:
            raise ValueError('ids list cannot be empty')
        for id_val in v:
            if id_val <= 0 or id_val > 10**9:
                raise ValueError('All ids must be positive integers <= 10^9')
        return v

class BatchStatus(BaseModel):
    batch_id: str
    ids: List[int]
    status: str

class IngestionStatus(BaseModel):
    ingestion_id: str
    status: str
    batches: List[BatchStatus]

def get_priority_value(priority: Priority) -> int:
    return {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[priority]

def create_batches(ids: List[int], batch_size: int = 3) -> List[List[int]]:
    return [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]

def process_batch_sync():
    global last_processed_time
    while True:
        current_time = time.time()

        with processing_lock:
            if batch_queue and (current_time - last_processed_time) >= RATE_LIMIT_SECONDS:
                priority_val, counter, ingestion_id, batch_idx = heapq.heappop(batch_queue)

                if ingestion_id in ingestion_store:
                    ingestion_store[ingestion_id]["batches"][batch_idx]["status"] = "triggered"

                    if all(batch["status"] == "triggered" for batch in ingestion_store[ingestion_id]["batches"]):
                        ingestion_store[ingestion_id]["status"] = "triggered"

                last_processed_time = current_time

        time.sleep(0.1)


background_thread = threading.Thread(target=process_batch_sync, daemon=True)
background_thread.start()

app = FastAPI()
app.add_exception_handler(RequestValidationError, validation_exception_handler)

@app.post("/ingest", status_code=202)
async def ingest_data(request: IngestRequest):
    global queue_counter
    with processing_lock:
        ingestion_id = str(uuid.uuid4())
        batches = create_batches(request.ids)

        batch_objects = []
        for i, batch_ids in enumerate(batches):
            batch_obj = {
                "batch_id": f"{ingestion_id}_{i}",
                "ids": batch_ids,
                "status": "yet_to_start"
            }
            batch_objects.append(batch_obj)

            priority_val = get_priority_value(request.priority)
            heapq.heappush(batch_queue, (priority_val, queue_counter, ingestion_id, i))
            queue_counter += 1

        ingestion_store[ingestion_id] = {
            "ingestion_id": ingestion_id,
            "status": "yet_to_start",
            "batches": batch_objects,
            "priority": request.priority
        }

    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
async def get_status(ingestion_id: str):
    with processing_lock:
        if ingestion_id not in ingestion_store:
            raise HTTPException(status_code=404, detail="Ingestion ID not found")

        data = ingestion_store[ingestion_id]
        return IngestionStatus(
            ingestion_id=data["ingestion_id"],
            status=data["status"],
            batches=[
                BatchStatus(
                    batch_id=batch["batch_id"],
                    ids=batch["ids"],
                    status=batch["status"]
                )
                for batch in data["batches"]
            ]
        )
