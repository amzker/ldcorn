import asyncio
import os
import time
import uuid
import random
import numpy as np
from contextlib import asynccontextmanager
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.datasets import make_regression
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi import WebSocket, WebSocketDisconnect

MODEL = None
MONGO_CLIENT = None

# THIS IS NOT ENV LEAK , IT IS MY LOCAL TEMP DB
# DB WAS WIPED ON EACH BENCHMARK RUN AND RESTARTED FRESH
MONGODB_URI="mongodb://localhost:27017/"

def train_model():
    print(f"[{os.getpid()}] Training dummy ML model...")
    X, y = make_regression(n_samples=5000, n_features=20, random_state=42)
    model = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
    model.fit(X, y)
    print(f"[{os.getpid()}] Model trained.")
    return model

def run_ml_prediction():
    if MODEL is None:
        return {"error": "Model not loaded"}
    
    start = time.time()
    X_test = np.random.rand(1000, 20)
    predictions = MODEL.predict(X_test)
    elapsed = time.time() - start
    
    return {
        "predictions_count": len(predictions),
        "mean_prediction": float(np.mean(predictions)),
        "compute_time_seconds": elapsed
    }

@asynccontextmanager
async def lifespan(app: FastAPI):
    global MODEL, MONGO_CLIENT
    MODEL = await asyncio.to_thread(train_model)
    
    MONGO_CLIENT = AsyncIOMotorClient(MONGODB_URI, maxPoolSize=30)
    db = MONGO_CLIENT.get_database("ldcorn_test")
    users_coll = db.get_collection("users")
    
    await users_coll.create_index("user_id", unique=True)
    
    await users_coll.update_one({"user_id": "1"}, {"$set": {"name": "Alice"}}, upsert=True)
    await users_coll.update_one({"user_id": "2"}, {"$set": {"name": "Bob"}}, upsert=True)
        
    yield
    
    if MONGO_CLIENT:
        MONGO_CLIENT.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root_endpoint():
    pid = os.getpid()
    
    db = MONGO_CLIENT.get_database("ldcorn_test")
    users_coll = db.get_collection("users")
    cursor = users_coll.find({}, {"_id": 0, "user_id": 1, "name": 1}).limit(10)
    users = await cursor.to_list(length=10)
            
    return {"worker_pid": pid, "status": "fast_io_ok", "users": users}

@app.get("/db/read")
async def db_read_endpoint():
    pid = os.getpid()
    
    db = MONGO_CLIENT.get_database("ldcorn_test")
    users_coll = db.get_collection("users")
    count = await users_coll.estimated_document_count()
            
    return {"worker_pid": pid, "user_count": count}

class User(BaseModel):
    name: str

class UpsertUser(BaseModel):
    user_id: str
    name: str


@app.post("/db/insert")
async def db_insert_endpoint(user: User):
    pid = os.getpid()
    
    db = MONGO_CLIENT.get_database("ldcorn_test")
    users_coll = db.get_collection("users")
    user_id = str(uuid.uuid4())
    await users_coll.insert_one({"user_id": user_id, "name": user.name})
            
    return {"worker_pid": pid, "status": "inserted", "name": user.name, "user_id": user_id}

@app.post("/db/upsert")
async def db_upsert_endpoint(user: UpsertUser):
    pid = os.getpid()
    db = MONGO_CLIENT.get_database("ldcorn_test")
    users_coll = db.get_collection("users")
    await users_coll.update_one(
        {"user_id": user.user_id},
        {"$set": {"name": user.name}},
        upsert=True
    )
    return {"worker_pid": pid, "status": "upserted", "user_id": user.user_id, "name": user.name}

@app.delete("/db/delete/{user_id}")
async def db_delete_endpoint(user_id: str):
    pid = os.getpid()
    db = MONGO_CLIENT.get_database("ldcorn_test")
    users_coll = db.get_collection("users")
    result = await users_coll.delete_one({"user_id": user_id})
    return {"worker_pid": pid, "status": "deleted", "deleted_count": result.deleted_count}

@app.get("/math/prime/{n}")
async def prime_endpoint(n: int):
    pid = os.getpid()
    
    def calculate_primes(limit):
        primes = []
        for possiblePrime in range(2, limit + 1):
            isPrime = True
            for num in range(2, int(possiblePrime ** 0.5) + 1):
                if possiblePrime % num == 0:
                    isPrime = False
                    break
            if isPrime:
                primes.append(possiblePrime)
        return len(primes)

    count = await asyncio.to_thread(calculate_primes, n)
    random_time = random.uniform(1,3)
    time.sleep(random_time) # well in real prod app this is not going be there at all, if it is then you have bigger problems , checkout loopsentry to find blocks (yes shameless selfpromotion)
    return {"worker_pid": pid, "primes_found": count, "limit": n}

@app.get("/ml-pipeline/start")
async def ml_endpoint():
    pid = os.getpid()
    
    result = await asyncio.to_thread(run_ml_prediction)
    result["worker_pid"] = pid
    random_time = random.uniform(1,3)
    time.sleep(random_time)
    
    return result

STATEFUL_COUNTER = 0

@app.get("/counter")
async def counter_endpoint():
    global STATEFUL_COUNTER
    STATEFUL_COUNTER += 1
    return {"worker_pid": os.getpid(), "counter": STATEFUL_COUNTER}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    worker_pid = os.getpid()
    try:
        await websocket.send_text(f"Connected to worker {worker_pid}")
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Worker {worker_pid} echoes: {data}")
    except WebSocketDisconnect:
        print(f"[{worker_pid}] Client disconnected from WebSocket")