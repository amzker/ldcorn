import asyncio
import os
import time
import numpy as np
from contextlib import asynccontextmanager
from fastapi import FastAPI
import aiosqlite
from sklearn.ensemble import RandomForestRegressor
from sklearn.datasets import make_regression
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi import WebSocket, WebSocketDisconnect

MODEL = None
DB_PATH = "examples_test.db"

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
    global MODEL
    MODEL = await asyncio.to_thread(train_model)
    
    async with aiosqlite.connect(DB_PATH, timeout=60) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
        await db.execute("INSERT OR IGNORE INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
        await db.commit()
        
    yield
    
    # shutdown
    # we do not delete the db file here.
    # during a sighup hot-reload, the old workers shut down. if they delete the db file,
    # the new workers will suddenly crash with "no such table: users" because their database just vanished!

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
    
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        async with db.execute("SELECT id, name FROM users") as cursor:
            users = await cursor.fetchall()
            
    return {"worker_pid": pid, "status": "fast_io_ok", "users": users}

@app.get("/db/read")
async def db_read_endpoint():
    pid = os.getpid()
    
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            count = await cursor.fetchone()
            
    return {"worker_pid": pid, "user_count": count[0]}

class User(BaseModel):
    name: str

@app.post("/db/insert")
async def db_insert_endpoint(user: User):
    pid = os.getpid()
    
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("INSERT INTO users (name) VALUES (?)", (user.name,))
        await db.commit()
            
    return {"worker_pid": pid, "status": "inserted", "name": user.name}

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
    return {"worker_pid": pid, "primes_found": count, "limit": n}

@app.get("/ml-pipeline/start")
async def ml_endpoint():
    pid = os.getpid()
    
    result = await asyncio.to_thread(run_ml_prediction)
    result["worker_pid"] = pid
    
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