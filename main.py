import os
import json
import logging
import asyncio
import threading
from contextlib import asynccontextmanager
import pika
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from db import init_db, get_ticket_events, get_event_by_id, update_event_status, prune_old_events, get_database_stats, get_latest_events
from consumer import start_consumer

# Setup logs
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format='level=%(levelname)s logger=%(name)s %(message)s'
)
logger = logging.getLogger("replay_manager.main")

DB_PATH = os.getenv('DB_PATH', '/app/data/replay.db')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', '127.0.0.1')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')

# Background task variables
consumer_thread = None
stop_event = None

async def db_retention_worker():
    """Background loop to prune events older than 30 days every 24 hours."""
    while True:
        try:
            logger.info("action=db_retention_worker status=running")
            prune_old_events(DB_PATH, days=30)
        except Exception as e:
            logger.error("action=db_retention_worker status=failed error=\"%s\"", str(e))
        await asyncio.sleep(86400) # Sleep for 24 hours

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start consumer thread
    global consumer_thread, stop_event
    stop_event = threading.Event()
    init_db(DB_PATH)
    
    # Start RabbitMQ consumer in a background daemon thread
    consumer_thread = threading.Thread(
        target=start_consumer,
        args=(stop_event,),
        daemon=True,
        name="RabbitMQ-Consumer-Thread"
    )
    consumer_thread.start()
    
    # Start database maintenance task in the asyncio loop
    asyncio.create_task(db_retention_worker())
    
    yield
    
    # Shutdown routines
    logger.info("action=shutdown msg=\"Stopping background threads...\"")
    stop_event.set()
    if consumer_thread and consumer_thread.is_alive():
        consumer_thread.join(timeout=5)
    logger.info("action=shutdown status=complete")

app = FastAPI(
    title="Replay Manager API",
    description="Microservice to log and retry RabbitMQ messages",
    version="1.0.0",
    lifespan=lifespan
)

# Serve index.html directly as a file from the templates folder
@app.get("/", response_class=HTMLResponse)
async def read_index():
    try:
        file_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
        with open(file_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except Exception as e:
        logger.error("action=read_index_file status=failed error=\"%s\"", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Index template file not found")

@app.get("/api/tickets/{ticket_id}")
async def get_history(ticket_id: str):
    try:
        events = get_ticket_events(DB_PATH, ticket_id)
        return events
    except Exception as e:
        logger.error("Failed to query history for ticket %s: %s", ticket_id, str(e))
        raise HTTPException(status_code=500, detail="Database query failed")

@app.get("/api/stats")
async def get_stats():
    try:
        return get_database_stats(DB_PATH)
    except Exception as e:
        logger.error("action=get_stats status=failed error=\"%s\"", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to query database stats")

@app.get("/api/events")
async def get_recent_events(queue: str = None, limit: int = 30):
    try:
        return get_latest_events(DB_PATH, queue_name=queue, limit=limit)
    except Exception as e:
        logger.error("action=get_recent_events status=failed error=\"%s\"", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to query recent events")

@app.get("/api/health")
async def get_health():
    import time
    # Check if thread is running
    is_alive = False
    if 'consumer_thread' in globals() and consumer_thread is not None:
        is_alive = consumer_thread.is_alive()
        
    # Check heartbeat recency (5 mins)
    heartbeat_ok = False
    heartbeat_file = "/tmp/worker_heartbeat"
    if os.path.exists(heartbeat_file):
        try:
            mtime = os.path.getmtime(heartbeat_file)
            if time.time() - mtime < 300:
                heartbeat_ok = True
        except Exception:
            pass
            
    status = "active" if (is_alive and heartbeat_ok) else "inactive"
    return {
        "status": status,
        "thread_alive": is_alive,
        "heartbeat_ok": heartbeat_ok
    }

@app.post("/api/events/{event_id}/retry")
async def trigger_retry(event_id: int):
    # 1. Fetch event from DB
    event = get_event_by_id(DB_PATH, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
        
    payload = event["payload"]
    routing_key = event["routing_key"]
    
    # 2. Resend to RabbitMQ exchange
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Publish directly back to exchange with routing key (uses default exchange for queues)
        # Note: If your system uses a custom exchange, replace '' with your exchange name.
        channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=payload,
            properties=pika.BasicProperties(
                delivery_mode=2, # make message persistent
            )
        )
        connection.close()
        
        # 3. Reset the event status in the database to PENDING
        update_event_status(DB_PATH, event_id, status="PENDING", error_message="Manually retried via Web UI")
        
        logger.info("action=replay_message status=success event_id=%d routing_key=%s", event_id, routing_key)
        return {"status": "success", "message": f"Message replayed to queue {routing_key}"}
        
    except Exception as e:
        logger.error("action=replay_message status=failed error=\"%s\"", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to publish message: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    # Bind to port 8005 internally (expose as port 8008 or configurable in docker-compose)
    uvicorn.run("main:app", host="0.0.0.0", port=8005, reload=False)
