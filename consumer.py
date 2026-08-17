import os
import json
import time
import logging
import pika
import hashlib
from db import save_event, init_db, update_event_by_hash

logger = logging.getLogger("replay_manager.consumer")

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', '127.0.0.1')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
RABBITMQ_QUEUE = os.getenv('LOG_EVENTS_QUEUE', 'log-events')
DB_PATH = os.getenv('DB_PATH', '/app/data/replay.db')
HEARTBEAT_FILE = "/tmp/worker_heartbeat"

def touch_heartbeat():
    try:
        os.makedirs(os.path.dirname(HEARTBEAT_FILE), exist_ok=True)
        with open(HEARTBEAT_FILE, 'a'):
            os.utime(HEARTBEAT_FILE, None)
    except Exception as e:
        logger.warning("action=heartbeat status=failed error=\"%s\"", str(e))

def get_canonical_hash(payload_str: str) -> str:
    try:
        data = json.loads(payload_str)
        canonical_str = json.dumps(data, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        return hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()
    except Exception:
        return hashlib.sha256(payload_str.encode('utf-8')).hexdigest()

def extract_ticket_id(data: dict, body_str: str) -> str:
    """Helper to extract ticket ID from various possible webhook formats."""
    # 1. Try direct keys
    for key in ['id', 'ticket_id', 'ticketId']:
        if key in data and data[key]:
            return str(data[key])
            
    # 2. Try nested webhook structure
    if "payload" in data:
        try:
            payload_data = json.loads(data["payload"])
            for key in ['id', 'ticket_id', 'ticketId']:
                if key in payload_data and payload_data[key]:
                    return str(payload_data[key])
        except Exception:
            pass
            
    # 3. Fallback to regex extraction
    import re
    # Unescape payload if wrapped in JSON string
    target_str = body_str
    payload_match = re.search(r'"payload"\s*:\s*"(.*)"\s*,\s*"payload_encoding"', body_str)
    if payload_match:
        target_str = payload_match.group(1).replace('\\"', '"').replace('\\\\', '\\')

    # Match "id":"12345" or "ticket_id":12345 (with optional backslashes for safety)
    match = re.search(r'\\?"(?:id|ticket_id|ticketId)\\?"\s*:\s*\\?"?(\w+)\\?"?', target_str)
    if match:
        return match.group(1)
        
    return "0"

def start_consumer(stop_event=None):
    logger.info("action=start_consumer msg=\"Initializing background RabbitMQ consumer...\"")
    
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300
    )

    while True:
        if stop_event and stop_event.is_set():
            logger.info("action=stop_consumer status=success")
            break
            
        connection = None
        try:
            logger.info("action=rabbitmq_connect host=%s port=%d", RABBITMQ_HOST, RABBITMQ_PORT)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Declare the log-events queue (durable)
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            
            def callback(ch, method, properties, body):
                touch_heartbeat()
                try:
                    body_str = body.decode('utf-8', errors='ignore')
                    logger.debug("Received event: %s", body_str)
                    
                    # Parse payload
                    try:
                        data = json.loads(body_str)
                    except Exception:
                        data = {}
                    
                    if data.get("is_report") is True:
                        # This is a delivery status report from a sender
                        event_hash = data.get("event_hash")
                        status = data.get("status", "SUCCESS")
                        error_message = data.get("error_message")
                        
                        update_event_by_hash(
                            db_path=DB_PATH,
                            event_hash=event_hash,
                            status=status,
                            error_message=error_message
                        )
                        logger.info("action=update_event_status status=success event_hash=%s new_status=%s", event_hash, status)
                    else:
                        # This is a new event from GLPI/source
                        ticket_id = extract_ticket_id(data, body_str)
                        routing_key = method.routing_key or "unknown"
                        
                        # Generate canonical hash from payload
                        event_hash = get_canonical_hash(body_str)
                        
                        # Save to SQLite as PENDING
                        event_id = save_event(
                            db_path=DB_PATH,
                            ticket_id=ticket_id,
                            event_hash=event_hash,
                            routing_key=routing_key,
                            payload=body_str,
                            status="PENDING",
                            error_message=None
                        )
                        logger.info("action=save_event status=success event_id=%d ticket_id=%s routing_key=%s event_hash=%s", event_id, ticket_id, routing_key, event_hash)
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    logger.error("action=process_message_failed error=\"%s\"", str(e), exc_info=True)
                    # Reject message but requeue=True to retry later if it is a DB write failure
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    time.sleep(2)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
            
            logger.info("action=rabbitmq_consume_started queue=%s msg=\"Waiting for messages...\"", RABBITMQ_QUEUE)
            while channel.is_open:
                if stop_event and stop_event.is_set():
                    break
                connection.process_data_events(time_limit=5)
                touch_heartbeat()
                
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning("action=rabbitmq_connect_failed error=\"%s\" reconnecting_in=10s", str(e))
            time.sleep(10)
        except Exception as e:
            logger.error("action=consumer_error error=\"%s\" reconnecting_in=10s", str(e), exc_info=True)
            time.sleep(10)
        finally:
            if connection and not connection.is_closed:
                try:
                    connection.close()
                except Exception:
                    pass
