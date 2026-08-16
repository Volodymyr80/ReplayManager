import os
import json
import hashlib
import pika
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("replay_manager.sdk_example")

# RabbitMQ Configuration
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', '127.0.0.1')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
SENDER_QUEUE_NAME = os.getenv('RABBITMQ_QUEUE', 'my-worker-queue') # The worker queue (e.g. sms-sender)

def get_canonical_hash(payload_str: str) -> str:
    """
    Computes a SHA256 hash of the canonical JSON representation.
    Ensures that differences in formatting, spaces, or keys sorting 
    do not lead to mismatched hashes across microservices.
    """
    try:
        data = json.loads(payload_str)
        canonical_str = json.dumps(data, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        return hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()
    except Exception:
        # Fallback to direct string hashing if json parsing fails
        return hashlib.sha256(payload_str.encode('utf-8')).hexdigest()

def send_delivery_report(event_hash: str, status: str, reason: str = None):
    """
    Publishes a status report to Replay Manager queue 'log-events'.
    This call is fail-safe; network/broker problems will not halt your service.
    
    Statuses:
      - 'SUCCESS': Message processed and action (e.g., SMTP email sent) succeeded.
      - 'FAILED': Processing encountered an error (e.g., SMTP crash, connection timeout).
      - 'SKIPPED': Business logic skipped processing (e.g., comment is private).
    """
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Build the payload
        report = {
            "is_report": True,
            "event_hash": event_hash,
            "status": status,
            "error_message": reason,
            "routing_key": SENDER_QUEUE_NAME
        }
        
        # Publish to the Replay Manager logging queue
        channel.basic_publish(
            exchange='',
            routing_key='log-events',
            body=json.dumps(report),
            properties=pika.BasicProperties(
                delivery_mode=2 # persistent
            )
        )
        connection.close()
        logger.info("action=send_delivery_report status=success event_hash=%s status=%s", event_hash, status)
    except Exception as e:
        # Keep the delivery report fail-safe to protect SLA!
        logger.error("action=send_delivery_report status=failed error=\"%s\"", str(e))

# Example usage in consumer callback
def on_message_callback(ch, method, properties, body):
    body_str = body.decode('utf-8', errors='ignore')
    
    # 1. Compute canonical hash before anything else
    event_hash = get_canonical_hash(body_str)
    
    try:
        # --- Simulating Business Filters (Soft-Drops) ---
        payload = json.loads(body_str)
        if payload.get("is_private") == 1:
            logger.info("Skipping comment because it is private.")
            
            # Report SKIPPED status with a descriptive reason
            send_delivery_report(event_hash, "SKIPPED", "Comment is private (is_private=1)")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
            
        # --- Simulating Outbound Action ---
        # e.g., send_email(payload.get('subject'), payload.get('body'))
        logger.info("Processing message and sending outbound notification...")
        
        # Report SUCCESS status
        send_delivery_report(event_hash, "SUCCESS")
        
    except Exception as ex:
        logger.error("Failed to process event: %s", str(ex))
        
        # Report FAILED status with the exception traceback/details
        send_delivery_report(event_hash, "FAILED", str(ex))
        
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)
