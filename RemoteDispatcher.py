import pika
import json
import os
import logging
import time

# Configure Logging
logger = logging.getLogger(__name__)

class RemoteDispatcher:
    """
    Handles RabbitMQ connection and message publishing.
    """
    def __init__(self, amqp_url, exchange_name, routing_key):
        self.amqp_url = amqp_url
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.connection = None
        self.channel = None
        # self._connect()
        

    def _connect(self):
        """Establishes connection to RabbitMQ."""
        try:
            params = pika.URLParameters(self.amqp_url)
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            
            # Declare exchange (durable=True ensures it survives restarts)
            self.channel.exchange_declare(
                exchange=self.exchange_name, 
                exchange_type='direct', 
                durable=True
            )
            logger.info("✅ Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"❌ RabbitMQ Connection Error: {e}")
            self.connection = None

    def send_task(self, file_path):
        """
        Publishes file metadata to RabbitMQ.
        Returns True if successful, False if failed.
        """
        if not self.connection or self.connection.is_closed:
            logger.warning("⚠️ Connection lost. Reconnecting...")
            self._connect()
            if not self.connection:
                return False

        try:
            file_stat = os.stat(file_path)
            
            # Construct the Payload
            payload = {
                "file_path": file_path,
                "file_name": os.path.basename(file_path),
                "file_size_bytes": file_stat.st_size,
                "event_timestamp": time.time()
            }
            
            message_body = json.dumps(payload)

            # Publish with delivery_mode=2 (Persistent messages)
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            
            logger.info(f"🚀 Sent to MQ: {os.path.basename(file_path)}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to publish message: {e}")
            return False

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()