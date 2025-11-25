import pika
import json
import os
import logging
import time
from datetime import datetime

# Configure Logging
logger = logging.getLogger(__name__)

class RemoteDispatcher:
    def __init__(self, rabbit_dict):
        self.rabbit_dict = rabbit_dict
        print("\n ...................................", self.rabbit_dict['user'], self.rabbit_dict['pass'], self.rabbit_dict['host'], self.rabbit_dict['port'],
            self.rabbit_dict['vhost'])
        self.credentials = pika.PlainCredentials(self.rabbit_dict['user'], self.rabbit_dict['pass'])
        self.parameters = pika.ConnectionParameters(
            host=self.rabbit_dict['host'],
            port=self.rabbit_dict['port'],
            virtual_host=self.rabbit_dict['vhost'],
            credentials=self.credentials
        )
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        """Attempts to connect with a retry loop."""
        while True:
            try:
                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()
                
                # Enable Publisher Confirms (Critical for Data Safety)
                self.channel.confirm_delivery()
                
                # self.channel.exchange_declare(
                #     exchange=self.rabbit_dict['exchange'], 
                #     exchange_type='direct', 
                #     durable=True
                # )
                logger.info("✅ Connected to RabbitMQ with Publisher Confirms")
                return
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"❌ Connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def send_task(self, file_path, watching_dir):
        """
        Sends message with retry logic and confirmation checking.
        """
        payload = self._create_payload(file_path, watching_dir)
        if not payload: return False

        message_body = json.dumps(payload)

        if file_path.lower().endswith('.dav'):
            target_routing_key = self.rabbit_dict['routing_key_vid']
        else:
            # Assumes everything else allowed (jpg, png) is an image
            target_routing_key = self.rabbit_dict['routing_key_img']
        
        # Retry loop for sending
        retries = 3
        for attempt in range(retries):
            try:
                if self.connection is None or self.connection.is_closed:
                    logger.warning("⚠️ Connection lost. Reconnecting...")
                    self._connect()

                # Publish
                self.channel.basic_publish(
                    exchange=self.rabbit_dict['exchange'],
                    routing_key=target_routing_key,
                    body=message_body,
                    properties=pika.BasicProperties(
                        delivery_mode=2, # Persistent
                        content_type='application/json'
                    )
                )
                
                logger.info(f"🚀 Sent to MQ: {os.path.basename(file_path)}")
                return True # Success

            except (pika.exceptions.UnroutableError, pika.exceptions.AMQPError) as e:
                logger.warning(f"⚠️ Publish failed (Attempt {attempt+1}/{retries}): {e}")
                time.sleep(2) # Wait before retry
                self._connect() # Force reconnect

        logger.error(f"❌ FAILED to send {file_path} after {retries} attempts.")
        return False

    def _create_payload(self, file_path, watching_dir):
        try:
            file_stat = os.stat(file_path)
            event_time = datetime.now().astimezone().isoformat()
            payload = {
                "FilePath": file_path,
                "WatchingDir": watching_dir,
                "EventTime": event_time,
                "EventSrc": "shell",
                "ServerId": self.rabbit_dict['server_id']
                }
            return payload
        except OSError:
            return None
            
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()