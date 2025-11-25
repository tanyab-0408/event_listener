import pika
import json
import time
from datetime import datetime

# --- Configuration (same as consumer) ---
SPHOST_ID = "frs212"
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_VHOST = "pulse"
RABBITMQ_USER = f"u.{SPHOST_ID}.pulse.admin"
RABBITMQ_PASS = "192.168.10.212"
QUEUE_NAME = f"q.pulse.error.file.{SPHOST_ID}"

# --- Connection Setup ---
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    virtual_host=RABBITMQ_VHOST,
    credentials=credentials
)

def send_test_message():
    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Create a test message
        test_message = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "file.upload",
            "filename": f"test_file_{int(time.time())}.txt",
            "file_size": 1024,
            "host_id": SPHOST_ID,
            "message": "This is a test message from producer"
        }
        
        # Convert to JSON and publish
        message_body = json.dumps(test_message)
        
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json'
            )
        )
        
        print(f"✓ Message sent successfully at {datetime.now()}")
        print(f"Message content: {message_body}")
        connection.close()
        return True
        
    except Exception as e:
        print(f"✗ Failed to send message: {e}")
        return False

def send_multiple_messages(count=5, delay=2):
    """Send multiple test messages with delay between them"""
    for i in range(count):
        print(f"\nSending message {i+1}/{count}...")
        send_test_message()
        if i < count - 1:  # Don't sleep after the last message
            time.sleep(delay)

if __name__ == "__main__":
    print("RabbitMQ Producer Test")
    print("=" * 50)
    print(f"Target Queue: {QUEUE_NAME}")
    print(f"Virtual Host: {RABBITMQ_VHOST}")
    print(f"Username: {RABBITMQ_USER}")
    print("=" * 50)
    
    # Ask user how many messages to send
    try:
        count = int(input("How many test messages to send? (default: 1) ") or "1")
        if count > 1:
            send_multiple_messages(count=count, delay=2)
        else:
            send_test_message()
    except ValueError:
        print("Invalid input. Sending 1 message.")
        send_test_message()
    
    print("\nTest completed. Check your consumer to see if messages were received.")