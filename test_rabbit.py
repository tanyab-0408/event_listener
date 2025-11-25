import pika

# --- Configuration ---
SPHOST_ID="frs212"
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_VHOST = "pulse"
RABBITMQ_USER = f"u.{SPHOST_ID}.pulse.admin"
RABBITMQ_PASS = "192.168.10.212"
QUEUE_NAME = f"q.pulse.error.file.{SPHOST_ID}"

# # Connection parameters (same as your .env) '09myWb0NgkjeSGqMvVTt'
# credentials = pika.PlainCredentials('u.ftp213.pulse.admin', '192.168.10.213')
# parameters = pika.ConnectionParameters(
#     host='localhost',
#     port=5672,
#     virtual_host='pulse',
#     credentials=credentials
# )

# # The error queue name - you'll need to find out what it's called
# # It's probably something like: q.pulse.error.file.ftp213
# ERROR_QUEUE = 'q.pulse.error.file.ftp213' 

# --- Connection Setup ---
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    virtual_host=RABBITMQ_VHOST,
    credentials=credentials
)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# --- Message Handler ---
def callback(ch, method, properties, body):
    print(f"Received message: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Start consuming
channel.basic_consume(
    queue=QUEUE_NAME,
    on_message_callback=callback
)

print("Waiting for messages... Press CTRL+C to exit.")
channel.start_consuming()
