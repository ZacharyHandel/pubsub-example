import os
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

# Set project ID and topic/subscription names
project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'pubsub-example-436820')

# Define fully-qualified topic and subscription names
topic_name = f'projects/{project_id}/topics/test-topic'
subscription_name = f'projects/{project_id}/subscriptions/test-sub'

# Callback function to process received messages
def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()
    print("Acknowledged message.")

# Initialize Subscriber client
with pubsub_v1.SubscriberClient() as subscriber:
    # Try to create the subscription
    try:
        subscriber.create_subscription(
            name=subscription_name, topic=topic_name
        )
        print(f"Subscription {subscription_name} created.")
    except AlreadyExists:
        print(f"Subscription {subscription_name} already exists, skipping creation.")

    # Subscribe to the topic and process messages
    future = subscriber.subscribe(subscription_name, callback)
    try:
        print(f"Listening for messages on {subscription_name}...")
        future.result()  # Blocks and waits for messages indefinitely
    except KeyboardInterrupt:
        future.cancel()
        print("Stopped listening for messages.")
