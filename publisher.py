import os
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

# Initialize Publisher client
publisher = pubsub_v1.PublisherClient()

# Get the project ID from environment variables
project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'pubsub-example-436820')

# Define the fully-qualified topic name
topic_name = f'projects/{project_id}/topics/test-topic'

# Try to create the topic
try:
    publisher.create_topic(name=topic_name)
    print(f"Topic {topic_name} created.")
except AlreadyExists:
    print(f"Topic {topic_name} already exists, skipping creation.")

# Number of messages to publish
num_messages = 1000

# Function to publish messages
def publish_messages():
    futures = []
    for i in range(num_messages):
        data = f'Message {i}'.encode('utf-8')  # Convert the message to bytes
        future = publisher.publish(topic_name, data, spam='eggs')
        futures.append(future)
        print(f"Published message {i}")

    # Ensure all messages are successfully published
    for future in futures:
        print(f"Message result: {future.result()}")

# Publish the messages
publish_messages()
