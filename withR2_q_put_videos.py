#!/usr/bin/env python3
"""
Publish video URLs from videos_1.json to a RabbitMQ queue as tasks.
Queue is durable; messages are persistent. Consumers should use manual ack (auto_ack=False).
"""

import json
import os
import sys

import pika
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/")
RABBITMQ_QUEUE = os.environ["RABBITMQ_QUEUE"]

VIDEOS_JSON = "videos_1.json"


def main():
    with open(VIDEOS_JSON, "r") as f:
        links = json.load(f)

    if not links:
        print("No links in", VIDEOS_JSON)
        return

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
    )

    conn = pika.BlockingConnection(parameters)
    channel = conn.channel()

    # Durable queue: survives broker restart. Use with manual ack on consumers.
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    published = 0
    for url in links:
        if not url or not isinstance(url, str):
            continue
        channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=url.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,  # persist message
            ),
        )
        published += 1

    conn.close()
    print(f"Published {published} tasks to queue '{RABBITMQ_QUEUE}'.")


if __name__ == "__main__":
    main()
    sys.exit(0)
