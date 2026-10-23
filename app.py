# AI was used to generate the python code below.
import json
import os
import threading
import time
from collections import defaultdict

import pika
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS


# Load environment variables from the local .env file.
# In Azure, these values will come from App Service application settings.
load_dotenv()


# Create the Flask application.
app = Flask(__name__)

# Allow requests from other origins such as the Store Front application.
CORS(app)


# Store consumed orders in memory.
# Data will reset whenever the service restarts.
orders = []

# Protect the shared orders list because the RabbitMQ consumer
# runs in a background thread while Flask handles API requests.
orders_lock = threading.Lock()


@app.route("/health", methods=["GET"])
def health():
    """
    Health check endpoint used to confirm that the service is running.
    """

    return jsonify({
        "status": "healthy",
        "service": "order-analytics-service"
    })


@app.route("/orders", methods=["GET"])
def get_orders():
    """
    Return all orders successfully consumed from RabbitMQ.
    """

    with orders_lock:
        return jsonify(orders)


@app.route("/analytics/summary", methods=["GET"])
def analytics_summary():
    """
    Return overall order statistics.

    The summary includes:
    - total number of orders
    - total quantity of products ordered
    - total revenue
    - average order value
    """

    with orders_lock:
        current_orders = list(orders)

    total_orders = len(current_orders)
    total_items = 0
    total_revenue = 0.0

    for order in current_orders:
        quantity = int(order.get("quantity", 1))
        total = float(order.get("total", 0))

        total_items += quantity
        total_revenue += total

    average_order_value = (
        total_revenue / total_orders
        if total_orders > 0
        else 0
    )

    return jsonify({
        "total_orders": total_orders,
        "total_items": total_items,
        "total_revenue": round(total_revenue, 2),
        "average_order_value": round(average_order_value, 2)
    })


@app.route("/analytics/products", methods=["GET"])
def analytics_products():
    """
    Aggregate order statistics by product.

    Each product includes:
    - number of orders
    - total quantity ordered
    - total revenue
    """

    product_stats = defaultdict(
        lambda: {
            "orders": 0,
            "quantity": 0,
            "revenue": 0.0
        }
    )

    with orders_lock:
        current_orders = list(orders)

    for order in current_orders:
        product = order.get("product", "Unknown")
        quantity = int(order.get("quantity", 1))
        total = float(order.get("total", 0))

        product_stats[product]["orders"] += 1
        product_stats[product]["quantity"] += quantity
        product_stats[product]["revenue"] += total

    results = []

    for product, stats in product_stats.items():
        results.append({
            "product": product,
            "orders": stats["orders"],
            "quantity": stats["quantity"],
            "revenue": round(stats["revenue"], 2)
        })

    return jsonify(results)


@app.route("/analytics/top-products", methods=["GET"])
def analytics_top_products():
    """
    Return products ranked by total quantity ordered.
    """

    product_quantities = defaultdict(int)

    with orders_lock:
        current_orders = list(orders)

    for order in current_orders:
        product = order.get("product", "Unknown")
        quantity = int(order.get("quantity", 1))

        product_quantities[product] += quantity

    ranked_products = sorted(
        product_quantities.items(),
        key=lambda item: item[1],
        reverse=True
    )

    results = [
        {
            "product": product,
            "quantity": quantity
        }
        for product, quantity in ranked_products
    ]

    return jsonify(results)


def process_order(channel, method, properties, body):
    """
    Process one message received from RabbitMQ.

    The message is decoded from JSON and stored in memory.
    RabbitMQ is acknowledged only after successful processing.
    """

    try:
        # Convert the RabbitMQ message body from bytes into a Python dictionary.
        order = json.loads(body.decode("utf-8"))

        # Store the processed order in memory.
        with orders_lock:
            orders.append(order)

        print(f"Consumed order: {order}")

        # Remove the message from RabbitMQ only after successful processing.
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as error:
        print(f"Failed to process order: {error}")

        # Reject the failed message and return it to the queue.
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )


def consume_orders():
    """
    Continuously connect to RabbitMQ and consume messages
    from the durable order_queue.

    If RabbitMQ is temporarily unavailable, the service waits
    and retries instead of stopping the Flask application.
    """

    rabbitmq_url = os.getenv("RABBITMQ_CONNECTION_STRING")

    if not rabbitmq_url:
        print("RABBITMQ_CONNECTION_STRING is not configured.")
        return

    while True:
        try:
            # Connect to RabbitMQ using the environment-based connection string.
            parameters = pika.URLParameters(rabbitmq_url)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Match the durable queue created by the Order Service.
            channel.queue_declare(
                queue="order_queue",
                durable=True
            )

            # Process only one unacknowledged message at a time.
            channel.basic_qos(prefetch_count=1)

            # Register the callback used whenever a new order arrives.
            channel.basic_consume(
                queue="order_queue",
                on_message_callback=process_order,
                auto_ack=False
            )

            print("Connected to RabbitMQ. Waiting for orders...")

            # Continuously wait for new RabbitMQ messages.
            channel.start_consuming()

        except Exception as error:
            print(f"RabbitMQ connection failed: {error}")
            print("Retrying in 5 seconds...")

            time.sleep(5)


def start_consumer():
    """
    Start the RabbitMQ consumer in a daemon background thread
    so Flask can serve API requests at the same time.
    """

    consumer_thread = threading.Thread(
        target=consume_orders,
        daemon=True
    )

    consumer_thread.start()


if __name__ == "__main__":
    # Start the RabbitMQ consumer before starting Flask.
    start_consumer()

    # Read the service port from the environment.
    # Port 4000 is used as the default for local development.
    port = int(os.getenv("PORT", "4000"))

    # Listen on all interfaces so the service can later run inside Azure.
    app.run(
        host="0.0.0.0",
        port=port,
        use_reloader=False
    )