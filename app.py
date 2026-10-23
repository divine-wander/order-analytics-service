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


# Load local environment variables.
# Azure App Service uses its configured environment variables instead.
load_dotenv()


# Create the Flask application.
app = Flask(__name__)
CORS(app)


# Orders are stored in memory for this project.
# The list resets whenever the service restarts.
orders = []

# Protect the shared list because Flask and the RabbitMQ
# consumer run in separate threads.
orders_lock = threading.Lock()


def normalize_order(order):
    """
    Convert incoming orders into one consistent structure.

    The REST Client sends:
        product: "Cat Food"
        price: 34.99
        total: 69.98

    The Vue Store Front sends:
        product: {
            id: 2,
            name: "Cat Food",
            price: 34.99
        }
        totalPrice: 69.98

    Both formats are normalized here.
    """

    product = order.get("product", "Unknown")

    # Store Front sends product as an object.
    if isinstance(product, dict):
        product_name = product.get("name", "Unknown")
        price = float(product.get("price", 0))
        product_id = product.get("id")
    else:
        # REST Client may send product as a plain string.
        product_name = str(product)
        price = float(order.get("price", 0))
        product_id = order.get("product_id")

    quantity = int(order.get("quantity", 1))

    # Support both payload formats.
    total = order.get("total")

    if total is None:
        total = order.get("totalPrice")

    # If neither total field exists, calculate it.
    if total is None:
        total = price * quantity

    normalized_order = {
        "product": product_name,
        "quantity": quantity,
        "price": round(price, 2),
        "total": round(float(total), 2)
    }

    # Preserve product ID when one exists.
    if product_id is not None:
        normalized_order["product_id"] = product_id

    return normalized_order


@app.route("/health", methods=["GET"])
def health():
    """
    Confirm that the Order Analytics Service is running.
    """

    return jsonify({
        "status": "healthy",
        "service": "order-analytics-service"
    })


@app.route("/orders", methods=["GET"])
def get_orders():
    """
    Return all orders consumed from RabbitMQ.
    """

    with orders_lock:
        return jsonify(list(orders))


@app.route("/analytics/summary", methods=["GET"])
def analytics_summary():
    """
    Return overall analytics:
    - total orders
    - total items
    - total revenue
    - average order value
    """

    with orders_lock:
        current_orders = list(orders)

    total_orders = len(current_orders)

    total_items = sum(
        order["quantity"]
        for order in current_orders
    )

    total_revenue = sum(
        order["total"]
        for order in current_orders
    )

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
    Aggregate analytics for each product.

    Each product includes:
    - number of orders
    - total quantity
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
        product_name = order["product"]

        product_stats[product_name]["orders"] += 1
        product_stats[product_name]["quantity"] += order["quantity"]
        product_stats[product_name]["revenue"] += order["total"]

    results = []

    for product_name, stats in product_stats.items():
        results.append({
            "product": product_name,
            "orders": stats["orders"],
            "quantity": stats["quantity"],
            "revenue": round(stats["revenue"], 2)
        })

    return jsonify(results)


@app.route("/analytics/top-products", methods=["GET"])
def analytics_top_products():
    """
    Rank products by total quantity ordered.
    """

    product_quantities = defaultdict(int)

    with orders_lock:
        current_orders = list(orders)

    for order in current_orders:
        product_quantities[order["product"]] += order["quantity"]

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
    Process one RabbitMQ message.

    The order is normalized and stored before RabbitMQ
    receives an acknowledgement.
    """

    try:
        # Decode RabbitMQ message.
        raw_order = json.loads(
            body.decode("utf-8")
        )

        # Convert either supported order format
        # into our standard analytics structure.
        order = normalize_order(raw_order)

        # Store the normalized order in memory.
        with orders_lock:
            orders.append(order)

        print(f"Consumed order: {order}")

        # Acknowledge only after successful processing.
        channel.basic_ack(
            delivery_tag=method.delivery_tag
        )

    except Exception as error:
        print(f"Failed to process order: {error}")

        # Return failed messages to RabbitMQ.
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True
        )


def consume_orders():
    """
    Continuously connect to RabbitMQ and consume orders.
    """

    rabbitmq_url = os.getenv(
        "RABBITMQ_CONNECTION_STRING"
    )

    if not rabbitmq_url:
        print(
            "RABBITMQ_CONNECTION_STRING "
            "is not configured."
        )
        return

    while True:
        try:
            parameters = pika.URLParameters(
                rabbitmq_url
            )

            connection = pika.BlockingConnection(
                parameters
            )

            channel = connection.channel()

            # Ensure the durable queue exists.
            channel.queue_declare(
                queue="order_queue",
                durable=True
            )

            # Handle one unacknowledged message
            # at a time.
            channel.basic_qos(
                prefetch_count=1
            )

            channel.basic_consume(
                queue="order_queue",
                on_message_callback=process_order,
                auto_ack=False
            )

            print(
                "Connected to RabbitMQ. "
                "Waiting for orders..."
            )

            channel.start_consuming()

        except Exception as error:
            print(
                f"RabbitMQ connection failed: {error}"
            )

            print(
                "Retrying in 5 seconds..."
            )

            time.sleep(5)


def start_consumer():
    """
    Run the RabbitMQ consumer in a background thread
    while Flask handles HTTP requests.
    """

    consumer_thread = threading.Thread(
        target=consume_orders,
        daemon=True
    )

    consumer_thread.start()


# Start the consumer when Gunicorn imports app.py in Azure.
start_consumer()


if __name__ == "__main__":
    # Default local development port.
    port = int(
        os.getenv("PORT", "4000")
    )

    app.run(
        host="0.0.0.0",
        port=port,
        use_reloader=False
    )