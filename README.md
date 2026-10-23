# Order Analytics Service

The Order Analytics Service is part of the CST8915 Midterm Project.

Its purpose is to consume order messages from RabbitMQ and provide REST API endpoints for viewing order history and calculated analytics.

## Technology

- Python
- Flask
- RabbitMQ
- Pika
- Gunicorn

## Default Port

```text
4000
```

## Configuration

The service uses environment variables for configuration.

```text
PORT
RABBITMQ_CONNECTION_STRING
```

A safe template is provided in:

```text
.env.example
```

The real `.env` file is ignored by Git and should not contain credentials that are committed to the repository.

## Current Endpoint

### Health Check

```http
GET /health
```

Example response:

```json
{
  "status": "healthy",
  "service": "order-analytics-service"
}
```

## Planned Endpoints

The completed service will provide:

```http
GET /health
GET /orders
GET /analytics/summary
GET /analytics/products
GET /analytics/top-products
```

## RabbitMQ Integration

The service will consume order messages from the durable RabbitMQ queue:

```text
order_queue
```

RabbitMQ connectivity will be configured using:

```text
RABBITMQ_CONNECTION_STRING
```

Messages will be acknowledged only after they have been successfully processed and stored by the analytics service.