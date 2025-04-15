# RabbitMQ Consumer Template

Production-ready RabbitMQ consumer implementation in Python with best practices for reliability, monitoring, and security.

## Features

- 🛡️ **Auto-reconnection** with exponential backoff
- 📊 **Prometheus metrics** integration
- 🔒 **SSL/TLS encryption** support
- ♻️ Dead Letter Queue handling
- 📈 Prefetch control for load balancing
- 📝 Structured logging
- 🚦 Graceful shutdown handling
- 🏗️ Quorum queues support (RabbitMQ 3.8+)
- 🧩 Modular architecture

## Requirements

- Python 3.8+
- RabbitMQ 3.8+
- Libraries:
  - `pika==1.3.2`
  - `prometheus-client==0.17.0`

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `AMQP_URL` | RabbitMQ connection URL | `amqps://user:pass@host:5671/vhost` |
| `QUEUE_NAME` | Target queue name | `orders.prod` |
| `CA_CERT_PATH` | Path to CA certificate | `/certs/ca.pem` |
| `CLIENT_CERT_PATH` | Path to client certificate | `/certs/client.crt` |
| `CLIENT_KEY_PATH` | Path to client private key | `/certs/client.key` |

### SSL Setup
1. Generate certificates:
```bash
openssl req -x509 -newkey rsa:4096 -keyout client.key -out client.crt -days 365