"""RabbitMQ Consumer"""

import logging
from logging.handlers import RotatingFileHandler
import ssl
import signal
import time
import json
import pika
import requests

from pika.exceptions import AMQPError
from prometheus_client import Counter, Histogram, start_http_server

import config


# ==================== #
#   Custom Exceptions  #
# ==================== #
class TransientError(Exception):
    """Временная ошибка, можно повторить попытку"""


class CriticalError(Exception):
    """Фатальная ошибка, требуется вмешательство"""


# ==================== #
#   Metrics Setup      #
# ==================== #
PROCESSED_MSG = Counter("processed_messages", "Total processed messages")
FAILED_MSG = Counter("failed_messages", "Total failed messages", ["reason"])
PROCESS_TIME = Histogram("process_time_seconds", "Message processing time")


# ==================== #
#   Consumer Class     #
# ==================== #
class RabbitConsumer:
    """RabbitMQ Consumer with DLX and Batch Processing"""

    def __init__(self, amqp_url, queue_name):
        self._connection = None
        self._channel = None
        self._url = amqp_url
        self._queue = queue_name
        self._reconnect_delay = 1
        self._max_reconnect_delay = 300
        self._consumer_tag = None
        self._running = False
        self._login_message_batch = []

        self._ssl_enabled = False
        if config.CA_CERT_PATH:
            self._ssl_context = ssl.create_default_context(cafile=config.CA_CERT_PATH)
            self._ssl_context.load_cert_chain(
                certfile=config.CLIENT_CERT_PATH if config.CLIENT_CERT_PATH else "",
                keyfile=config.CLIENT_KEY_PATH,
            )
            self._ssl_enabled = True

    def _connect(self):
        """Установка соединения"""
        logging.info("Connecting to RabbitMQ")
        params = pika.URLParameters(self._url)

        if self._ssl_enabled:
            params.ssl_options = pika.SSLOptions(self._ssl_context)

        params.socket_timeout = int(config.SOCKET_TIMEOUT)
        params.connection_attempts = int(config.CONNECTION_ATTEMPTS)
        params.retry_delay = int(config.RETRY_DELAY)

        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        if channel is None:
            raise RuntimeError("Failed to create channel.")

        return connection, channel

    def _reconnect(self):
        """Механизм переподключения"""
        while self._running:
            try:
                self._connection, self._channel = self._connect()
                if self._channel is None:
                    raise RuntimeError("Channel not initialized.")
                self._setup_infrastructure()
                self._start_consuming()
                self._schedule_batch_send()
                self._reconnect_delay = 1
                return
            except Exception as e:
                logging.error(
                    "Reconnect failed: %s. Retry in %s seconds", e, self._reconnect_delay
                )
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )

    def _setup_infrastructure(self):
        """Настройка очередей"""
        if self._channel is None:
            raise RuntimeError("Channel is None in _setup_infrastructure")

        self._channel.exchange_declare(
            exchange="dlx", exchange_type="direct", durable=True
        )
        self._channel.queue_declare(
            queue="dlq", durable=True, arguments={"x-queue-mode": "lazy"}
        )
        self._channel.queue_bind("dlq", "dlx", routing_key="dlq")

        self._channel.queue_declare(
            queue=self._queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "dlx",
                "x-dead-letter-routing-key": "dlq",
                "x-queue-type": "classic",
            },
        )
        self._channel.basic_qos(prefetch_count=int(config.PREFETCH_COUNT))

    def _start_consuming(self):
        """Запуск потребления сообщений"""
        if self._channel is None:
            raise RuntimeError("Channel is None in _start_consuming")
        self._consumer_tag = self._channel.basic_consume(
            queue=self._queue, on_message_callback=self._on_message, auto_ack=False
        )

    def _on_message(self, channel, method, properties, body):
        """Обработка сообщения"""
        start_time = time.time()
        try:
            message = json.loads(body)
            key = message.get("key", "")
            if key.startswith("login:"):
                self._login_message_batch.append(
                    {
                        "message": message,
                        "delivery_tag": method.delivery_tag,
                        "start_time": start_time,
                    }
                )
            elif key.startswith("scheme:vector"):
                self._handle_vector(message)
                channel.basic_ack(method.delivery_tag)
                PROCESSED_MSG.inc()
                PROCESS_TIME.observe(time.time() - start_time)
            else:
                raise CriticalError("Неправильный ключ")

        except TransientError as e:
            logging.warning("Transient error: %s", e)
            channel.basic_nack(method.delivery_tag, requeue=True)
            FAILED_MSG.labels(reason="transient").inc()

        except CriticalError as e:
            logging.error("Critical error: %s", e)
            channel.basic_nack(method.delivery_tag, requeue=False)
            FAILED_MSG.labels(reason="critical").inc()
            self._send_to_dlq(body, str(e))

        except (AMQPError, RuntimeError) as e:
            logging.exception("Unexpected error while processing message")
            channel.basic_nack(method.delivery_tag, requeue=False)
            FAILED_MSG.labels(reason="unexpected").inc()
            self._send_to_dlq(body, str(e))

    def _handle_vector(self, message):
        """
        Обработка сообщений 'scheme:vector' — триггерит загрузку промтов из Redis в Milvus через /upload_promts_data.
        """
        try:
            response = requests.post(
                "http://192.168.111.151:8080/upload_promts_data",
            )
            if response.status_code != 200:
                raise TransientError(
                    f"Ошибка вызова /upload_promts_data: {response.status_code} - {response.text}"
                )
            logging.info("Триггер загрузки промтов из Redis в Milvus выполнен успешно.")
        except Exception as e:
            logging.error("Ошибка обработки scheme:vector: %s", e)
            raise TransientError(f"Ошибка обработки scheme:vector: {e}")

    def _handle_login(self, message):
        login = message.get("key")[6:]
        value = message.get("value")
        house_id = str(value.get("houseId"))
        address = value.get("address")

        data = {
            "login": login,
            "address": address,
            "houseId": house_id,
        }

        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(
                "http://192.168.111.151:8080/v1/addresses",
                json=data,
                headers=headers,
                timeout=10,
            )
            if response.status_code != 200:
                raise TransientError(
                    f"Ошибка отправки данных в Милвус: {response.status_code} - {response.text}"
                )
        except requests.exceptions.RequestException as e:
            raise TransientError(f"Request exception: {e}") from e

    def _send_to_dlq(self, body, error):
        """Заглушка для отправки в DLQ (можно логировать)"""
        logging.warning("Message sent to DLQ. Reason: %s | Body: %s", error, body)

    def _send_batch(self):
        """Отправка накопленных сообщений 'login:' одним запросом"""
        if not self._login_message_batch:
            logging.info("No messages to send in batch")
            self._schedule_batch_send()
            return

        start_time = time.time()
        batch_data = [
            {
                "login": item["message"]["key"][6:],
                "address": item["message"]["value"].get("address"),
                "houseId": str(item["message"]["value"].get("houseId")),
            }
            for item in self._login_message_batch
        ]

        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(
                "http://192.168.111.151:8080/v1/addresses",
                json=[batch_data],
                headers=headers,
            )
            if response.status_code != 200:
                raise TransientError(
                    f"Ошибка отправки батча: {response.status_code} - {response.text}"
                )

            for item in self._login_message_batch:
                if self._channel:
                    self._channel.basic_ack(item["delivery_tag"])
                else:
                    logging.error("Channel is None, cannot acknowledge message")
            logging.info("Batch sent successfully: %s items", len(batch_data))
            PROCESSED_MSG.inc(len(batch_data))
            PROCESS_TIME.observe(time.time() - start_time)
            self._login_message_batch = []

        except TransientError as e:
            logging.warning("Transient error in batch: %s", e)
            FAILED_MSG.labels(reason="transient").inc()

        except requests.exceptions.RequestException as e:
            logging.error("Request error in batch: %s", e)
            for item in self._login_message_batch:
                if self._channel:
                    if self._channel:
                        if self._channel:
                            self._channel.basic_nack(item["delivery_tag"], requeue=False)
                        else:
                            logging.error("Channel is None, cannot nack message")
                    else:
                        logging.error("Channel is None, cannot nack message")
                else:
                    logging.error("Channel is None, cannot nack message")
                self._send_to_dlq(json.dumps(item["message"]), str(e))
            FAILED_MSG.labels(reason="request_error").inc(len(self._login_message_batch))
            self._login_message_batch = []

        except (ValueError, KeyError, TypeError) as e:
            logging.error("Unexpected error in batch: %s", e)
            for item in self._login_message_batch:
                if self._channel:
                    self._channel.basic_nack(item["delivery_tag"], requeue=False)
                else:
                    logging.error("Channel is None, cannot nack message")
                self._send_to_dlq(json.dumps(item["message"]), str(e))
            FAILED_MSG.labels(reason="unexpected").inc(len(self._login_message_batch))
            self._login_message_batch = []

        self._schedule_batch_send()

    def _schedule_batch_send(self):
        if self._connection:
            self._connection.call_later(30, self._send_batch)

    def start(self):
        self._running = True
        logging.info("Starting consumer")
        self._reconnect()
        try:
            if self._channel:
                self._channel.start_consuming()
        except (AMQPError, RuntimeError) as e:
            logging.error("Consuming error: %s", e)
            if self._running:
                self._reconnect()

    def stop(self):
        self._running = False
        if self._channel:
            self._channel.stop_consuming()
        if self._connection:
            self._connection.close()


# ==================== #
#   Main Execution     #
# ==================== #
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            RotatingFileHandler(config.LOG_PATH, maxBytes=10 * 1024 * 1024, backupCount=5),
            logging.StreamHandler(),
        ],
    )

    start_http_server(int(config.METRICS_PORT))

    consumer = RabbitConsumer(amqp_url=config.AMQP_URL, queue_name=config.QUEUE_NAME)

    signal.signal(signal.SIGINT, lambda s, f: consumer.stop())
    signal.signal(signal.SIGTERM, lambda s, f: consumer.stop())

    while True:
        try:
            consumer.start()
        except KeyboardInterrupt:
            break
        except (AMQPError, RuntimeError) as e:
            logging.error("Consumer crash: %s, restarting in 10s", e)
            time.sleep(10)
