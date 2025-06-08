import logging
import ssl
import signal
import time
import json
import pika
from prometheus_client import Counter, Histogram, start_http_server
import config
import requests

# ==================== #
#   Custom Exceptions  #
# ==================== #
class TransientError(Exception):
    """Временная ошибка, можно повторить попытку"""
    pass

class CriticalError(Exception):
    """Фатальная ошибка, требуется вмешательство"""
    pass

# ==================== #
#   Metrics Setup      #
# ==================== #
PROCESSED_MSG = Counter('processed_messages', 'Total processed messages')
FAILED_MSG = Counter('failed_messages', 'Total failed messages', ['reason'])
PROCESS_TIME = Histogram('process_time_seconds', 'Message processing time')

# ==================== #
#   Consumer Class     #
# ==================== #
class RabbitConsumer:
    def __init__(self, amqp_url, queue_name):
        self._connection = None
        self._channel = None
        self._url = amqp_url
        self._queue = queue_name
        self._reconnect_delay = 1
        self._max_reconnect_delay = 300
        self._consumer_tag = None
        self._running = False
        self._login_message_batch = []  # Список для накопления сообщений "login:"

        # SSL конфигурация
        self._ssl_enabled = False
        if config.CA_CERT_PATH:
            self._ssl_context = ssl.create_default_context(
                cafile=config.CA_CERT_PATH
            )
            self._ssl_context.load_cert_chain(
                certfile=config.CLIENT_CERT_PATH,
                keyfile=config.CLIENT_KEY_PATH
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
        
        return pika.BlockingConnection(params)

    def _reconnect(self):
        """Механизм переподключения"""
        while self._running:
            try:
                self._connection = self._connect()
                self._channel = self._connection.channel()
                self._setup_infrastructure()
                self._start_consuming()
                self._schedule_batch_send()  # Планируем отправку батчей
                self._reconnect_delay = 1
                return
            except Exception as e:
                logging.error(f"Reconnect failed: {e}. Retry in {self._reconnect_delay}s")
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

    def _setup_infrastructure(self):
        """Настройка очередей"""
        # DLX
        self._channel.exchange_declare(exchange='dlx', exchange_type='direct', durable=True)
        self._channel.queue_declare(queue='dlq', durable=True, arguments={'x-queue-mode': 'lazy'})
        self._channel.queue_bind('dlq', 'dlx', routing_key='dlq')

        # Основная очередь
        self._channel.queue_declare(
            queue=self._queue,
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': 'dlq',
                'x-queue-type': 'classic'
            }
        )
        self._channel.basic_qos(prefetch_count=int(config.PREFETCH_COUNT))

    def _start_consuming(self):
        """Запуск потребления сообщений"""
        self._consumer_tag = self._channel.basic_consume(
            queue=self._queue,
            on_message_callback=self._on_message,
            auto_ack=False
        )

    def _on_message(self, channel, method, properties, body):
        """Обработка сообщения"""
        start_time = time.time()
        try:
            message = json.loads(body)
            key = message.get('key', '')
            if key.startswith('login:'):
                self._login_message_batch.append({
                    'message': message,
                    'delivery_tag': method.delivery_tag,
                    'start_time': start_time
                })
            elif key.startswith('scheme:'):
                self._handle_promt(message)
                channel.basic_ack(method.delivery_tag)
                PROCESSED_MSG.inc()
                PROCESS_TIME.observe(time.time() - start_time)
            else:
                raise CriticalError('Неправильный ключ')
        except TransientError as e:
            logging.warning(f"Transient error: {e}")
            channel.basic_nack(method.delivery_tag, requeue=True)
            FAILED_MSG.labels(reason='transient').inc()
        except CriticalError as e:
            logging.error(f"Critical error: {e}")
            channel.basic_nack(method.delivery_tag, requeue=False)
            FAILED_MSG.labels(reason='critical').inc()
            self._send_to_dlq(body, str(e))
        except Exception as e:
            logging.exception("Unexpected error")
            channel.basic_nack(method.delivery_tag, requeue=False)
            FAILED_MSG.labels(reason='unexpected').inc()
            self._send_to_dlq(body, str(e))

    def _handle_promt(self, message):
        """Обработка сообщений 'scheme:'"""
        pass  # Реализуйте логику для "scheme:"

    def _handle_login(self, message):
        """Обработка одного сообщения 'login:' (для возможного использования)"""
        login = message.get('key')[6:]
        value = message.get('value')
        house_id = str(value.get('houseId'))
        address = value.get('address')

        data = {
            'login': login,
            'address': address,
            'houseId': house_id,
        }

        headers = {
            'Content-Type': 'application/json'
        }

        try:
            response = requests.post(
                'http://192.168.111.151:8080/v1/addresses',
                json=data,
                headers=headers
            )
            if not response.status_code == 200:
                raise TransientError(f"Ошибка отправки данных в Милвус: {response.status_code} - {response.text}")
        except Exception as e:
            raise TransientError(f'Ошибка отправки данных в Милвус: {str(e)}')

    def _send_to_dlq(self, body, error):
        """Дополнительные действия с DLQ"""
        pass  # Можно добавить логирование в БД

    def _send_batch(self):
        """Отправка накопленных сообщений 'login:' одним запросом"""
        if not self._login_message_batch:
            self._schedule_batch_send()
            return  # Нечего отправлять

        start_time = time.time()
        # Собираем данные для отправки
        batch_data = [[]]
        for item in self._login_message_batch:
            message = item.get('message')
            login = message.get('key')[6:]  # Убираем префикс "login:"
            value = message.get('value', {})
            batch_data[0].append({
                'login': login,
                'address': value.get('address'),
                'houseId': str(value.get('houseId'))
            })

        headers = {'Content-Type': 'application/json'}

        if not batch_data:
            logging.info(f"Batch sent and acknowledged successfully: {self._login_message_batch} items")
        try:
            print(batch_data)
            logging.info(batch_data)
            response = requests.post(
                'http://192.168.111.151:8080/v1/addresses',
                json=batch_data,
                headers=headers
            )
            if response.status_code != 200:
                raise TransientError(f"Ошибка отправки батча: {response.status_code} - {response.text}")

            for item in self._login_message_batch:
                self._channel.basic_ack(item['delivery_tag'])
            logging.info(f"Batch sent and acknowledged successfully: {len(batch_data)} items")
            PROCESSED_MSG.inc(len(batch_data))
            PROCESS_TIME.observe(time.time() - start_time)
            self._login_message_batch = []

        except TransientError as e:
            logging.warning(f"Transient error in batch: {e}")
            # Сообщения остаются неподтвержденными для повторной обработки
            FAILED_MSG.labels(reason='transient').inc()

        except Exception as e:
            logging.error(f"Critical error in batch: {e}")
            # Отправляем все сообщения в DLQ
            for item in self._login_message_batch:
                self._channel.basic_nack(item['delivery_tag'], requeue=False)
                self._send_to_dlq(json.dumps(item['message']), str(e))
            FAILED_MSG.labels(reason='critical').inc(len(self._login_message_batch))
            self._login_message_batch = []

        # Планируем следующую отправку
        self._schedule_batch_send()

    def _schedule_batch_send(self):
        """Планирование отправки пачки"""
        self._connection.call_later(5, self._send_batch)

    def start(self):
        """Запуск потребителя"""
        self._running = True
        logging.info("Starting consumer")
        self._reconnect()
        try:
            self._channel.start_consuming()
        except Exception as e:
            logging.error(f"Consuming error: {e}")
            if self._running:
                self._reconnect()

    def stop(self):
        """Остановка"""
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
        level=logging.ERROR,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(config.LOG_PATH),
            logging.StreamHandler()
        ]
    )
    
    start_http_server(int(config.METRICS_PORT))
    
    consumer = RabbitConsumer(
        amqp_url=config.AMQP_URL,
        queue_name=config.QUEUE_NAME
    )
    
    signal.signal(signal.SIGINT, lambda s,f: consumer.stop())
    signal.signal(signal.SIGTERM, lambda s,f: consumer.stop())
    
    while True:
        try:
            consumer.start()
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.error(f"Consumer crash: {e}, restarting in 10s")
            time.sleep(10)