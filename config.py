from dotenv import load_dotenv
import os
load_dotenv()

# ==================== #
#   RabbitMQ Settings  #
# ==================== #
AMQP_URL = os.getenv('AMQP_URL')
QUEUE_NAME = os.getenv('QUEUE_NAME')

# ==================== #
#   SSL Configuration  #
# ==================== #
CA_CERT_PATH = os.getenv('CA_CERT_PATH')
CLIENT_CERT_PATH = os.getenv('CLIENT_CERT_PATH')
CLIENT_KEY_PATH = os.getenv('CLIENT_KEY_PATH')

# ==================== #
#   Network Settings   #
# ==================== #
SOCKET_TIMEOUT = int(os.getenv('SOCKET_TIMEOUT', 10))
CONNECTION_ATTEMPTS = int(os.getenv('CONNECTION_ATTEMPTS', 3))
RETRY_DELAY = os.getenv('RETRY_DELAY', '5')
PREFETCH_COUNT = os.getenv('PREFETCH_COUNT', '100')

# ==================== #
#   Monitoring         #
# ==================== #
METRICS_PORT = os.getenv('METRICS_PORT', 8000)
LOG_PATH = os.path.join(os.path.dirname(__file__), 'logs', 'consumer.log')


MILVUS_LOGIN_ENDPOINT = os.getenv('MILVUS_LOGIN_ENDPOINT', 'http://192.168.111.151:8080/v1/addresses')
MILVUS_SCHEME_ENDPOINT= os.getenv('MILVUS_SCHEME_ENDPOINT', 'http://192.168.111.151:8080/v1/promts')