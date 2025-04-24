from confluent_kafka.admin import AdminClient
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def error_callback(err):
    logger.error(f"Kafka error: {err}")


# Конфигурация административного клиента
admin_conf = {
    "bootstrap.servers": "rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:9091,"
    "rc1b-mq4ajmtffresd11r.mdb.yandexcloud.net:9091,"
    "rc1d-2msrqk83jn1vnarh.mdb.yandexcloud.net:9091",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "producer-consumer",
    "sasl.password": "1qazXSW@",
    "ssl.ca.location": "/Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
    "error_cb": error_callback,
}

# Создание административного клиента
admin_client = AdminClient(admin_conf)


def describe_topic(topic_name):
    """Получение детальной информации о топике"""
    try:
        # Получаем метаданные кластера
        cluster_metadata = admin_client.list_topics(timeout=10)

        # Проверяем существование топика
        if topic_name not in cluster_metadata.topics:
            logger.error(f"Topic '{topic_name}' does not exist")
            return

        # Получаем информацию о топике
        topic_metadata = cluster_metadata.topics[topic_name]

        # Выводим основную информацию
        print(f"\nTopic: {topic_name}")
        print(f"PartitionCount: {len(topic_metadata.partitions)}")
        print(f"ReplicationFactor: {len(topic_metadata.partitions[0].replicas)}")

        # Выводим информацию по каждой партиции
        for partition, metadata in topic_metadata.partitions.items():
            print(f"\tPartition: {partition}")
            print(f"\tLeader: {metadata.leader}")
            print(f"\tReplicas: {metadata.replicas}")
            print("-" * 50)

    except Exception as e:
        logger.error(f"Failed to describe topic: {e}")


# Использование
describe_topic("test-topic")
