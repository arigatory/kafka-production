from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Schema Registry
sr_conf = {
    "url": "https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:443",
    "basic.auth.user.info": "producer-consumer:1qazXSW@",
    "ssl.ca.location": "/Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
}

# Инициализация клиента Schema Registry
try:
    schema_registry_client = SchemaRegistryClient(sr_conf)
except Exception as e:
    logger.error(f"Failed to create Schema Registry client: {e}")
    raise

# Загрузка схем
try:
    with open("schema-key.json") as f:
        key_schema_str = json.dumps(json.load(f))

    with open("schema-value.json") as f:
        value_schema_str = json.dumps(json.load(f))
except Exception as e:
    logger.error(f"Failed to load schemas: {e}")
    raise

# Создание сериализаторов
key_serializer = JSONSerializer(key_schema_str, schema_registry_client)
value_serializer = JSONSerializer(value_schema_str, schema_registry_client)

# Конфигурация продюсера
producer_conf = {
    "bootstrap.servers": "rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:9091,rc1b-mq4ajmtffresd11r.mdb.yandexcloud.net:9091,rc1d-2msrqk83jn1vnarh.mdb.yandexcloud.net:9091",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "producer-consumer",
    "sasl.password": "1qazXSW@",
    "ssl.ca.location": "/Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
}

# Создание продюсера
producer = Producer(producer_conf)


def delivery_report(err, msg):
    """Callback для отчетов о доставке сообщений"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(
            f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
        )


# Создание тестового сообщения
message_key = {"id": "msg-123"}
message_value = {"text": "Тестовое сообщение", "timestamp": datetime.now().isoformat()}

try:
    # Создание контекста сериализации
    key_context = SerializationContext("test-topic", MessageField.KEY)
    value_context = SerializationContext("test-topic", MessageField.VALUE)

    # Отправка сообщения
    producer.produce(
        topic="test-topic",
        key=key_serializer(message_key, key_context),  # Передаем контекст
        value=value_serializer(message_value, value_context),  # Передаем контекст
        on_delivery=delivery_report,
    )

    # Ожидание завершения отправки
    producer.flush()
    logger.info("Message successfully produced")

except Exception as e:
    logger.error(f"Failed to produce message: {e}")
