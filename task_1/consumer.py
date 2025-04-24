from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import json
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def error_callback(err):
    logger.error("Kafka error: %s", err)


# Конфигурация Schema Registry
sr_conf = {
    "url": "https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:443",
    "basic.auth.user.info": "producer-consumer:1qazXSW@",
    "ssl.ca.location": "/Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
}

try:
    schema_registry_client = SchemaRegistryClient(sr_conf)

    # Получаем актуальные версии схем
    key_schema = schema_registry_client.get_latest_version(
        "test-topic-key"
    ).schema.schema_str
    value_schema = schema_registry_client.get_latest_version(
        "test-topic-value"
    ).schema.schema_str

    # Создаем десериализаторы
    key_deserializer = JSONDeserializer(key_schema)
    value_deserializer = JSONDeserializer(value_schema)

except Exception as e:
    logger.error("Failed to initialize Schema Registry: %s", e)
    raise

# Конфигурация консьюмера
consumer_conf = {
    "bootstrap.servers": "rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:9091,rc1b-mq4ajmtffresd11r.mdb.yandexcloud.net:9091,rc1d-2msrqk83jn1vnarh.mdb.yandexcloud.net:9091",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "producer-consumer",
    "sasl.password": "1qazXSW@",
    "ssl.ca.location": "/Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
    "group.id": "test-consumer-group",
    "auto.offset.reset": "earliest",
    "error_cb": error_callback,
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["test-topic"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("Consumer error: %s", msg.error())
            continue

        # Создаем контекст для десериализации
        key_context = SerializationContext(msg.topic(), MessageField.KEY)
        value_context = SerializationContext(msg.topic(), MessageField.VALUE)

        try:
            # Десериализуем ключ и значение
            key = key_deserializer(msg.key(), key_context) if msg.key() else None
            value = (
                value_deserializer(msg.value(), value_context) if msg.value() else None
            )

            # Выводим в читаемом формате
            print("\nReceived message:")
            print(f"Key: {json.dumps(key, indent=2, ensure_ascii=False)}")
            print(f"Value: {json.dumps(value, indent=2, ensure_ascii=False)}")
            print(f"Partition: {msg.partition()}, Offset: {msg.offset()}")

        except Exception as e:
            logger.error("Failed to deserialize message: %s", e)
            # Если не удалось десериализовать, выводим как есть
            print("\nRaw message:")
            print(f"Key: {msg.key()}")
            print(f"Value: {msg.value().decode('utf-8') if msg.value() else None}")

except KeyboardInterrupt:
    logger.info("Consumer interrupted")
finally:
    consumer.close()
