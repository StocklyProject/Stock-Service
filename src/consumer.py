import json
from aiokafka import AIOKafkaConsumer
from .logger import logger

async def async_kafka_consumer(topic: str, group_id: str):
    logger.info(f"Initializing Kafka consumer for topic '{topic}' with group ID '{group_id}'")
    try:
        consumer = AIOKafkaConsumer(
            topic,
            # bootstrap_servers=['192.168.10.24:9094'],
            bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True,
            max_poll_interval_ms=300000,
            session_timeout_ms=45000,
        )
        await consumer.start()
        logger.info(f"Kafka consumer started for topic '{topic}' with group ID '{group_id}'")
        return consumer
    except Exception as e:
        logger.error(f"Error initializing Kafka consumer for topic '{topic}': {e}")
        raise
