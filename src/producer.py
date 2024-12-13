from aiokafka import AIOKafkaProducer
from src.logger import logger
import orjson


async def init_kafka_producer():
    try:
        # bootstrap_servers = ['kafka:9092']
        bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092']

        if isinstance(bootstrap_servers, str):
            bootstrap_servers = [bootstrap_servers]

        # Kafka Producer 초기화
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: orjson.dumps(v) if isinstance(v, dict) else v  # JSON 직렬화
            # key_serializer=lambda k: k.encode('utf-8')
        )
        await producer.start()
        logger.info("Kafka producer initialized successfully.")
        return producer
    except Exception as e:
        logger.error(f"Error initializing Kafka producer: {e}")