import json
from aiokafka import AIOKafkaConsumer

# 비동기 Kafka Consumer 초기화
async def async_kafka_consumer(topic: str, group_id: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=['192.168.10.24:9094'],
        group_id=group_id,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True,
        max_poll_interval_ms=300000,  # 기본값은 300000ms(5분)이며, 필요에 따라 조정
        session_timeout_ms=45000,
    )
    await consumer.start()
    return consumer
