# consumer.py
import json
from kafka import KafkaConsumer, KafkaProducer

# Kafka Consumer 초기화
def kafka_consumer(topic: str, group_id: str):
    print(f"Initializing Kafka Consumer for topic '{topic}' with group_id '{group_id}'")
    return KafkaConsumer(
        topic,
        bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
        auto_offset_reset='earliest',
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True,
    )

# Kafka Producer 초기화 (결과 전송용)
def init_kafka_producer():
    print("Initializing Kafka Producer")
    return KafkaProducer(
        bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
        api_version=(2, 8, 0),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# # 메시지 처리 함수
# def process_message(message, producer):
#     stock_data = message.value
#     job_id = stock_data.get("job_id")
#     print(f"Processing job_id {job_id} with data: {stock_data}")
#
#     # 처리 후 Kafka로 결과 전송
#     result_data = {"job_id": job_id, "status": "processed", "result": "success"}
#     producer.send('processed_stock_data', result_data)
#     producer.flush()
#     print(f"Result sent to Kafka for job_id {job_id}")
#
# # Consumer 실행 함수
# def start_consumer(topic: str, group_id: str):
#     consumer = kafka_consumer(topic, group_id)
#     producer = init_kafka_producer()
#     print(f"Consumer started for topic '{topic}' with group_id '{group_id}'")
#
#     for message in consumer:
#         process_message(message, producer)