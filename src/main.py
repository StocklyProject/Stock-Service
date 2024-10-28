# import json
# from fastapi import FastAPI, Query
# from fastapi.responses import StreamingResponse
# from starlette.middleware.cors import CORSMiddleware
# from datetime import datetime, timedelta
# import asyncio
# from queue import Queue
# from .consumer import kafka_consumer, init_kafka_producer
# from threading import Thread
#
# app = FastAPI()
#
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
#
#
# # Kafka 데이터를 읽어서 큐에 전달하는 스레드 작업 함수
# def consume_to_queue(consumer, queue):
#     for message in consumer:
#         queue.put(message.value)  # 메시지 데이터를 큐에 전달
#     consumer.close()
#
# # SSE 비동기 이벤트 생성기
# async def sse_event_generator(consumer):
#     print("Starting SSE generator")
#     queue = Queue()
#
#     # Kafka 소비자를 스레드에서 실행
#     consumer_thread = Thread(target=consume_to_queue, args=(consumer, queue))
#     consumer_thread.start()
#
#     try:
#         while True:
#             if not queue.empty():
#                 stock_data = queue.get()
#                 print(f"Sending stock data to client: {stock_data}")
#                 yield f"data: {json.dumps(stock_data)}\n\n"
#             await asyncio.sleep(0.5)  # 메시지 간의 대기
#     except Exception as e:
#         print(f"Error in SSE generator: {e}")
#     finally:
#         consumer.close()
#         consumer_thread.join()
#         print("Closed Kafka consumer")
#
#
# # SSE 엔드포인트 (실시간 데이터 스트리밍)
# @app.get("/stream/{symbol}", response_class=StreamingResponse)
# async def sse_stream(symbol: str):
#     topic = f"real_time_stock_prices_{symbol}"
#     # 고유한 group_id 생성
#     group_id = f"stream_consumer_group_{symbol}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
#     consumer = kafka_consumer(topic, group_id)
#     print(f"Starting SSE stream for symbol '{symbol}' on topic '{topic}' with unique group_id '{group_id}'")
#     return StreamingResponse(sse_event_generator(consumer), media_type="text/event-stream")
#
# # 필터링된 데이터 스트리밍 엔드포인트 (SSE)
# @app.get("/streamFilter", response_class=StreamingResponse)
# async def sse_filtered_stream(symbol: str = Query(...), interval: int = Query(...)):
#     topic = f"filtered_{symbol}_{interval}m"
#     # 고유한 group_id 생성
#     group_id = f"filter_consumer_group_{symbol}_{interval}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
#     consumer = kafka_consumer(topic, group_id)
#     print(f"Starting filtered SSE stream for symbol '{symbol}' with interval '{interval}' and unique group_id '{group_id}'")
#     return StreamingResponse(sse_event_generator(consumer), media_type="text/event-stream")
#
# # Kafka Producer를 활용한 필터링된 데이터 처리
# async def batch_processor(symbol: str, interval: int):
#     topic = f"real_time_stock_prices_{symbol}"
#     consumer = kafka_consumer(topic, f"batch_processor_{symbol}_{interval}")
#     producer = init_kafka_producer()
#
#     interval_duration = timedelta(minutes=interval)
#     data_buffer = []
#     current_interval = datetime.now().replace(second=0, microsecond=0)
#     output_topic = f"filtered_{symbol}_{interval}m"
#
#     try:
#         for message in consumer:
#             stock_data = message.value
#             timestamp = datetime.strptime(stock_data["date"], "%H%M%S")
#             timestamp = datetime.combine(current_interval.date(), timestamp.time())
#
#             if timestamp < current_interval + interval_duration:
#                 data_buffer.append(stock_data)
#             else:
#                 if data_buffer:
#                     filtered_data = generate_filtered_data(data_buffer)
#                     print(f"Sending filtered data: {filtered_data}")
#                     producer.send(output_topic, value=filtered_data)
#                     producer.flush()
#                     data_buffer = [stock_data]
#                     current_interval += interval_duration
#
#             await asyncio.sleep(0.5)
#     finally:
#         consumer.close()
#         producer.close()
#         print(f"Closed consumer and producer for batch processing on topic '{output_topic}'")
#
# # 필터링된 데이터 생성 함수
# def generate_filtered_data(data_buffer):
#     return {
#         "date": data_buffer[-1]["date"],
#         "open": float(data_buffer[0]["open"]),
#         "close": float(data_buffer[-1]["close"]),
#         "day_high": max(float(data["day_high"]) for data in data_buffer),
#         "day_low": min(float(data["day_low"]) for data in data_buffer),
#         "volume": sum(int(data["volume"]) for data in data_buffer),
#         "transaction_volume": sum(int(data["transaction_volume"]) for data in data_buffer),
#     }


import json
from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse
from starlette.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 비동기 Kafka Consumer 초기화
async def async_kafka_consumer(topic: str, group_id: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers='kafka-broker.stockly.svc.cluster.local:9092',
        group_id=group_id,
        auto_offset_reset='earliest',
    )
    await consumer.start()
    return consumer

# SSE 비동기 이벤트 생성기
async def sse_event_generator(consumer):
    try:
        async for message in consumer:
            stock_data = json.loads(message.value.decode("utf-8"))
            print(f"Sending stock data to client: {stock_data}")
            yield f"data: {json.dumps(stock_data)}\n\n"
            await asyncio.sleep(0.5)  # 메시지 간의 대기
    except Exception as e:
        print(f"Error in SSE generator: {e}")
    finally:
        await consumer.stop()
        print("Closed Kafka consumer")

# SSE 엔드포인트 (실시간 데이터 스트리밍)
@app.get("/stream/{symbol}", response_class=StreamingResponse)
async def sse_stream(symbol: str):
    topic = f"real_time_stock_prices_{symbol}"
    group_id = f"stream_consumer_group_{symbol}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    consumer = await async_kafka_consumer(topic, group_id)
    print(f"Starting SSE stream for symbol '{symbol}' on topic '{topic}' with unique group_id '{group_id}'")
    return StreamingResponse(sse_event_generator(consumer), media_type="text/event-stream")

# 필터링된 데이터 스트리밍 엔드포인트 (SSE)
@app.get("/streamFilter", response_class=StreamingResponse)
async def sse_filtered_stream(symbol: str = Query(...), interval: int = Query(...)):
    topic = f"filtered_{symbol}_{interval}m"
    group_id = f"filter_consumer_group_{symbol}_{interval}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    consumer = await async_kafka_consumer(topic, group_id)
    print(f"Starting filtered SSE stream for symbol '{symbol}' with interval '{interval}' and unique group_id '{group_id}'")
    return StreamingResponse(sse_event_generator(consumer), media_type="text/event-stream")

# Kafka Producer 비동기 초기화 (결과 전송용)
async def init_kafka_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers='kafka-broker.stockly.svc.cluster.local:9092',
    )
    await producer.start()
    return producer

# 필터링된 데이터 생성 함수
def generate_filtered_data(data_buffer):
    return {
        "date": data_buffer[-1]["date"],
        "open": float(data_buffer[0]["open"]),
        "close": float(data_buffer[-1]["close"]),
        "day_high": max(float(data["day_high"]) for data in data_buffer),
        "day_low": min(float(data["day_low"]) for data in data_buffer),
        "volume": sum(int(data["volume"]) for data in data_buffer),
        "transaction_volume": sum(int(data["transaction_volume"]) for data in data_buffer),
    }