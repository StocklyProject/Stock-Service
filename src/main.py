import json
from fastapi import FastAPI, APIRouter
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from aiokafka import AIOKafkaConsumer
from .database import get_db_connection
from datetime import datetime
import asyncio
from . import routes as stockDetails_routes
from starlette.middleware.cors import CORSMiddleware
import pytz

# 최신 데이터를 저장할 변수, 빈 딕셔너리로 초기화
latest_data = {}
volume_accumulator = 0  # 거래량 합계를 저장할 변수
KST = pytz.timezone('Asia/Seoul')  # 한국 시간대 설정

# Kafka에서 최신 데이터를 계속 가져오면서 1분간의 거래량을 누적하는 비동기 함수
async def fetch_latest_data():
    global latest_data, volume_accumulator
    consumer = AIOKafkaConsumer(
        'real_time_stock_prices',
        bootstrap_servers=['192.168.10.24:9094'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='stock_data_group'
    )

    await consumer.start()  # 비동기 KafkaConsumer 시작
    try:
        async for message in consumer:
            data = message.value
            latest_data = data  # 최신 데이터로 업데이트
            volume_accumulator += int(data['volume'])  # 거래량 누적
    finally:
        await consumer.stop()  # 종료 시 KafkaConsumer 종료

# 매 1분마다 최신 데이터를 DB에 저장하고, 거래량을 초기화하는 함수
async def store_latest_data():
    global latest_data, volume_accumulator
    if latest_data:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 한국 시간으로 현재 시간 설정
        now_kst = datetime.now(KST)

        # DB에 데이터 저장 쿼리
        query = """
        INSERT INTO stock (symbol, date, open, close, high, low, volume, rate, rate_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            latest_data['symbol'],
            now_kst,  # 한국 시간으로 저장
            latest_data['open'],
            latest_data['close'],
            latest_data['high'],
            latest_data['low'],
            volume_accumulator,  # 누적된 거래량 저장
            latest_data['rate'],
            latest_data['rate_price']
        )
        cursor.execute(query, values)
        connection.commit()
        cursor.close()
        connection.close()

        # 거래량 누적 변수 초기화
        volume_accumulator = 0

# lifespan 핸들러 설정
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 스케줄러 설정 및 시작
    scheduler = AsyncIOScheduler()
    scheduler.add_job(store_latest_data, IntervalTrigger(minutes=1))  # 매 1분마다 최신 데이터 저장
    scheduler.start()

    # Kafka 데이터를 계속해서 수신하는 작업 실행
    kafka_task = asyncio.create_task(fetch_latest_data())

    yield  # FastAPI 애플리케이션이 실행되는 동안 스케줄러와 Kafka 수신 작업이 실행됨

    # 애플리케이션 종료 시 스케줄러와 Kafka 수신 작업 종료
    scheduler.shutdown()
    kafka_task.cancel()

# lifespan을 FastAPI 앱에 적용
app = FastAPI(lifespan=lifespan)

# 기본 API 라우터 설정
router = APIRouter(prefix="/api/v1")
app.include_router(stockDetails_routes.router)

# CORS 미들웨어 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 기본 엔드포인트 추가
@app.get("/")
def hello():
    return {"message": "stock service consumer 메인페이지입니다"}