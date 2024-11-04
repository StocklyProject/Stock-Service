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
import yfinance as yf
import pytz
from .logger import logger

# 기존 최신 데이터 및 거래량 누적 변수 설정
latest_data = {}
volume_accumulator = 0
KST = pytz.timezone('Asia/Seoul')

# 20개 회사의 심볼 리스트 정의
company_symbols = [
    '005930.KS', '005935.KS', '000660.KS', '373220.KS', '005380.KS', '005389.KS',
    '005385.KS', '005387.KS', '207940.KS', '000270.KS', '068270.KS', '051910.KS',
    '005490.KS', '051915.KS', '035420.KS', '006400.KS', '006405.KS', '105560.KS',
    '028260.KS', '012330.KS'
]

# yfinance로 전체 과거 데이터를 수집하여 DB에 저장하는 함수
async def store_historical_data():
    connection = get_db_connection()
    cursor = connection.cursor()

    for symbol in company_symbols:
        stock = yf.Ticker(symbol)
        data = stock.history(period="max")  # 존재하는 전체 데이터를 수집

        # `.KS`를 제거한 심볼 생성
        clean_symbol = symbol.replace(".KS", "")

        for date, row in data.iterrows():
            date_kst = date.to_pydatetime().astimezone(KST)
            values = (
                clean_symbol,  # `.KS`를 제거한 심볼을 사용
                date_kst,
                int(row['Open']),
                int(row['Close']),
                int(row['High']),
                int(row['Low']),
                int(row['Volume']),
                row['Close'] - row['Open'],  # rate_price
                round((row['Close'] - row['Open']) / row['Open'] * 100, 2) if row['Open'] else 0.0  # rate
            )

            # 데이터베이스에 삽입
            query = """
            INSERT INTO stock (symbol, date, open, close, high, low, volume, rate_price, rate)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, values)

    connection.commit()
    cursor.close()
    connection.close()


async def fetch_latest_data():
    global latest_data, volume_accumulator
    consumer = AIOKafkaConsumer(
        'real_time_stock_prices',
        bootstrap_servers=['192.168.0.54:9094'],
        group_id='stock_data_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
        enable_auto_commit=True,
    )

    await consumer.start()
    try:
        async for message in consumer:
            try:
                data = json.loads(message.value)

                # 데이터가 사전형인지 확인
                if isinstance(data, dict):
                    latest_data = data
                    volume_accumulator += int(data.get('volume', 0))  # 거래량 누적
                    logger.debug(f"Processed data: {latest_data}")
                else:
                    logger.error("Received non-dict data from Kafka after deserialization: %s", data)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e} - Raw message: {message.value}")
            except Exception as e:
                logger.error(f"Unexpected error processing message: {e} - Raw message: {message.value}")
    finally:
        await consumer.stop()


# 매 1분마다 최신 데이터를 DB에 저장하고 거래량을 초기화하는 함수
async def store_latest_data():
    global latest_data, volume_accumulator
    if isinstance(latest_data, dict):  # latest_data가 사전형인지 확인
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
            latest_data.get('symbol'),
            now_kst,  # 한국 시간으로 저장
            latest_data.get('open'),
            latest_data.get('close'),
            latest_data.get('high'),
            latest_data.get('low'),
            volume_accumulator,  # 누적된 거래량 저장
            latest_data.get('rate'),
            latest_data.get('rate_price')
        )
        cursor.execute(query, values)
        connection.commit()
        cursor.close()
        connection.close()

        # 거래량 누적 변수 초기화
        volume_accumulator = 0
    else:
        logger.error("latest_data is not a dictionary, skipping store_latest_data execution")

# lifespan 핸들러 설정
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 앱 시작 시 과거 데이터를 한 번만 저장
    await store_historical_data()

    # 스케줄러 설정 및 시작
    scheduler = AsyncIOScheduler()
    scheduler.add_job(store_latest_data, IntervalTrigger(minutes=1))
    scheduler.start()

    # Kafka 데이터 수신 비동기 작업 시작
    kafka_task = asyncio.create_task(fetch_latest_data())

    yield  # 앱이 실행되는 동안 스케줄러와 Kafka 작업이 유지됨

    scheduler.shutdown()
    kafka_task.cancel()


# FastAPI 앱 설정
# app = FastAPI(lifespan=lifespan)
app = FastAPI()

# 기존 코드 유지
router = APIRouter(prefix="/api/v1")
app.include_router(stockDetails_routes.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def hello():
    return {"message": "stock service consumer 메인페이지입니다"}