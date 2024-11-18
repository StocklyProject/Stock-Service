import json
from fastapi import FastAPI, APIRouter
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from aiokafka import AIOKafkaConsumer
from .database import get_db_connection, get_db_connection_async
from datetime import datetime
import asyncio
from . import routes as stockDetails_routes
from starlette.middleware.cors import CORSMiddleware
import yfinance as yf
import pytz
from .service import get_symbols_for_page

# 기존 최신 데이터 및 거래량 누적 변수 설정
latest_data = {}
volume_accumulator = 0
KST = pytz.timezone('Asia/Seoul')

# 20개 회사의 심볼 리스트 정의
company_symbols =['005930.KS', '003550.KS'] # ['삼성전자', 'LG']

async def download_stock_data(symbol):
    loop = asyncio.get_event_loop()
    stock = await loop.run_in_executor(None, yf.Ticker, symbol)
    data = await loop.run_in_executor(None, stock.history, "max")
    return data

async def store_historical_data():
    async with await get_db_connection_async() as connection:
        async with connection.cursor() as cursor:
            for symbol in company_symbols:
                data = await download_stock_data(symbol)
                clean_symbol = symbol.replace(".KS", "")
                for date, row in data.iterrows():
                    date_kst = date.to_pydatetime().astimezone(KST)
                    trading_value = int(row['Volume']) * int(row['Close'])
                    rate_price = int(row['Close']) - int(row['Open'])
                    rate = round((rate_price / row['Open']) * 100, 2) if row['Open'] else 0.0
                    values = (
                        clean_symbol,
                        date_kst,
                        int(row['Open']),
                        int(row['Close']),
                        int(row['High']),
                        int(row['Low']),
                        int(row['Volume']),
                        rate,
                        rate_price,
                        trading_value,
                    )
                    query = """
                        INSERT INTO stock (symbol, date, open, close, high, low, volume, rate, rate_price, trading_value)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            open = VALUES(open),
                            close = VALUES(close),
                            high = VALUES(high),
                            low = VALUES(low),
                            volume = VALUES(volume),
                            rate = VALUES(rate),
                            rate_price = VALUES(rate_price),
                            trading_value = VALUES(trading_value)
                    """
                    await cursor.execute(query, values)
                await connection.commit()


async def fetch_latest_data(page: int = 1):
    global latest_data, volume_accumulator

    # 페이지별로 심볼 리스트를 가져옴
    symbols = get_symbols_for_page(page)
    # 페이지 심볼 리스트로 `latest_data`와 `volume_accumulator` 초기화
    latest_data = {symbol: {} for symbol in symbols}
    volume_accumulator = {symbol: 0 for symbol in symbols}

    consumer = AIOKafkaConsumer(
        'real_time_stock_prices',
        auto_offset_reset='latest',
        bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
        # bootstrap_servers=['kafka:9092'],
        group_id=f'stock_data_group_page_{page}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
        enable_auto_commit=True,
    )

    await consumer.start()
    try:
        async for message in consumer:
            try:
                data = json.loads(message.value)

                # 데이터가 사전형인지 확인하고 심볼별로 최신 데이터 및 거래량 누적
                if isinstance(data, dict):
                    symbol = data.get('symbol')
                    if symbol in symbols:
                        latest_data[symbol] = data  # 각 심볼의 최신 데이터 저장
                        volume_accumulator[symbol] += int(data.get('volume', 0))  # 각 심볼별로 거래량 누적
            except Exception as e:
                pass
    finally:
        await consumer.stop()


async def store_latest_data(page: int = 1):
    global latest_data, volume_accumulator

    # 비동기로 DB 연결
    connection = await get_db_connection_async()
    async with connection.cursor() as cursor:
        # 페이지별 심볼 리스트를 가져옴
        symbols = get_symbols_for_page(page)  # 비동기로 호출
        now_kst = datetime.now(KST)  # 한국 시간으로 현재 시간 설정

        for symbol in symbols:
            data = latest_data.get(symbol, {})
            if data:  # 데이터가 있는 심볼만 저장
                # 1분 동안 누적된 거래량 가져오기
                accumulated_volume = volume_accumulator.get(symbol, 0)

                # close_price와 volume을 float 또는 int로 변환
                close_price = float(data.get('close') or 0)  # close가 없을 경우 기본값 0
                accumulated_volume = int(accumulated_volume)

                # trading_value 계산
                trading_value = close_price * accumulated_volume

                values = (
                    data.get('symbol'),
                    now_kst,  # 한국 시간으로 저장
                    float(data.get('open') or 0),
                    close_price,
                    float(data.get('high') or 0),
                    float(data.get('low') or 0),
                    accumulated_volume,  # 1분 동안 누적된 거래량 저장
                    float(data.get('rate') or 0),
                    float(data.get('rate_price') or 0),
                    trading_value  # 누적 거래량과 close를 곱한 값
                )

                # DB에 데이터 저장 쿼리
                query = """
                INSERT INTO stock (symbol, date, open, close, high, low, volume, rate, rate_price, trading_value)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
                ON DUPLICATE KEY UPDATE
                    open = new.open,
                    close = new.close,
                    high = new.high,
                    low = new.low,
                    volume = new.volume,
                    rate = new.rate,
                    rate_price = new.rate_price,
                    trading_value = new.trading_value
                """
                await cursor.execute(query, values)  # 비동기 실행

                # 각 심볼의 거래량 누적 초기화
                volume_accumulator[symbol] = 0

        await connection.commit()  # 커밋


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
app = FastAPI(lifespan=lifespan)
# app = FastAPI()

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