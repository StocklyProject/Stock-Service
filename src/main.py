# import json
# from fastapi import FastAPI, APIRouter
# from contextlib import asynccontextmanager
# from apscheduler.schedulers.asyncio import AsyncIOScheduler
# from apscheduler.triggers.interval import IntervalTrigger
# from aiokafka import AIOKafkaConsumer
# from .database import get_db_connection, get_db_connection_async
# from datetime import datetime
# import asyncio
# from . import routes as stockDetails_routes
# from starlette.middleware.cors import CORSMiddleware
# import yfinance as yf
# import pytz
# from .service import get_symbols_for_page
# from src.logger import logger

# # 기존 최신 데이터 및 거래량 누적 변수 설정
# latest_data = {}
# volume_accumulator = 0
# KST = pytz.timezone('Asia/Seoul')

# # 20개 회사의 심볼 리스트 정의
# company_symbols =['005930.KS', '003550.KS'] # ['삼성전자', 'LG']

# async def download_stock_data(symbol):
#     loop = asyncio.get_event_loop()
#     stock = await loop.run_in_executor(None, yf.Ticker, symbol)
#     data = await loop.run_in_executor(None, stock.history, "max")
#     return data

# async def store_historical_data():
#     async with await get_db_connection_async() as connection:
#         async with connection.cursor() as cursor:
#             for symbol in company_symbols:
#                 data = await download_stock_data(symbol)
#                 clean_symbol = symbol.replace(".KS", "")
#                 for date, row in data.iterrows():
#                     date_kst = date.to_pydatetime().astimezone(KST)
#                     trading_value = int(row['Volume']) * int(row['Close'])
#                     rate_price = int(row['Close']) - int(row['Open'])
#                     rate = round((rate_price / row['Open']) * 100, 2) if row['Open'] else 0.0
#                     values = (
#                         clean_symbol,
#                         date_kst,
#                         int(row['Open']),
#                         int(row['Close']),
#                         int(row['High']),
#                         int(row['Low']),
#                         int(row['Volume']),
#                         rate,
#                         rate_price,
#                         trading_value,
#                     )
#                     query = """
#                         INSERT INTO stock (symbol, date, open, close, high, low, volume, rate, rate_price, trading_value)
#                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#                         ON DUPLICATE KEY UPDATE
#                             open = VALUES(open),
#                             close = VALUES(close),
#                             high = VALUES(high),
#                             low = VALUES(low),
#                             volume = VALUES(volume),
#                             rate = VALUES(rate),
#                             rate_price = VALUES(rate_price),
#                             trading_value = VALUES(trading_value)
#                     """
#                     await cursor.execute(query, values)
#                 await connection.commit()


# async def fetch_latest_data(page: int = 1):
#     global latest_data, volume_accumulator

#     # 페이지별로 심볼 리스트를 가져옴
#     symbols = get_symbols_for_page(page)
#     # 페이지 심볼 리스트로 `latest_data`와 `volume_accumulator` 초기화
#     latest_data = {symbol: {} for symbol in symbols}
#     volume_accumulator = {symbol: 0 for symbol in symbols}

#     consumer = AIOKafkaConsumer(
#         'real_time_stock_prices',
#         auto_offset_reset='latest',
#         # bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
#         bootstrap_servers=['kafka:9092'],
#         group_id=f'stock_data_group_page_{page}',
#         value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
#         enable_auto_commit=True,
#     )

#     await consumer.start()
#     try:
#         async for message in consumer:
#             try:
#                 data = json.loads(message.value)

#                 # 데이터가 사전형인지 확인하고 심볼별로 최신 데이터 및 거래량 누적
#                 if isinstance(data, dict):
#                     symbol = data.get('symbol')
#                     if symbol in symbols:
#                         latest_data[symbol] = data  # 각 심볼의 최신 데이터 저장
#                         volume_accumulator[symbol] += int(data.get('volume', 0))  # 각 심볼별로 거래량 누적
#             except Exception as e:
#                 pass
#     finally:
#         await consumer.stop()

# async def store_latest_data(page: int = 1):
#     global latest_data, volume_accumulator

#     # DB 연결
#     connection = await get_db_connection_async()
#     if not connection:
#         logger.error("Failed to establish database connection.")
#         return

#     async with connection.cursor() as cursor:
#         symbols = get_symbols_for_page(page)
#         now_kst = datetime.now(KST)

#         for symbol in symbols:
#             data = latest_data.get(symbol, {})
#             if data:
#                 accumulated_volume = volume_accumulator.get(symbol, 0)

#                 # 데이터 타입 변환
#                 try:
#                     close_price = float(data.get('close') or 0)
#                     accumulated_volume = int(accumulated_volume)
#                     trading_value = close_price * accumulated_volume

#                     values = (
#                         data.get('symbol'),
#                         now_kst,
#                         float(data.get('open') or 0),  # 변환된 값
#                         close_price,                  # 변환된 값
#                         float(data.get('high') or 0), # 변환된 값
#                         float(data.get('low') or 0),  # 변환된 값
#                         accumulated_volume,           # 변환된 값
#                         float(data.get('rate') or 0), # 변환된 값
#                         float(data.get('rate_price') or 0), # 변환된 값
#                         trading_value                 # 변환된 값
#                     )

#                     # 디버깅 로그
#                     logger.debug(f"Prepared values for DB: {values}")

#                     query = """
#                     INSERT INTO stock (symbol, date, open, close, high, low, volume, rate, rate_price, trading_value)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
#                     ON DUPLICATE KEY UPDATE
#                         open = new.open,
#                         close = new.close,
#                         high = new.high,
#                         low = new.low,
#                         volume = new.volume,
#                         rate = new.rate,
#                         rate_price = new.rate_price,
#                         trading_value = new.trading_value
#                     """
#                     await cursor.execute(query, values)
#                     logger.debug(f"Query executed successfully for symbol: {symbol}")
#                 except Exception as e:
#                     logger.error(f"Error processing data for symbol {symbol}: {e}")

#                 # 거래량 초기화
#                 volume_accumulator[symbol] = 0
#                 logger.debug(f"Volume accumulator reset for {symbol}.")

#         try:
#             await connection.commit()
#             logger.info("Database changes committed successfully.")
#         except Exception as e:
#             logger.error(f"Failed to commit transaction: {e}")

# # lifespan 핸들러 설정
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # 앱 시작 시 과거 데이터를 한 번만 저장
#     await store_historical_data()

#     # 스케줄러 설정 및 시작
#     scheduler = AsyncIOScheduler()
#     scheduler.add_job(store_latest_data, IntervalTrigger(minutes=1))
#     scheduler.start()

#     # Kafka 데이터 수신 비동기 작업 시작
#     kafka_task = asyncio.create_task(fetch_latest_data())

#     yield  # 앱이 실행되는 동안 스케줄러와 Kafka 작업이 유지됨

#     scheduler.shutdown()
#     kafka_task.cancel()


# # FastAPI 앱 설정
# app = FastAPI(lifespan=lifespan)
# # app = FastAPI()

# # 기존 코드 유지
# router = APIRouter(prefix="/api/v1")
# app.include_router(stockDetails_routes.router)

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# @app.get("/")
# def hello():
#     return {"message": "stock service consumer 메인페이지입니다"}


import json
from fastapi import FastAPI, APIRouter
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from aiokafka import AIOKafkaConsumer
from .database import get_db_connection_async
from datetime import datetime,timedelta
import asyncio
from . import routes as stockDetails_routes
from starlette.middleware.cors import CORSMiddleware
import yfinance as yf
import pytz
from .service import get_symbols_for_page
from src.logger import logger
from collections import defaultdict

# 기존 최신 데이터 및 거래량 누적 변수 설정
KST = pytz.timezone('Asia/Seoul')
company_symbols = ['005930.KS', '003550.KS']  # ['삼성전자', 'LG']

# Kafka 데이터 큐 생성
data_queue = asyncio.Queue()

# 과거 데이터 다운로드
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
    global data_queue

    consumer = AIOKafkaConsumer(
        'real_time_stock_prices',
        auto_offset_reset='latest',
        # bootstrap_servers=['kafka:9092'],
        bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
        group_id=f'stock_data_group_page_{page}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
        enable_auto_commit=True,
    )

    try:
        logger.info("Starting Kafka consumer...")
        await consumer.start()
        logger.info("Kafka consumer started successfully.")

        async for message in consumer:
            try:
                logger.debug(f"Message received: {message.value}")
                data = message.value
                if isinstance(data, dict):
                    await data_queue.put(data)
                    logger.info(f"Data added to queue: {data}")
                else:
                    logger.warning(f"Unexpected message format: {type(data)} - {data}")
            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}")

    except Exception as e:
        logger.error(f"Error starting Kafka consumer: {e}")

    finally:
        logger.info("Stopping Kafka consumer...")
        await consumer.stop()
        logger.info("Kafka consumer stopped.")



# 1분 동안의 데이터를 저장하는 딕셔너리
symbol_accumulator = defaultdict(lambda: {
    "first_open": None,  # 가장 처음 open 값
    "latest_close": None,  # 가장 마지막 close 값
    "high_max": float("-inf"),
    "low_min": float("inf"),
    "volume_sum": 0,
    "rate_sum": 0.0,
    "rate_price_sum": 0.0,
    "count": 0
})

async def store_latest_data():
    global data_queue, symbol_accumulator

    # DB 연결
    connection = await get_db_connection_async()
    if not connection:
        logger.error("Failed to establish database connection.")
        return

    async with connection.cursor() as cursor:
        while not data_queue.empty():  # 큐에 데이터가 있을 때만 처리
            data = await data_queue.get()
            try:
                symbol = data.get('symbol')
                acc = symbol_accumulator[symbol]

                # 가장 처음 open 값
                if acc["first_open"] is None:
                    acc["first_open"] = float(data.get('open', 0))

                # 가장 마지막 close 값
                acc["latest_close"] = float(data.get('close', 0))

                # 데이터 누적
                acc["high_max"] = max(acc["high_max"], float(data.get('high', 0)))
                acc["low_min"] = min(acc["low_min"], float(data.get('low', 0)))
                acc["volume_sum"] += int(data.get('volume', 0))
                acc["rate_sum"] += float(data.get('rate', 0))
                acc["rate_price_sum"] += float(data.get('rate_price', 0))
                acc["count"] += 1

            except Exception as e:
                logger.error(f"Error accumulating data: {e}")

        # 1분 동안 누적된 데이터를 처리
        now_kst = datetime.now(KST)
        try:
            for symbol, acc in symbol_accumulator.items():
                if acc["count"] == 0:  # 데이터가 없는 경우 건너뜀
                    continue

                # 누적된 데이터로 계산
                open_value = acc["first_open"]  # 가장 처음 open 값
                close_value = acc["latest_close"]  # 가장 마지막 close 값
                high_max = acc["high_max"]
                low_min = acc["low_min"]
                volume_sum = acc["volume_sum"]
                rate_avg = acc["rate_sum"] / acc["count"]  # 평균 rate
                rate_price_avg = acc["rate_price_sum"] / acc["count"]  # 평균 rate_price
                trading_value = close_value * volume_sum

                # 데이터베이스 저장 값
                values = (
                    symbol,
                    now_kst,
                    open_value,
                    close_value,
                    high_max,
                    low_min,
                    volume_sum,
                    rate_avg,
                    rate_price_avg,
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
                logger.info(f"Averaged data for symbol {symbol} stored successfully.")

            # 누적 데이터 초기화
            symbol_accumulator.clear()

        except Exception as e:
            logger.error(f"Error storing aggregated data: {e}")

        await connection.commit()

# lifespan 핸들러 설정
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 앱 시작 시 과거 데이터를 한 번만 저장
    await store_historical_data()

    # Kafka 데이터 수신 작업 비동기 시작
    kafka_task = asyncio.create_task(fetch_latest_data())

    # 스케줄러 설정 및 시작
    scheduler = AsyncIOScheduler()
    scheduler.add_job(store_latest_data, IntervalTrigger(seconds=60))  # 60초마다 실행
    scheduler.start()

    yield

    # 종료 시 정리
    scheduler.shutdown()
    kafka_task.cancel()
    await kafka_task


# FastAPI 앱 설정
app = FastAPI(lifespan=lifespan)

# 기존 라우터 및 미들웨어 유지
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
    return {"message": "Stock service consumer 메인페이지입니다"}
