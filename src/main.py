import json
from fastapi import FastAPI, APIRouter
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from aiokafka import AIOKafkaConsumer
from .database import get_db_connection_async
from datetime import datetime,timedelta
import asyncio
from . import routes as stockDetails_routes
from starlette.middleware.cors import CORSMiddleware
import yfinance as yf
import pytz
from .service import get_symbols_for_page
from .logger import logger
from collections import defaultdict
from .consumer import async_kafka_consumer
# 기존 최신 데이터 및 거래량 누적 변수 설정
KST = pytz.timezone('Asia/Seoul')
company_symbols = ['005930.KS', '003550.KS', '000660.KS', '207940.KS', '000270.KS']  # ['삼성전자', 'LG']

# Kafka 데이터 큐 생성
data_queue = asyncio.Queue()

# 과거 데이터 다운로드 (단일 심볼)
async def download_stock_data(symbol):
    """
    단일 심볼에 대한 yfinance 데이터를 다운로드합니다.
    """
    loop = asyncio.get_event_loop()
    stock = await loop.run_in_executor(None, yf.Ticker, symbol)
    data = await loop.run_in_executor(None, stock.history, "max")
    return symbol, data


# 병렬로 과거 데이터를 다운로드하고 저장
async def store_historical_data():
    """
    여러 회사의 데이터를 병렬로 다운로드하고 데이터베이스에 저장합니다.
    """
    async with await get_db_connection_async() as connection:
        async with connection.cursor() as cursor:
            # 병렬로 다운로드 작업 생성
            tasks = [download_stock_data(symbol) for symbol in company_symbols]
            results = await asyncio.gather(*tasks)  # 병렬 처리

            # 다운로드된 데이터를 데이터베이스에 저장
            for symbol, data in results:
                clean_symbol = symbol.replace(".KS", "")
                for date, row in data.iterrows():
                    # 데이터 가공
                    date_kst = date.to_pydatetime().astimezone(KST)
                    trading_value = int(row['Volume']) * int(row['Close'])
                    rate_price = int(row['Close']) - int(row['Open'])
                    rate = round((rate_price / row['Open']) * 100, 2) if row['Open'] else 0.0

                    # 삽입할 데이터 구성
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
                        1  # is_daily = True
                    )

                    # 데이터 삽입 쿼리
                    query = """
                        INSERT INTO stock (symbol, date, open, close, high, low, volume, rate, rate_price, trading_value, is_daily)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            open = VALUES(open),
                            close = VALUES(close),
                            high = VALUES(high),
                            low = VALUES(low),
                            volume = VALUES(volume),
                            rate = VALUES(rate),
                            rate_price = VALUES(rate_price),
                            trading_value = VALUES(trading_value),
                            is_daily = VALUES(is_daily)
                    """
                    # 쿼리 실행
                    await cursor.execute(query, values)

                logger.info(f"Stored historical data for symbol {symbol}")

            # 트랜잭션 커밋
            await connection.commit()
            logger.info("All historical data stored successfully.")


async def fetch_latest_data(page: int = 1):
    global data_queue
    consumer = await async_kafka_consumer('real_time_stock_prices', f'stock_data_group_page_{page}')
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
                    0
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
                    trading_value = VALUES(trading_value),
                    is_daily = VALUES(is_daily)
                """
                await cursor.execute(query, values)
                logger.info(f"Averaged data for symbol {symbol} stored successfully.")

            # 누적 데이터 초기화
            symbol_accumulator.clear()

        except Exception as e:
            logger.error(f"Error storing aggregated data: {e}")

        await connection.commit()

async def aggregate_daily_data():
    try:
        connection = await get_db_connection_async()
    except Exception as e:
        logger.error(f"Failed to establish database connection: {e}")
        return

    async with connection.cursor() as cursor:
        try:
            # 어제 날짜 계산
            today_kst = datetime.now(KST).date()
            yesterday_kst = today_kst - timedelta(days=1)

            # 어제 분단위 데이터를 조회 및 집계
            query = """
            SELECT 
                s.symbol,
                MIN(s.open) AS open,
                MAX(s.high) AS high,
                MIN(s.low) AS low,
                (
                    SELECT close
                    FROM stock
                    WHERE symbol = s.symbol AND DATE(date) = %s AND is_daily = 0
                    ORDER BY date DESC
                    LIMIT 1
                ) AS close,
                SUM(s.volume) AS volume,
                SUM(s.trading_value) AS trading_value
            FROM stock s
            WHERE DATE(s.date) = %s AND is_daily = 0
            GROUP BY s.symbol;
            """
            await cursor.execute(query, (yesterday_kst, yesterday_kst))
            daily_data = await cursor.fetchall()

            # 전날 종가 데이터를 가져오기
            prev_close_query = """
            SELECT symbol, close
            FROM stock
            WHERE DATE(date) = %s AND is_daily = 1
            """
            await cursor.execute(prev_close_query, (yesterday_kst - timedelta(days=1),))
            prev_close_data = {row[0]: row[1] for row in await cursor.fetchall()}

            # 일별 데이터 저장
            for row in daily_data:
                symbol, open_price, high, low, close, volume, trading_value = row

                # 전날 종가 가져오기
                prev_close = prev_close_data.get(symbol, None)

                # rate와 rate_price 계산
                if prev_close is not None:
                    rate_price = close - prev_close
                    rate = (rate_price / prev_close) * 100 if prev_close else 0.0
                else:
                    rate_price = 0
                    rate = 0.0

                insert_query = """
                INSERT INTO stock (symbol, date, open, close, high, low, volume, trading_value, is_daily, rate, rate_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    close = VALUES(close),
                    high = VALUES(high),
                    low = VALUES(low),
                    volume = VALUES(volume),
                    trading_value = VALUES(trading_value),
                    is_daily = VALUES(is_daily),
                    rate = VALUES(rate),
                    rate_price = VALUES(rate_price)
                """
                await cursor.execute(insert_query, (
                    symbol, yesterday_kst, open_price, close, high, low, volume, trading_value, rate, rate_price
                ))

            # 어제 날짜의 분단위 데이터 삭제
            delete_query = "DELETE FROM stock WHERE DATE(date) = %s AND is_daily = 0"
            await cursor.execute(delete_query, (yesterday_kst,))

            await connection.commit()
            logger.info(f"Aggregated and cleaned up data for {yesterday_kst}.")

        except Exception as e:
            logger.error(f"Error during daily aggregation: {e}")

        finally:
            if connection:
                await connection.ensure_closed()



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
    # scheduler.add_job(aggregate_daily_data, CronTrigger(hour=0, minute=0, timezone=KST))  # 매일 자정 실행
    # scheduler.add_job(aggregate_daily_data, IntervalTrigger(seconds=60))
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
