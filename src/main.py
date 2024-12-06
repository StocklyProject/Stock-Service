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
from .stock import routes as stockDetails_routes
from starlette.middleware.cors import CORSMiddleware
import yfinance as yf
import pytz
from .stock.service import get_symbols_for_page
from .logger import logger
from collections import defaultdict
from .consumer import async_kafka_consumer
from src.faust_app.app import app_faust
from .alert import routes as alert_routes
from .alert.service import consume_real_time_prices

# 기존 최신 데이터 및 거래량 누적 변수 설정
KST = pytz.timezone('Asia/Seoul')
company_symbols = ['005930.KS', '003550.KS', '000660.KS', '207940.KS', '000270.KS', '035720.KS', '035420.KS', '225190.KS', '035900.KS', '010130.KS']

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


async def aggregate_daily_data():
    """Aggregate daily data and store in the database."""
    try:
        connection = await get_db_connection_async()
        async with connection.cursor() as cursor:
            today_kst = datetime.now(KST).date()
            yesterday_kst = today_kst - timedelta(days=1)

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

            prev_close_query = """
            SELECT symbol, close
            FROM stock
            WHERE DATE(date) = %s AND is_daily = 1
            """
            await cursor.execute(prev_close_query, (yesterday_kst - timedelta(days=1),))
            prev_close_data = {row[0]: row[1] for row in await cursor.fetchall()}

            for row in daily_data:
                symbol, open_price, high, low, close, volume, trading_value = row
                prev_close = prev_close_data.get(symbol)

                rate_price = close - prev_close if prev_close else 0
                rate = (rate_price / prev_close) * 100 if prev_close else 0.0

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
                    symbol, yesterday_kst, open_price, close, high, low, volume,
                    trading_value, rate, rate_price
                ))

            delete_query = "DELETE FROM stock WHERE DATE(date) = %s AND is_daily = 0"
            await cursor.execute(delete_query, (yesterday_kst,))

            await connection.commit()
            logger.info(f"Aggregated daily data for {yesterday_kst}.")
    except Exception as e:
        logger.error(f"Error during daily aggregation: {e}")

# 심볼별 누적 데이터를 저장하는 딕셔너리
symbol_accumulator = defaultdict(lambda: {
    "first_open": None,
    "latest_close": None,
    "high_max": float('-inf'),  # 1분간 close의 최대값
    "low_min": float('inf'),    # 1분간 close의 최소값
    "volume_sum": 0,
    "count": 0
})

async def consume_and_aggregate():
    """Consume real-time stock data from Kafka and aggregate it."""
    consumer = await async_kafka_consumer('real_time_stock_prices', 'stock_data_group_page')
    try:
        last_commit_time = datetime.now(KST).replace(second=0, microsecond=0)

        async for message in consumer:
            try:
                data = message.value  # Kafka 메시지는 이미 dict로 변환된 상태
                symbol = data.get("symbol")
                if not symbol:
                    logger.warning(f"Missing symbol in message: {data}")
                    continue

                # 심볼 데이터 가져오기
                acc = symbol_accumulator[symbol]

                # 누적 데이터 갱신
                close_value = float(data.get("close", 0))
                if acc["first_open"] is None or acc["first_open"] == 0:
                    connection = await get_db_connection_async()
                    async with connection.cursor() as cursor:
                        # 당일 00:00:00 시점의 close 값을 가져오는 쿼리
                        today_start_query = """
                        SELECT close
                        FROM stock
                        WHERE symbol = %s AND is_daily = 0 AND date = %s
                        LIMIT 1
                        """
                        today_date = datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0)
                        await cursor.execute(today_start_query, (symbol, today_date))
                        today_data = await cursor.fetchone()

                        if today_data:  # 당일 데이터가 존재할 경우
                            acc["first_open"] = float(today_data[0])
                        else:  # 당일 데이터가 없을 경우 기존 로직으로 처리
                            prev_close_query = """
                            SELECT close
                            FROM stock
                            WHERE symbol = %s AND is_daily = 0
                            ORDER BY date DESC
                            LIMIT 1
                            """
                            await cursor.execute(prev_close_query, (symbol,))
                            previous_data = await cursor.fetchone()
                            acc["first_open"] = float(previous_data[0]) if previous_data else close_value

                acc["latest_close"] = close_value
                acc["high_max"] = max(acc["high_max"], close_value)  # close 값으로 high 계산
                acc["low_min"] = min(acc["low_min"], close_value)    # close 값으로 low 계산
                acc["volume_sum"] += int(data.get("volume", 0))
                acc["count"] += 1

                # 1분 간격 데이터 저장
                current_time = datetime.now(KST).replace(second=0, microsecond=0)
                if (current_time - last_commit_time).seconds >= 60:
                    logger.info(f"Saving aggregated data at {current_time}")
                    await save_aggregated_data(current_time)
                    last_commit_time = current_time
                    symbol_accumulator.clear()

            except Exception as e:
                logger.error(f"Error processing message: {e}")
    finally:
        await consumer.stop()


async def save_aggregated_data(commit_time):
    """Save aggregated data to the database."""
    global symbol_accumulator
    try:
        connection = await get_db_connection_async()
        async with connection.cursor() as cursor:
            for symbol, acc in symbol_accumulator.items():
                
                # 집계된 데이터
                open_value = acc["first_open"]  # 최초 데이터의 open 값을 그대로 사용
                close_value = acc["latest_close"]
                high_max = acc["high_max"]
                low_min = acc["low_min"]
                volume_sum = acc["volume_sum"]
                trading_value = close_value * volume_sum

                # 최초 데이터 처리
                if acc["count"] == 1:  # 최초 데이터인 경우
                    rate_price = close_value - open_value
                    rate = round((rate_price / open_value) * 100, 2) if open_value else 0.0
                else:
                    # 이전 close 값을 데이터베이스에서 가져옴
                    prev_close_query = """
                    SELECT close
                    FROM stock
                    WHERE symbol = %s AND is_daily = 0
                    ORDER BY date DESC
                    LIMIT 1
                    """
                    await cursor.execute(prev_close_query, (symbol,))
                    previous_data = await cursor.fetchone()
                    prev_close = previous_data[0] if previous_data else open_value

                    rate_price = close_value - prev_close
                    rate = round((rate_price / prev_close) * 100, 2) if prev_close else 0.0

                # 데이터 저장
                query = """
                INSERT INTO stock (symbol, date, open, close, high, low, volume, rate, rate_price, trading_value, is_daily)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
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
                values = (
                    symbol, commit_time, prev_close, close_value, high_max,
                    low_min, volume_sum, rate, rate_price, trading_value
                )
                logger.debug(f"Executing query for {symbol} with values: {values}")
                await cursor.execute(query, values)

            await connection.commit()
            logger.info(f"Aggregated data saved successfully at {commit_time}.")
            symbol_accumulator.clear()
    except Exception as e:
        logger.error(f"Error saving aggregated data: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await store_historical_data() 
    consume_aggregate_task = asyncio.create_task(consume_and_aggregate())
    alert_task = asyncio.create_task(consume_real_time_prices())

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        aggregate_daily_data,
        CronTrigger(hour=1, minute=0, timezone=KST)
    )
    scheduler.start()

    try:
        yield
    finally:
        # 스케줄러 및 백그라운드 작업 정리
        scheduler.shutdown()
        consume_aggregate_task.cancel()
        alert_task.cancel()
        await asyncio.gather(consume_aggregate_task, alert_task, return_exceptions=True)

# FastAPI 앱 설정
app = FastAPI(lifespan=lifespan)

# 기존 라우터 및 미들웨어 유지
router = APIRouter(prefix="/api/v1")
app.include_router(stockDetails_routes.router)
app.include_router(alert_routes.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://stockly-frontend.vercel.app", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def hello():
    return {"message": "Stock service consumer 메인페이지입니다"}
