import asyncio
import json
from .database import get_db_connection
import pytz
from .consumer import async_kafka_consumer
from typing import List
from .logger import logger
from datetime import datetime, timedelta
from typing import List, Dict, Any
import orjson
from decimal import Decimal


KST = pytz.timezone('Asia/Seoul')

def format_data(row):
    """데이터 형식을 지정된 형식에 맞게 변환"""
    def safe_int(value):
        return int(value) if value is not None else 0

    def safe_float(value):
        return float(value) if value is not None else 0.0

    rate_price = safe_float(row.get('rate_price', 0))
    rate_price_str = f"{int(rate_price)}" if rate_price < 0 else f"+{int(rate_price)}"

    rate = safe_float(row.get('rate', 0.0))
    rate_str = f"{rate:.2f}" if rate < 0 else f"+{rate:.2f}"

    return {
        "symbol": row.get("symbol", ""),
        "date": row["date_group"],
        "open": f"{safe_float(row.get('open', 0.0)):.2f}",
        "close": f"{safe_float(row.get('close', 0.0)):.2f}",
        "high": f"{safe_float(row.get('high', 0.0)):.2f}",
        "low": f"{safe_float(row.get('low', 0.0)):.2f}",
        "rate_price": rate_price_str,
        "rate": rate_str,
        "volume": str(safe_int(row.get('volume', 0))),
        "trading_value": f"{safe_float(row.get('trading_value', 0.0)):.2f}"
    }


async def get_filtered_data(symbol: str, interval: str, start_date=None):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    # interval 값을 숫자로 변환
    try:
        interval_minutes = int(interval.replace("m", ""))
    except ValueError:
        raise ValueError("Invalid interval format. Use values like '1m', '5m', '10m'.")

    if interval_minutes == 1:
        # 1분봉은 데이터베이스 값 그대로 조회
        query = """
            SELECT 
                symbol,
                open,
                close,
                high,
                low,
                volume,
                trading_value,
                DATE_FORMAT(date, '%Y-%m-%d %H:%i:00') AS date_group,
                ROUND(rate, 2) AS rate,
                rate_price
            FROM stock
            WHERE symbol = %s AND date >= %s AND TIME(date) != '00:00:00'
            ORDER BY date_group
        """
    else:
        # N분봉은 쿼리로 계산
        query = f"""
                WITH grouped_data AS (
                    SELECT 
                        symbol,
                        DATE_FORMAT(
                            DATE_SUB(date, INTERVAL MINUTE(date) % {interval_minutes} MINUTE), '%Y-%m-%d %H:%i:00'
                        ) AS date_group,
                        FIRST_VALUE(open) OVER (
                            PARTITION BY DATE_FORMAT(
                                DATE_SUB(date, INTERVAL MINUTE(date) % {interval_minutes} MINUTE), '%Y-%m-%d %H:%i:00'
                            ) ORDER BY date ASC
                        ) AS open,
                        LAST_VALUE(close) OVER (
                            PARTITION BY DATE_FORMAT(
                                DATE_SUB(date, INTERVAL MINUTE(date) % {interval_minutes} MINUTE), '%Y-%m-%d %H:%i:00'
                            ) ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS close,
                        MAX(high) OVER (
                            PARTITION BY DATE_FORMAT(
                                DATE_SUB(date, INTERVAL MINUTE(date) % {interval_minutes} MINUTE), '%Y-%m-%d %H:%i:00'
                            )
                        ) AS high,
                        MIN(low) OVER (
                            PARTITION BY DATE_FORMAT(
                                DATE_SUB(date, INTERVAL MINUTE(date) % {interval_minutes} MINUTE), '%Y-%m-%d %H:%i:00'
                            )
                        ) AS low,
                        SUM(volume) OVER (
                            PARTITION BY DATE_FORMAT(
                                DATE_SUB(date, INTERVAL MINUTE(date) % {interval_minutes} MINUTE), '%Y-%m-%d %H:%i:00'
                            )
                        ) AS volume,
                        SUM(trading_value) OVER (
                            PARTITION BY DATE_FORMAT(
                                DATE_SUB(date, INTERVAL MINUTE(date) % {interval_minutes} MINUTE), '%Y-%m-%d %H:%i:00'
                            )
                        ) AS trading_value
                    FROM stock
                    WHERE symbol = %s AND date >= %s AND TIME(date) != '00:00:00'
                ),
                unique_data AS (
                    SELECT DISTINCT
                        symbol,
                        open,
                        close,
                        high,
                        low,
                        volume,
                        trading_value,
                        LAG(close) OVER (PARTITION BY symbol ORDER BY date_group ASC) AS prev_close,
                        date_group
                    FROM grouped_data
                )
                SELECT 
                    symbol,
                    open,
                    close,
                    high,
                    low,
                    volume,
                    trading_value,
                    ROUND((close - prev_close) / prev_close * 100, 2) AS rate, -- rate 계산
                    close - prev_close AS rate_price, -- rate_price 계산
                    date_group
                FROM unique_data
                WHERE prev_close IS NOT NULL -- 이전 데이터가 없는 첫 번째 그룹 제외
                ORDER BY date_group;
        """

    cursor.execute(query, (symbol, start_date))
    rows = cursor.fetchall()
    formatted_rows = [format_data(row) for row in rows]
    cursor.close()
    connection.close()
    return formatted_rows


# SSE 비동기 이벤트 생성기
async def sse_event_generator(topic: str, group_id: str, symbol: str):
    consumer = await async_kafka_consumer(topic, group_id)
    try:
        async for message in consumer:
            # 메시지의 값을 JSON으로 파싱
            try:
                data = json.loads(message.value) if isinstance(message.value, str) else message.value
            except json.JSONDecodeError:
                continue

            if "__faust" in data:
                data.pop("__faust")
                logger.debug(f"Removed '__faust' from data: {data}")

            # JSON으로 파싱된 데이터에서 symbol을 확인
            if isinstance(data, dict) and data.get("symbol") == symbol:
                yield f"data: {json.dumps(data)}\n\n"  # 클라이언트에 데이터 전송

    except asyncio.CancelledError:
        # 클라이언트 연결이 끊겼을 때 발생
        print(f"Client disconnected from stream for symbol: {symbol}")


def get_symbols_for_page(page: int, page_size: int = 20) -> List[str]:
    start_index = (page - 1) * page_size
    database = get_db_connection()
    cursor = database.cursor()

    query = """
        SELECT symbol
        FROM company
        WHERE is_deleted = 0
        ORDER BY id
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, (page_size, start_index))
    # 심볼만 리스트로 반환
    symbols: List[str] = [row[0] for row in cursor.fetchall()]

    cursor.close()
    database.close()

    return symbols

async def sse_pagination_generator(topic: str, group_id: str, symbols: List[str]):
    consumer = await async_kafka_consumer(topic, group_id)
    if consumer is None:
        logger.error("Kafka consumer setup failed, exiting generator.")
        return

    symbol_data_dict = {symbol: None for symbol in symbols}
    logger.info(f"Kafka consumer started - Group ID: {group_id}")

    try:
        async for message in consumer:
            try:
                # 메시지가 이미 dict인지 확인
                data = message.value if isinstance(message.value, dict) else json.loads(message.value)
            except json.JSONDecodeError:
                logger.warning("Failed to decode message")
                continue

            if "__faust" in data:
                data.pop("__faust")

            # 심볼별 데이터 업데이트
            symbol = data.get("symbol")
            if symbol in symbol_data_dict:
                # 데이터가 변경되었을 경우에만 전송
                if symbol_data_dict[symbol] != data:
                    symbol_data_dict[symbol] = data
                    logger.debug(f"Updated data for symbol {symbol}")

                    # 즉시 전송 (변경된 데이터만)
                    bundled_data = orjson.dumps([data]).decode("utf-8")
                    logger.info(f"Sending updated data: {bundled_data}")
                    yield f"data: {bundled_data}\n\n"

    except asyncio.CancelledError:
        logger.info("Client disconnected from SSE stream.")


def get_latest_symbols_data(symbols: List[str]) -> List[Dict[str, Any]]:
    database = get_db_connection()
    cursor = database.cursor(dictionary=True)

    query = """
        SELECT s1.symbol, s1.high, s1.low, s1.volume, s1.date, s1.open, s1.close, s1.rate, s1.rate_price, s1.trading_value
        FROM stock s1
        INNER JOIN (
            SELECT symbol, MAX(date) AS max_date
            FROM stock
            WHERE is_deleted = 0 AND symbol IN (%s)
            GROUP BY symbol
        ) s2 ON s1.symbol = s2.symbol AND s1.date = s2.max_date
        WHERE s1.is_deleted = 0
        ORDER BY s1.id ASC  -- Ensure the ordering by id
        LIMIT 20            -- Limit to 20 records if needed
    """
    format_strings = ', '.join(['%s'] * len(symbols))
    query = query % format_strings

    cursor.execute(query, symbols)

    latest_data = cursor.fetchall()

    cursor.close()
    database.close()

    return latest_data