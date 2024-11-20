import asyncio
import json
from .database import get_db_connection
import pytz
from .consumer import async_kafka_consumer
from typing import List
from .logger import logger
from datetime import datetime, timedelta
from typing import List, Dict, Any


KST = pytz.timezone('Asia/Seoul')

# 데이터 형식 변환 함수
def format_data(row):
    """데이터 형식을 지정된 형식에 맞게 변환"""
    return {
        "symbol": row["symbol"],
        "date": row["date_group"].strftime("%H:%M:%S") if isinstance(row["date_group"], datetime) else row["date_group"],
        "open": f"{float(row['open'])}",
        "close": f"{float(row['close'])}",
        "high": str(row["high"]),
        "low": str(row["low"]),
        "rate_price": f"{int(row.get('rate_price', 0))}" if row.get('rate_price', 0) < 0 else f"+{int(row.get('rate_price', 0))}",
        "rate": f"{row.get('rate', 0.0)}" if row.get('rate', 0.0) < 0 else f"+{row.get('rate', 0.0)}",
        "volume": str(row["volume"]),
        "trading_value": str(row["trading_value"])
    }

async def get_filtered_data(symbol: str, interval: str, start_time=None):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    # Interval에 따른 group_by 설정
    if interval == "1m":
        group_by = "FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(created_at) / 60) * 60)"
    elif interval == "5m":
        group_by = "FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(created_at) / 300) * 300)"
    else:
        raise ValueError("Invalid interval")

    # SQL 쿼리 실행
    query = f"""
        SELECT 
            symbol,
            AVG(open) AS open,
            AVG(close) AS close,
            MAX(high) AS high,
            MIN(low) AS low,
            SUM(volume) AS volume,
            AVG(rate) AS rate,
            AVG(rate_price) AS rate_price,
            {group_by} AS date_group,
            SUM(trading_value) AS trading_value,
            MAX(created_at) AS last_created
        FROM stock
        WHERE symbol = %s AND created_at >= %s
        GROUP BY date_group
        ORDER BY date_group
    """

    cursor.execute(query, (symbol, start_time))
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

            # JSON으로 파싱된 데이터에서 symbol을 확인
            if isinstance(data, dict) and data.get("symbol") == symbol:
                yield f"data: {json.dumps(data)}\n\n"  # 클라이언트에 데이터 전송
    except asyncio.CancelledError:
        # 클라이언트 연결이 끊겼을 때 발생
        print(f"Client disconnected from stream for symbol: {symbol}")
    finally:
        await consumer.stop()  # 스트리밍 종료 시 Kafka 소비자 종료
        print(f"Stream for {symbol} stopped.")


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
        return  # 연결 실패 시 스트리밍을 종료하도록 처리

    symbol_data_dict = {symbol: None for symbol in symbols}
    last_send_time = datetime.now()
    logger.info(f"Kafka consumer started - Group ID: {group_id}")

    try:
        async for message in consumer:
            try:
                # 메시지가 이미 dict인지 확인
                data = message.value if isinstance(message.value, dict) else json.loads(message.value)
            except json.JSONDecodeError:
                logger.warning("Failed to decode message")
                continue

            # 심볼별 데이터 업데이트
            symbol = data.get("symbol")
            if symbol in symbol_data_dict:
                symbol_data_dict[symbol] = data
                logger.debug(f"Updated data for symbol {symbol}")

            # 1초마다 데이터 번들링 및 전송
            current_time = datetime.now()
            if (current_time - last_send_time) >= timedelta(seconds=1):
                bundled_data = json.dumps([data for data in symbol_data_dict.values() if data is not None])
                logger.info(f"Sending bundled data: {bundled_data}")
                yield f"data: {bundled_data}\n\n"

                last_send_time = current_time
                await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        logger.info("Client disconnected from SSE stream.")
    finally:
        await consumer.stop()
        logger.info(f"Kafka consumer stopped - Group ID: {group_id}")


def get_latest_symbols_data(symbols: List[str]) -> List[Dict[str, Any]]:
    database = get_db_connection()
    cursor = database.cursor(dictionary=True)

    # Use a query to get the latest record for each symbol
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
    # Format query symbols for the IN clause
    format_strings = ', '.join(['%s'] * len(symbols))
    query = query % format_strings

    # Execute the query with symbols as parameters
    cursor.execute(query, symbols)

    # Fetch results and format them as a list of dictionaries
    latest_data = cursor.fetchall()

    cursor.close()
    database.close()

    return latest_data