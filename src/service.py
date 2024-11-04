import asyncio
import json
from .database import get_db_connection
from datetime import datetime
import pytz
from .logger import logger
from .consumer import async_kafka_consumer

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
        "volume": str(row["volume"])
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
                logger.error(f"Failed to decode message: {message.value}")
                continue

            # JSON으로 파싱된 데이터에서 symbol을 확인
            if isinstance(data, dict) and data.get("symbol") == symbol:
                yield f"data: {json.dumps(data)}\n\n"  # 클라이언트에 데이터 전송

            await asyncio.sleep(0.5)  # 메시지 간 대기 시간 설정
    except asyncio.CancelledError:
        logger.info("Client disconnected, cancelling SSE generator.")
    finally:
        await consumer.stop()
        logger.info(f"Kafka consumer stopped for topic '{topic}' with group ID '{group_id}'")



# async def sse_batch_generator():
#     buffer: List[dict] = []
#     async for stock_data in consume_stock_data():
#         buffer.append(stock_data)
#         if len(buffer) >= 20:
#             yield f"data: {json.dumps(buffer)}\n\n"
#             buffer.clear()
#         await asyncio.sleep(0.3)  # 조절 가능한 대기 시간
