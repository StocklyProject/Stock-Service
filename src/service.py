import asyncio
import json
from .database import get_db_connection
from datetime import datetime, timedelta

# 데이터 형식 변환 함수
def format_data(row):
    """데이터 형식을 지정된 형식에 맞게 변환"""
    return {
        "symbol": row["symbol"],
        "date": row["date"].strftime("%H:%M:%S") if isinstance(row["date"], datetime) else row["date"],
        "open": f"{float(row['open'])}",
        "close": f"{float(row['close'])}",
        "high": str(row["high"]),
        "low": str(row["low"]),
        "rate_price": f"{int(row.get('rate_price', 0))}" if row.get('rate_price', 0) < 0 else f"+{int(row.get('rate_price', 0))}",
        "rate": f"{row.get('rate', 0.0)}" if row.get('rate', 0.0) < 0 else f"+{row.get('rate', 0.0)}",  # 비율 값이 음수일 경우 그대로 표시
        "volume": str(row["volume"])
    }

# 필터링된 데이터 조회 함수
async def get_filtered_data(symbol: str, interval: str):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    now = datetime.now()

    # 각 interval에 맞는 시간 기준을 설정
    if interval == "1m":
        time_threshold = now - timedelta(minutes=1)
        group_by = "MINUTE(date)"
    elif interval == "5m":
        time_threshold = now - timedelta(minutes=5)
        group_by = "FLOOR(MINUTE(date) / 5)"
    elif interval == "1d":
        time_threshold = now - timedelta(days=1)
        group_by = "DATE(date)"
    elif interval == "1w":
        time_threshold = now - timedelta(weeks=1)
        group_by = "WEEK(date)"
    elif interval == "1M":
        time_threshold = now - timedelta(weeks=4)  # 월 단위는 약 4주
        group_by = "MONTH(date)"
    elif interval == "1y":
        time_threshold = now - timedelta(days=365)
        group_by = "YEAR(date)"
    else:
        raise ValueError("Invalid interval")

    # 필터링된 데이터 조회 쿼리
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
            MAX(date) AS date
        FROM stock
        WHERE symbol = %s AND date >= %s
        GROUP BY {group_by}
        ORDER BY date_group DESC
    """

    # Execute the query with the symbol and time_threshold
    cursor.execute(query, (symbol, time_threshold))

    rows = cursor.fetchall()

    # 결과 형식 변환
    formatted_rows = [format_data(row) for row in rows]

    cursor.close()
    connection.close()
    return formatted_rows

# SSE 비동기 이벤트 생성기
async def sse_event_generator(consumer):
    try:
        while True:
            message = await consumer.getone()  # 비동기로 메시지 하나를 가져옴
            stock_data = message.value
            print(f"Sending stock data to client: {stock_data}")
            yield f"data: {json.dumps(stock_data)}\n\n"
            await asyncio.sleep(0.5)  # 메시지 간의 대기
    except Exception as e:
        print(f"Error in SSE generator: {e}")
    finally:
        await consumer.stop()