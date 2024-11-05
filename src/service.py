import asyncio
import json
from .database import get_db_connection
from datetime import datetime
import pytz
from .consumer import async_kafka_consumer
from typing import List
from .logger import logger

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
                continue

            # JSON으로 파싱된 데이터에서 symbol을 확인
            if isinstance(data, dict) and data.get("symbol") == symbol:
                yield f"data: {json.dumps(data)}\n\n"  # 클라이언트에 데이터 전송

            await asyncio.sleep(0.5)  # 메시지 간 대기 시간 설정
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()

async def sse_pagination_generator(topic: str, group_id: str, symbol: List[str]):
    # Kafka Consumer 생성 및 시작 로그
    logger.info(f"Kafka consumer 생성 - 그룹 ID: {group_id}")
    consumer = async_kafka_consumer(topic, group_id)
    logger.info(f"Kafka consumer 시작됨 - 그룹 ID: {group_id}")

    # 테스트를 위해 symbol 리스트에서 첫 번째 심볼 선택
    single_symbol = symbol[0] if symbol else None

    # 데이터 버퍼링 및 전송 조건 설정
    buffer = []  # 데이터를 임시 저장할 버퍼 리스트
    batch_size = 20  # 한번에 전송할 데이터 개수
    batch_interval = 5  # 데이터 전송 주기 (초)

    try:
        last_send_time = datetime.now()  # 마지막 데이터 전송 시각 초기화
        logger.info("Kafka 메시지 수신 대기 중...")

        # Kafka 메시지 수신 루프
        async for message in consumer:
            # 메시지를 JSON 형식으로 파싱
            try:
                data = json.loads(message.value) if isinstance(message.value, str) else message.value
                logger.debug(f"수신된 메시지 데이터: {data}")
            except json.JSONDecodeError:
                logger.error(f"메시지 디코딩 실패 - 원본 메시지: {message.value}")
                continue

            # JSON 데이터에서 single_symbol과 일치하는지 확인하여 버퍼에 추가
            if isinstance(data, dict) and data.get("symbol") == single_symbol:
                buffer.append(data)
                logger.info(f"버퍼에 추가된 데이터: {data}")

            # 전송 조건: 버퍼 크기가 batch_size 이상이거나 batch_interval 경과
            current_time = datetime.now()
            if len(buffer) >= batch_size or (current_time - last_send_time).total_seconds() >= batch_interval:
                if buffer:
                    logger.info(f"데이터 전송 - 버퍼 크기: {len(buffer)}")
                    yield f"data: {json.dumps(buffer)}\n\n"  # SSE 클라이언트로 데이터 전송
                    buffer.clear()  # 버퍼 초기화
                    last_send_time = current_time  # 전송 시각 업데이트

            await asyncio.sleep(0.5)  # 메시지 간 대기 시간 설정

    except asyncio.CancelledError:
        logger.info("클라이언트 연결 취소로 SSE 생성기 종료")
    finally:
        await consumer.stop()
        logger.info(f"Kafka consumer 중지 - 토픽: {topic}, 그룹 ID: {group_id}")


