from fastapi import Query, APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from datetime import datetime
from .service import sse_event_generator, get_filtered_data
import json
import asyncio
import pytz
from datetime import timezone, timedelta
from .database import get_db_connection
from .logger import logger

KST = pytz.timezone('Asia/Seoul')

router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stocks"],
)

@router.get("/stream/{symbol}", response_class=StreamingResponse)
async def sse_stream(symbol: str):
    topic = "real_time_stock_prices"
    now_kst = datetime.now()
    group_id = f"sse_consumer_group_{symbol}_{now_kst.strftime('%Y%m%d%H%M%S%f')}"
    return StreamingResponse(sse_event_generator(topic, group_id, symbol), media_type="text/event-stream")

@router.get("/streamFilter", response_class=StreamingResponse)
async def sse_filtered_stream(symbol: str = Query(...), interval: str = Query(...)):
    async def filtered_event_generator():
        # 초기 데이터 전송 (당일 0시 기준으로 전체 데이터를 가져옴)
        start_of_day = datetime.now(timezone.utc).astimezone(KST).replace(hour=0, minute=0, second=0, microsecond=0)
        initial_data = await get_filtered_data(symbol, interval, start_of_day)

        # 초기 데이터 전송
        if initial_data:
            yield f"data: {json.dumps(initial_data)}\n\n"
            last_sent_time = max(
                datetime.strptime(item['date'], "%H:%M:%S").replace(tzinfo=KST) for item in initial_data
            )
        else:
            last_sent_time = datetime.now(timezone.utc).astimezone(KST)

        while True:
            current_time = datetime.now(timezone.utc).astimezone(KST)

            # 구간 종료 시각 계산 및 대기
            if interval == "1m":
                # 매 1분 간격으로 데이터 전송
                next_period_end = current_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
            elif interval == "5m":
                # 매 5분 구간의 종료 시각 계산
                next_period_end = (current_time + timedelta(minutes=5 - current_time.minute % 5)).replace(second=0, microsecond=0)
            else:
                raise ValueError("Invalid interval")

            # 구간의 종료 시각까지 대기
            await asyncio.sleep((next_period_end - current_time).total_seconds())

            # 구간 종료 후 새로운 데이터 가져오기
            new_data = await get_filtered_data(symbol, interval, last_sent_time)

            # 중복 전송 방지를 위해 마지막으로 전송된 시간 이후 데이터만 전송
            if new_data:
                unique_data = [item for item in new_data if
                               datetime.strptime(item['date'], "%H:%M:%S").replace(tzinfo=KST) > last_sent_time]
                if unique_data:
                    yield f"data: {json.dumps(unique_data)}\n\n"
                    last_sent_time = max(
                        datetime.strptime(item['date'], "%H:%M:%S").replace(tzinfo=KST) for item in unique_data
                    )

            # 짧은 대기 시간을 통해 데이터 반복 전송 방지
            await asyncio.sleep(5)

    return StreamingResponse(filtered_event_generator(), media_type="text/event-stream")


@router.get("/historicalFilter")
async def get_historical_data_filtered(
        symbol: str = Query(..., description="Stock symbol to retrieve data for"),
        interval: str = Query(..., description="Time interval: 1d, 1w, 1m, 1y")
):
    # interval에 따른 그룹화 기준 및 날짜 형식 선택
    if interval == "1d":
        group_by_clause = "date"  # 일 단위 집계
        select_clause = "DATE_FORMAT(date, '%Y-%m-%d') AS date"
    elif interval == "1w":
        # 주 단위 집계를 5일 간격으로 그룹화
        # 각 symbol의 데이터 시작 날짜로부터 5일씩 묶어서 그룹화
        group_by_clause = "FLOOR(DATEDIFF(date, (SELECT MIN(date) FROM stock WHERE symbol = %s)) / 5)"
        select_clause = """
            DATE_FORMAT(MIN(date), '%Y-%m-%d') AS start_date,
            DATE_FORMAT(MAX(date), '%Y-%m-%d') AS end_date
        """
    elif interval == "1m":
        group_by_clause = "YEAR(date), MONTH(date)"  # 월 단위 집계
        select_clause = "YEAR(date) AS year, DATE_FORMAT(MAX(date), '%Y-%m') AS date"
    elif interval == "1y":
        group_by_clause = "YEAR(date)"  # 연 단위 집계
        select_clause = "YEAR(date) AS year"
    else:
        raise HTTPException(status_code=400, detail="Invalid interval. Use '1d', '1w', '1m', or '1y'.")

    # DB에서 집계 데이터 조회
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    query = f"""
        SELECT 
            symbol,
            {select_clause},
            AVG(open) AS open,
            AVG(close) AS close,
            MAX(high) AS high,
            MIN(low) AS low,
            SUM(volume) AS volume,
            AVG(rate) AS rate,
            AVG(rate_price) AS rate_price
        FROM stock
        WHERE symbol = %s
        GROUP BY symbol, {group_by_clause}
        ORDER BY MIN(date);
    """
    # 주간 집계일 경우, 쿼리에 symbol 파라미터를 두 번 전달 (하나는 기준 날짜 계산용)
    if interval == "1w":
        cursor.execute(query, (symbol, symbol))
    else:
        cursor.execute(query, (symbol,))
    results = cursor.fetchall()
    cursor.close()
    db.close()

    if not results:
        raise HTTPException(status_code=404, detail="No data found for the given symbol and interval.")

    return results

