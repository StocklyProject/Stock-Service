from fastapi import Query, APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from datetime import datetime
from .service import sse_event_generator, get_filtered_data, get_symbols_for_page, sse_pagination_generator, get_latest_symbols_data
import json
import asyncio
import pytz
from datetime import timezone, timedelta
from .database import get_db_connection
from aiokafka import AIOKafkaConsumer
from .logger import logger
import uuid
from .faust_app.sse import sse_stream
KST = pytz.timezone('Asia/Seoul')

router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stocks"],
)


@router.get("/sse/stream/{symbol}", response_class=StreamingResponse)
async def sse_stream(symbol: str):
    topic = "real_time_stock_prices"
    now_kst = datetime.now()
    group_id = f"sse_consumer_group_{uuid.uuid4()}"  # 고유한 group_id 생성
    return StreamingResponse(sse_event_generator(topic, group_id, symbol), media_type="text/event-stream")


@router.get("/sse/streamFilter", response_class=StreamingResponse)
async def sse_filtered_stream(symbol: str = Query(...), interval: str = Query(...)):
    async def filtered_event_generator():
        # 초기 데이터 설정
        start_of_day = datetime.now(timezone.utc).astimezone(KST).replace(hour=0, minute=0, second=0, microsecond=0)
        initial_data = await get_filtered_data(symbol, interval, start_of_day)
        last_sent_time = start_of_day

        if initial_data:
            yield f"data: {json.dumps(initial_data)}\n\n"
            last_sent_time = max(
                datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST) for item in initial_data
            )

        while True:
            try:
                new_data = await get_filtered_data(symbol, interval, last_sent_time)
                if new_data:
                    filtered_data = [
                        item for item in new_data
                        if datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST) > last_sent_time
                    ]
                    if filtered_data:
                        yield f"data: {json.dumps(filtered_data)}\n\n"
                        last_sent_time = max(
                            datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST) for item in filtered_data
                        )
            except asyncio.CancelledError:
                break

            await asyncio.sleep(0.5)

    # SSE 스트림 실행
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
            AND date < CURDATE() 
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

@router.get("/sse/stream/multiple/symbols", response_class=StreamingResponse)
async def sse_stream_multiple(page: int = Query(1)):
    symbols = get_symbols_for_page(page)
    # group_id = f"sse_consumer_group_{page}"  # 고유한 group_id 생성
    group_id = f"sse_consumer_group_{uuid.uuid4()}" 
    topic = "real_time_stock_prices"

    return StreamingResponse(sse_pagination_generator(topic, group_id, symbols), media_type="text/event-stream")

@router.get("/symbols")
async def get_latest_symbols(page: int = Query(1)):
    symbols = get_symbols_for_page(page)
    latest_data = get_latest_symbols_data(symbols)
    return latest_data

@router.get("/faust_stream/{symbol}", response_class=StreamingResponse)
async def stream_stock_data(symbol: str):
    """
    SSE를 통해 실시간으로 주식 데이터를 스트리밍
    """
    return sse_stream(symbol)
