from fastapi import Query, APIRouter
from fastapi.responses import StreamingResponse
from datetime import datetime
from .consumer import async_kafka_consumer
from .service import sse_event_generator, get_filtered_data
import json
import asyncio
import pytz
from datetime import timezone, timedelta

KST = pytz.timezone('Asia/Seoul')

router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stocks"],
)

# SSE 엔드포인트 (실시간 데이터 스트리밍)
@router.get("/stream/{symbol}", response_class=StreamingResponse)
async def sse_stream(symbol: str):
    topic = "real_time_stock_prices"
    now_kst = datetime.now(KST)
    group_id = f"sse_consumer_group_{symbol}_{now_kst.strftime('%Y%m%d%H%M%S%f')}"
    consumer = await async_kafka_consumer(topic, group_id)
    return StreamingResponse(sse_event_generator(consumer), media_type="text/event-stream")

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