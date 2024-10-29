from fastapi import Query, APIRouter
from fastapi.responses import StreamingResponse
from datetime import datetime
from .consumer import async_kafka_consumer
from .service import sse_event_generator,get_filtered_data
import json
import asyncio

router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stocks"],
)

# SSE 엔드포인트 (실시간 데이터 스트리밍)
@router.get("/stream/{symbol}", response_class=StreamingResponse)
async def sse_stream(symbol: str):
    topic = "real_time_stock_prices"
    group_id = f"sse_consumer_group_{symbol}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    consumer = await async_kafka_consumer(topic, group_id)
    return StreamingResponse(sse_event_generator(consumer), media_type="text/event-stream")


# 필터링된 데이터 SSE 스트리밍 엔드포인트 : 필터링 로직 추가
@router.get("/streamFilter", response_class=StreamingResponse)
async def sse_filtered_stream(symbol: str = Query(...), interval: str = Query(...)):
    async def filtered_event_generator():
        while True:
            data = await get_filtered_data(symbol, interval)
            yield f"data: {json.dumps(data)}\n\n"
            await asyncio.sleep(60)  # 1분마다 필터링된 데이터 가져옴 (필요시 조정 가능)

    return StreamingResponse(filtered_event_generator(), media_type="text/event-stream")