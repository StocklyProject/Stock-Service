import asyncio
import orjson
from fastapi.responses import StreamingResponse
from datetime import datetime, timedelta, timezone
from src.logger import logger
from .app import real_time_table

KST = timezone(timedelta(hours=9))

async def sse_event_generator(symbol: str):
    """
    SSE 이벤트 생성기: 한국 시간 기준으로 요청 시간 이후 데이터를 실시간으로 스트리밍
    """
    last_sent_data = None

    # 현재 한국 시간 기준 시작 시간 설정
    start_time_dt = datetime.now(KST)
    logger.info(f"Starting SSE generation for symbol: {symbol} after {start_time_dt}")

    while True:
        try:
            # 요청 시간 이후의 데이터 필터링
            matching_data = [
                value for key, value in real_time_table.items()
                if key == symbol and 
                datetime.strptime(value["timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST) > start_time_dt
            ]

            # 데이터가 없으면 대기
            if not matching_data:
                logger.debug(f"No new data for symbol: {symbol}. Retrying...")
                await asyncio.sleep(0.1)
                continue

            # 타임스탬프, 파티션, 오프셋 기준으로 정렬
            sorted_data = sorted(
                matching_data,
                key=lambda x: (
                    datetime.strptime(x["timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST),  # 타임스탬프
                    x.get("partition", 0),  # 파티션
                    x.get("offset", 0),  # 오프셋
                ),
            )

            # 최신 데이터 가져오기
            latest_data = sorted_data[-1]
            logger.info(f"Latest sorted data for symbol {symbol}: {latest_data}")

            # 중복 데이터 판별
            if last_sent_data is None or (
                latest_data["timestamp"] != last_sent_data["timestamp"] or
                latest_data["partition"] != last_sent_data["partition"] or
                latest_data["offset"] != last_sent_data["offset"]
            ):
                last_sent_data = latest_data
                yield f"data: {orjson.dumps(latest_data).decode('utf-8')}\n\n"
                logger.info(f"Sent latest data for {symbol}: {latest_data}")
            else:
                logger.debug(f"Duplicate data detected for {symbol}. Skipping...")

            # 다음 데이터를 기다림
            await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"Unexpected error in SSE generator for symbol {symbol}: {e}")
            break



def sse_stream(symbol: str):
    """
    SSE 스트림 생성.
    """
    response = StreamingResponse(
        sse_event_generator(symbol),
        media_type="text/event-stream"
    )
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    return response
