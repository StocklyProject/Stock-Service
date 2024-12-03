from fastapi import APIRouter, Request, Depends
from src.database import get_redis, get_db_connection_async
from .service import get_user_id_from_session
from fastapi.responses import StreamingResponse
from src.consumer import async_kafka_consumer
from src.logger import logger
import orjson
import uuid
import asyncio
import json

router = APIRouter(
    prefix="/api/v1/alert",
    tags=["alerts"],
)

@router.get("/stream", response_class=StreamingResponse)
async def stream_alerts(request: Request, redis=Depends(get_redis)):
    """
    SSE를 통해 실시간 알림 데이터를 스트리밍.
    Kafka Consumer가 `alert_triggers` 토픽을 구독하여 조건 충족 데이터를 스트리밍.
    """
    # 사용자 ID 가져오기
    user_id = await get_user_id_from_session(request, redis)
    logger.critical(f"User {user_id} connected to SSE stream.")

    # Kafka Consumer 생성
    group_id = f"sse_stock_alert_{uuid.uuid4()}"
    consumer = await async_kafka_consumer("alert_triggers", group_id)

    async def event_generator():
        try:
            logger.critical(f"SSE event generator started for user_id={user_id}.")
            async for message in consumer:
                try:
                    if not message.value:
                        logger.warning("Received an empty alert message, skipping...")
                        continue

                    # Kafka 메시지 디코딩
                    alert_data = (
                        message.value if isinstance(message.value, dict)
                        else orjson.loads(message.value)
                    )

                    logger.critical(f"Received alert data: {alert_data}")

                    # 사용자 ID 필터링 (타입 변환 및 비교)
                    alert_user_id = str(alert_data.get("user_id"))  # `alert_data`에서 user_id 추출
                    request_user_id = str(user_id)  # Redis에서 가져온 user_id를 문자열로 변환

                    if alert_user_id == request_user_id:
                        logger.critical(f"Streaming alert for user {request_user_id}: {alert_data}")
                        yield f"data: {orjson.dumps(alert_data).decode('utf-8')}\n\n"

                        # 알림을 성공적으로 SSE로 전송했으므로 is_active를 1로 업데이트
                        notification_id = alert_data.get("notification_id")
                        if notification_id:
                            connection = await get_db_connection_async()
                            async with connection.cursor() as cursor:
                                update_query = """
                                UPDATE notification
                                SET is_active = TRUE
                                WHERE id = %s
                                """
                                await cursor.execute(update_query, (notification_id,))
                                await connection.commit()
                                logger.critical(f"Notification {notification_id} marked as active.")
                    else:
                        logger.critical(f"Alert not for user {request_user_id}, skipping: {alert_data}")

                except orjson.JSONDecodeError as decode_error:
                    logger.error(f"Failed to decode alert message: {decode_error}, message: {message.value}")
                except Exception as message_error:
                    logger.error(f"Unexpected error processing alert message: {message_error}")

        except Exception as e:
            logger.error(f"Error in SSE stream: {e}")
        finally:
            await consumer.stop()
            logger.critical("SSE consumer stopped.")

    return StreamingResponse(event_generator(), media_type="text/event-stream")
