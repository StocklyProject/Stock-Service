from src.database import get_redis, get_db_connection_async  
from fastapi import Depends, Request, HTTPException
from src.consumer import async_kafka_consumer  # Kafka Consumer 반환
from src.producer import init_kafka_producer  # Kafka Producer 초기화
import orjson  # 빠른 JSON 직렬화/역직렬화
from src.logger import logger  # 로거 설정
import uuid  # UUID 생성
import asyncio

# async def consume_real_time_prices():
#     """
#     Kafka Consumer로 실시간 가격 데이터를 구독하고, 조건 충족 시 알림 트리거.
#     """
#     group_id = f"stock_alert_compare_{uuid.uuid4()}" 
#     consumer = await async_kafka_consumer("real_time_stock_prices", group_id)

#     try:
#         connection = await get_db_connection_async()
#         async with connection.cursor() as cursor:  # 기본 Cursor 사용

#             async for message in consumer:
#                 try:
#                     # 메시지가 비어 있는지 확인
#                     if not message.value:
#                         logger.warning("Received an empty message, skipping...")
#                         continue

#                     # 메시지가 dict 형태인지 확인 후 로드
#                     if isinstance(message.value, dict):
#                         stock_data = message.value
#                     else:
#                         stock_data = orjson.loads(message.value)

#                     # 메시지에서 company_id와 close 값 가져오기
#                     company_id = stock_data.get("id")
#                     stock_close_price = stock_data.get("close")

#                     if not company_id or stock_close_price is None:
#                         logger.warning(f"Invalid stock data: {stock_data}, skipping...")
#                         continue

#                     # DB에서 알림 조건 조회
#                     query = """
#                     SELECT id, user_id, price
#                     FROM notification
#                     WHERE company_id = %s AND is_active = FALSE
#                     """
#                     await cursor.execute(query, (company_id,))
#                     notifications = await cursor.fetchall()


#                     # 알림 조건 확인 및 처리
#                     for notification in notifications:
#                         try:
#                             # 튜플 언패킹
#                             notification_id, user_id, price = notification
#                             logger.critical(f"Processing notification: id={notification_id}, user_id={user_id}, price={price}")

#                             # 값들을 float로 변환
#                             stock_close_price_float = float(stock_close_price)
#                             price_float = float(price)

#                             # 조건 확인
#                             if stock_close_price_float == price_float:
#                                 logger.critical(f"Condition met for notification {notification_id}: stock_close_price={stock_close_price_float}, price={price_float}")

#                                 # Kafka 프로듀서 초기화
#                                 producer = await init_kafka_producer()
#                                 await producer.send_and_wait(
#                                     "alert_triggers",
#                                     value=orjson.dumps({
#                                         "notification_id": notification_id,
#                                         "user_id": user_id,
#                                         "company_id": company_id,
#                                         "current_price": stock_close_price_float,
#                                         "symbol": stock_data.get("symbol")
#                                     })
#                                 )
#                                 logger.critical(f"Triggered alert for notification {notification_id} "
#                                             f"at price {stock_close_price_float} for company_id {company_id}.")
#                                 await producer.stop()

#                                 # 알림 상태 업데이트
#                                 update_query = """
#                                 UPDATE notification
#                                 SET is_active = TRUE
#                                 WHERE id = %s
#                                 """
#                                 await cursor.execute(update_query, (notification_id,))
#                                 await connection.commit()
#                                 logger.critical(f"Notification {notification_id} marked as active.")
#                             else:
#                                 logger.critical(f"Condition not met for notification {notification_id}: stock_close_price={stock_close_price_float}, price={price_float}")
#                         except Exception as notification_error:
#                             logger.error(f"Error processing notification {notification}: {notification_error}")

#                 except orjson.JSONDecodeError as decode_error:
#                     logger.error(f"Failed to decode JSON message: {decode_error}, message: {message.value}")
#                 except Exception as message_error:
#                     logger.error(f"Unexpected error during message processing: {message_error}")

#     except Exception as e:
#         logger.error(f"Error in real-time price consumer: {e}")
#     finally:
#         await consumer.stop()
#         if connection:
#             await connection.close()
#         logger.info("Real-time price consumer stopped.")


async def consume_real_time_prices():
    """
    Kafka Consumer로 실시간 가격 데이터를 구독하고, 조건을 Kafka에서 조회하여 검사.
    """
    group_id = f"stock_alert_compare_{uuid.uuid4()}"
    consumer = await async_kafka_consumer("real_time_stock_prices", group_id)
    condition_consumer = await async_kafka_consumer("stock_prices_alert", f"condition_reader_{uuid.uuid4()}")

    try:
        # 조건 데이터 캐싱 (메모리 기반)
        conditions = []

        async def load_conditions():
            """Kafka에서 조건을 지속적으로 로드."""
            async for condition_message in condition_consumer:
                try:
                    if isinstance(condition_message.value, dict):
                        condition = condition_message.value
                    else:
                        condition = orjson.loads(condition_message.value)

                    conditions.append(condition)
                except Exception as e:
                    logger.error(f"Failed to load condition from message: {e}, message: {condition_message.value}")

        async def process_stock_data():
            """Kafka에서 실시간 주식 데이터를 읽고 조건을 확인."""
            async for stock_message in consumer:
                try:
                    if not stock_message.value:
                        logger.warning("Received an empty stock message, skipping...")
                        continue

                    stock_data = (
                        stock_message.value if isinstance(stock_message.value, dict)
                        else orjson.loads(stock_message.value)
                    )
                    company_id = stock_data.get("id")
                    stock_close_price = float(stock_data.get("close", 0))


                    if not company_id or stock_close_price is None:
                        logger.warning(f"Invalid stock data: {stock_data}, skipping...")
                        continue

                    # 관련 조건 필터링
                    relevant_conditions = [
                        cond for cond in conditions
                        if cond["company_id"] == company_id and not cond["is_active"]
                    ]

                    # 조건 검사 및 알림
                    for condition in relevant_conditions:
                        try:
                            target_price = float(condition["target_price"])
                            if stock_close_price == target_price:

                                producer = await init_kafka_producer()
                                await producer.send_and_wait(
                                    "alert_triggers",
                                    value=orjson.dumps({
                                        "notification_id": condition.get("notification_id"),
                                        "user_id": condition.get("user_id"),
                                        "company_id": condition.get("company_id"),
                                        "company_name": condition.get("company_name"),  # 회사 이름 추가
                                        "symbol": condition.get("symbol"),            # 심볼 추가
                                        "current_price": stock_close_price
                                    })
                                )
                                await producer.stop()

                                # 조건 상태 업데이트
                                condition["is_active"] = True
                            else:
                                logger.debug(f"Condition not met for condition {condition.get('notification_id')}: stock_close_price={stock_close_price}, target_price={target_price}")
                        except Exception as condition_error:
                            logger.error(f"Error processing condition {condition}: {condition_error}")

                except orjson.JSONDecodeError as decode_error:
                    logger.error(f"Failed to decode stock message: {decode_error}, message: {stock_message.value}")
                except Exception as stock_error:
                    logger.error(f"Unexpected error processing stock message: {stock_error}")

        # 동시에 두 작업 실행
        await asyncio.gather(load_conditions(), process_stock_data())

    except Exception as e:
        logger.error(f"Error in real-time price consumer: {e}")
    finally:
        await consumer.stop()
        await condition_consumer.stop()
        logger.info("Real-time price consumer and condition consumer stopped.")



async def get_user_id_from_session(request: Request, redis=Depends(get_redis)):
    # 쿠키에서 session_id 가져오기
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID is missing")

    # Redis에서 session_id로 user_id 조회
    user_id = await redis.get(session_id)
    if not user_id:
        raise HTTPException(status_code=401, detail="Session has expired or is invalid")

    return user_id.decode('utf-8')  # Redis에서 가져온 값을 문자열로 반환