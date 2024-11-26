import faust
from datetime import datetime, timedelta
from .models import Stock
from src.logger import logger
from pytz import timezone
from faust import current_event
from fastapi import APIRouter
from faust.tables.wrappers import WindowSet

router = APIRouter(
    prefix="/api/v1",
    tags=["stocks"],
)

# Faust 앱 정의
app_faust = faust.App(
    'stock_stream_app',
    broker='kafka://kafka:9092',
    store='memory://',
)

# Kafka 토픽 정의
stock_topic = app_faust.topic('real_time_stock_prices', key_type=str, value_type=Stock)
processed_topic = app_faust.topic('one_minutes_stock_prices', value_type=dict)


# 5분봉 테이블 정의
# five_minute_table = app_faust.Table(
#     'five_minute_table',
#     default=lambda: dict,
#     partitions=15,
#     on_window_close=lambda key, value, window: logger.info(f"5분봉 윈도우 닫힘: {key}, {value}")
# ).tumbling(
#     timedelta(minutes=5),
#     expires=timedelta(minutes=15),
#     key_index=True
# )

real_time_table = app_faust.Table(
    'real_time_table',
    default=dict,  # 딕셔너리 형태로 관리
    partitions=15
)

@app_faust.agent(stock_topic, concurrency=5)
async def process_stocks(stocks):
    """
    Kafka에서 들어온 주식 데이터를 처리하고 실시간 테이블을 갱신하는 에이전트.
    """
    async for stock in stocks:
        try:
            # Kafka 이벤트 메타데이터 가져오기
            event = current_event()
            if event:
                partition = event.message.partition
                # logger.info(f"Received data from partition {partition}: {stock}")
            if not event:
                logger.warning("No current event found. Skipping...")
                continue

            partition = event.message.partition
            offset = event.message.offset

            # Stock 데이터를 dict로 변환
            if hasattr(stock, "to_representation"):
                stock_dict = stock.to_representation()
            elif isinstance(stock, dict):
                stock_dict = stock
            else:
                # Stock 객체에서 속성 추출
                stock_dict = {
                    "id": stock.id,
                    "name": stock.name,
                    "symbol": stock.symbol,
                    "timestamp": stock.timestamp,
                    "open": stock.open,
                    "close": stock.close,
                    "high": stock.high,
                    "low": stock.low,
                    "rate_price": stock.rate_price,
                    "rate": stock.rate,
                    "volume": stock.volume,
                    "trading_value": stock.trading_value,
                }
            stock_dict.pop("__faust", None)

            # 심볼 추출
            symbol = stock_dict.get("symbol")
            if not symbol:
                logger.error("Stock data missing 'symbol'. Skipping...")
                continue

            # 새 데이터 생성
            new_entry = {
                "timestamp": stock_dict.get("timestamp"),
                "partition": partition,
                "offset": offset,
                "data": stock_dict,
            }

            # 기존 데이터 확인
            current_data = real_time_table[symbol]

            # 중복 데이터 확인
            if isinstance(current_data, dict):
                if (
                    current_data.get("partition") == partition
                    and current_data.get("offset") == offset
                ):
                    continue

            # 테이블 갱신 (항상 하나의 메시지만 유지)
            real_time_table[symbol] = new_entry

        except KeyError as e:
            logger.error(f"KeyError while processing stock data: {e}")
        except TypeError as e:
            logger.error(f"TypeError while processing stock data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while processing stock data: {e}")



# 윈도우가 닫힐 때만 데이터를 저장하고 초기화
def save_on_window_close(key, value, window):
    """
    윈도우가 닫힐 때 데이터를 저장하고 초기화합니다.
    """
    try:
        logger.critical(f"Window closed for {key}: {value}")

        # 1분 동안의 데이터를 저장
        one_minute_table[f"saved_{key}_{int(datetime.utcnow().timestamp())}"] = value

        # 초기화
        one_minute_table[key] = {
            "id": value["id"],
            "name": value["name"],
            "symbol": value["symbol"],
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "open": value["close"],  # 이전 close 값을 다음 open으로 사용
            "close": 0.0,
            "high": 0.0,
            "low": float("inf"),
            "rate_price": 0.0,
            "rate": 0.0,
            "volume": 0,
            "trading_value": 0,
        }
        logger.info(f"Reset completed for {key}")
    except Exception as e:
        logger.error(f"Error in save_on_window_close for {key}: {e}")







# 1분봉 테이블 정의
one_minute_table = app_faust.Table(
    'one_minute_table',
    default=lambda: {},  # 기본값으로 빈 딕셔너리를 사용
    partitions=15,
).tumbling(
    timedelta(minutes=1),  # 1분 단위 윈도우
    expires=timedelta(hours=1),  # 1시간 동안 만료되지 않도록 설정
    key_index=True
)

# # 한국 시간대
# KST = timezone("Asia/Seoul")


# def convert_to_kst(timestamp):
#     """
#     UTC 타임스탬프를 KST로 변환합니다.
#     """
#     return timestamp.astimezone(KST).replace(tzinfo=None)


# def update_candlestick(candlestick, data_list):
#     """
#     1분 윈도우 내 모든 데이터를 하나의 캔들스틱 데이터로 집계합니다.
#     :param candlestick: 집계할 초기 캔들스틱 데이터 (dict)
#     :param data_list: 1분 윈도우 내 데이터 리스트 (list of dict)
#     """
#     for data in data_list:
#         if not isinstance(data, dict):
#             logger.error(f"Invalid data format in update_candlestick: {data}, type: {type(data)}")
#             continue

#         try:
#             high = float(data["high"])
#             low = float(data["low"])
#             close = float(data["close"])
#             volume = int(data.get("volume", 0))
#             trading_value = float(data.get("trading_value", 0))
#             rate_price = float(data.get("rate_price", 0))
#         except (ValueError, TypeError, KeyError) as e:
#             logger.error(f"Data validation error in update_candlestick: {data} - {e}")
#             continue

#         # 캔들스틱 데이터 업데이트
#         candlestick["high"] = max(candlestick.get("high", high), high)
#         candlestick["low"] = min(candlestick.get("low", low), low)
#         candlestick["close"] = close
#         candlestick["volume"] += volume
#         candlestick["trading_value"] += trading_value

#         # 변동률 계산
#         if candlestick.get("rate_price", 0) > 0:
#             previous_rate_price = candlestick["rate_price"]
#             candlestick["rate"] = ((rate_price - previous_rate_price) / previous_rate_price) * 100

#         candlestick["rate_price"] = rate_price


# @app_faust.agent(stock_topic)
# async def process_stock_data(stocks):
#     """
#     Kafka 메시지를 수신하여 1분봉 데이터를 테이블에 저장하고 처리된 데이터를 Kafka 토픽에 전송합니다.
#     """
#     async for stock in stocks:
#         try:
#             # Stock 데이터를 dict로 변환
#             logger.info(f"Received stock: {stock}, type: {type(stock)}")
#             stock_dict = stock.to_representation() if hasattr(stock, "to_representation") else stock.__dict__
#             logger.info(f"Converted stock_dict: {stock_dict}, type: {type(stock_dict)}")

#             if not isinstance(stock_dict, dict):
#                 logger.error(f"Invalid stock format: {stock}")
#                 continue

#             # 타임스탬프 변환
#             stock_timestamp = datetime.strptime(stock_dict["timestamp"], "%Y-%m-%d %H:%M:%S")
#             stock_timestamp_kst = convert_to_kst(stock_timestamp)

#             # Symbol 및 1분 윈도우 키 생성
#             symbol = stock_dict["symbol"]
#             window_start = stock_timestamp_kst.replace(second=0, microsecond=0)
#             table_key = f"{symbol}_{window_start}"

#             # 현재 윈도우 데이터 가져오기
#             window_set = one_minute_table[table_key]
#             current_window_data = window_set.now()  # 현재 윈도우 데이터 가져오기
#             logger.info(f"Window data: {current_window_data}, type: {type(current_window_data)}")

#             # 윈도우 데이터가 없으면 초기화
#             if not current_window_data:
#                 current_window_data = {
#                     "id": stock.id,
#                     "name": stock.name,
#                     "symbol": stock.symbol,
#                     "timestamp": window_start.strftime("%Y-%m-%d %H:%M:%S"),
#                     "open": stock_dict["open"],
#                     "close": stock_dict["close"],
#                     "high": stock_dict["high"],
#                     "low": stock_dict["low"],
#                     "volume": 0,
#                     "trading_value": 0,
#                     "rate_price": 0,
#                     "rate": 0,
#                 }

#             # 캔들스틱 데이터 업데이트
#             logger.info(f"Updating candlestick with stock_dict: {stock_dict}, type: {type(stock_dict)}")
#             update_candlestick(current_window_data, [stock_dict])
#             one_minute_table[table_key] = current_window_data

#             # Kafka 토픽에 처리된 데이터 저장
#             try:
#                 trading_value = float(current_window_data.get("trading_value", 0))
#                 volume = float(current_window_data.get("volume", 0))
#                 average_price = trading_value / volume if volume > 0 else 0.0

#                 candlestick = {
#                     "id": stock.id,
#                     "name": stock.name,
#                     "symbol": stock.symbol,
#                     "timestamp": current_window_data["timestamp"],
#                     "open": current_window_data["open"],
#                     "close": current_window_data["close"],
#                     "high": current_window_data["high"],
#                     "low": current_window_data["low"],
#                     "average_price": average_price,
#                     "volume": volume,
#                     "trading_value": trading_value,
#                     "rate": current_window_data["rate"],
#                     "rate_price": current_window_data["rate_price"],
#                 }

#                 await processed_topic.send(value=candlestick)
#                 logger.info(f"Successfully sent candlestick to Kafka: {candlestick}")

#             except Exception as send_error:
#                 logger.error(f"Failed to send candlestick to Kafka: {current_window_data}, error: {send_error}")

#             logger.info(f"Updated 1-minute table for {table_key}: {current_window_data}")

#         except KeyError as ke:
#             logger.error(f"KeyError while processing stock data: {ke}, stock_dict: {stock_dict}")
#         except TypeError as te:
#             logger.error(f"TypeError while processing stock data: {te}, stock_dict: {stock_dict}")
#         except ValueError as ve:
#             logger.error(f"ValueError while processing stock data: {ve}, stock_dict: {stock_dict}")
#         except Exception as e:
#             logger.error(f"Unexpected error while processing stock data: {e}, stock_dict: {stock_dict}")


# 한국 시간대
KST = timezone("Asia/Seoul")

def convert_to_kst(timestamp):
    """
    UTC 타임스탬프를 KST로 변환합니다.
    """
    return timestamp.astimezone(KST).replace(tzinfo=None)

def update_candlestick(candlestick, data_list):
    """
    1분 윈도우 내 모든 데이터를 하나의 캔들스틱 데이터로 집계합니다.
    :param candlestick: 집계할 초기 캔들스틱 데이터 (dict)
    :param data_list: 1분 윈도우 내 데이터 리스트 (list of dict)
    """
    for data in data_list:
        if not isinstance(data, dict):
            logger.error(f"Invalid data format in update_candlestick: {data}, type: {type(data)}")
            continue

        try:
            high = float(data["high"])
            low = float(data["low"])
            close = float(data["close"])
            volume = int(data.get("volume", 0))
            trading_value = float(data.get("trading_value", 0))
            rate_price = float(data.get("rate_price", 0))
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Data validation error in update_candlestick: {data} - {e}")
            continue

        # 데이터 검증: candlestick 값이 잘못된 경우 초기화
        if not isinstance(candlestick.get("high"), (int, float)):
            candlestick["high"] = high
        if not isinstance(candlestick.get("low"), (int, float)):
            candlestick["low"] = low
        if not isinstance(candlestick.get("close"), (int, float)):
            candlestick["close"] = close

        # 캔들스틱 데이터 업데이트
        try:
            candlestick["high"] = max(candlestick["high"], high)
            candlestick["low"] = min(candlestick["low"], low)
            candlestick["close"] = close
            candlestick["volume"] += volume
            candlestick["trading_value"] += trading_value

            # 변동률 계산
            if candlestick.get("rate_price", 0) > 0:
                previous_rate_price = candlestick["rate_price"]
                candlestick["rate"] = ((rate_price - previous_rate_price) / previous_rate_price) * 100

            candlestick["rate_price"] = rate_price

        except Exception as e:
            logger.error(f"Error updating candlestick: {e}, candlestick: {candlestick}")
            continue

@app_faust.agent(stock_topic)
async def process_stock_data(stocks):
    """
    Kafka 메시지를 수신하여 1분봉 데이터를 테이블에 저장하고 처리된 데이터를 Kafka 토픽에 전송합니다.
    """
    processed_windows = set()  # 처리된 윈도우 키를 저장할 집합

    async for stock in stocks:
        try:
            # Stock 데이터를 dict로 변환
            logger.info(f"Received stock: {stock}, type: {type(stock)}")
            stock_dict = stock.to_representation() if hasattr(stock, "to_representation") else stock.__dict__

            # '__faust' 키 제거
            if "__faust" in stock_dict:
                del stock_dict["__faust"]
                logger.info(f"Removed '__faust' from stock_dict: {stock_dict}")

            if not isinstance(stock_dict, dict):
                logger.error(f"Invalid stock format: {stock}")
                continue

            # 타임스탬프 변환
            stock_timestamp = datetime.strptime(stock_dict["timestamp"], "%Y-%m-%d %H:%M:%S")
            stock_timestamp_kst = convert_to_kst(stock_timestamp)

            # Symbol 및 1분 윈도우 키 생성
            symbol = stock_dict["symbol"]
            window_start = stock_timestamp_kst.replace(second=0, microsecond=0)
            table_key = f"{symbol}_{window_start}"

            # 현재 윈도우 데이터 가져오기
            window_set = one_minute_table[table_key]  # WindowSet 객체 반환
            current_window_data = window_set.now()  # 현재 윈도우 데이터 가져오기
            logger.critical(f"Window data: {current_window_data}, type: {type(current_window_data)}")

            # 윈도우 데이터가 없으면 초기화
            if not current_window_data:
                current_window_data = {
                    "id": stock_dict["id"],
                    "name": stock_dict["name"],
                    "symbol": stock_dict["symbol"],
                    "timestamp": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "open": stock_dict["open"],
                    "close": stock_dict["close"],
                    "high": stock_dict["high"],
                    "low": stock_dict["low"],
                    "volume": 0,
                    "trading_value": 0,
                    "rate_price": 0,
                    "rate": 0,
                }

            # 캔들스틱 데이터 업데이트
            logger.info(f"Updating candlestick with stock_dict: {stock_dict}, type: {type(stock_dict)}")
            update_candlestick(current_window_data, [stock_dict])
            window_set.now()  # 업데이트된 데이터 반영
            one_minute_table[table_key] = current_window_data

            # Kafka 전송 로직 (1분에 한 번만 전송)
            now = datetime.now(KST).replace(tzinfo=None).replace(second=0, microsecond=0)  # offset-naive로 변환
            if window_start < now and table_key not in processed_windows:
                try:
                    trading_value = float(current_window_data.get("trading_value", 0))
                    volume = float(current_window_data.get("volume", 0))
                    average_price = trading_value / volume if volume > 0 else 0.0

                    candlestick = {
                        "id": current_window_data["id"],
                        "name": current_window_data["name"],
                        "symbol": current_window_data["symbol"],
                        "timestamp": current_window_data["timestamp"],
                        "open": current_window_data["open"],
                        "close": current_window_data["close"],
                        "high": current_window_data["high"],
                        "low": current_window_data["low"],
                        "average_price": average_price,
                        "volume": int(volume),
                        "trading_value": trading_value,
                        "rate": current_window_data["rate"],
                        "rate_price": current_window_data["rate_price"],
                    }

                    await processed_topic.send(value=candlestick)
                    logger.critical(f"Successfully sent candlestick to Kafka: {candlestick}")
                    processed_windows.add(table_key)  # 처리된 윈도우로 기록

                except Exception as send_error:
                    logger.error(f"Failed to send candlestick to Kafka: {current_window_data}, error: {send_error}")

            logger.critical(f"Updated 1-minute table for {table_key}: {current_window_data}")

        except KeyError as ke:
            logger.error(f"KeyError while processing stock data: {ke}, stock_dict: {stock_dict}")
        except ValueError as ve:
            logger.error(f"ValueError while processing stock data: {ve}, stock_dict: {stock_dict}")
        except Exception as e:
            logger.error(f"Unexpected error while processing stock data: {e}, stock_dict: {stock_dict}")
