import aiomysql
import os
import mysql.connector
import asyncio
import time
import redis.asyncio as aioredis

async def get_db_connection_async(retries=3, delay=5):
    """
    비동기로 MySQL 데이터베이스 연결을 시도하며, 실패할 경우 지정된 횟수만큼 재시도합니다.

    Args:
        retries (int): 최대 재시도 횟수
        delay (int): 재시도 간격(초)

    Returns:
        aiomysql.Connection: MySQL 연결 객체
    Raises:
        Exception: 최대 재시도 후에도 실패 시 에러 발생
    """
    host = os.getenv("MYSQL_HOST", "mysql")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    for attempt in range(retries):
        try:
            connection = await aiomysql.connect(
                host=host,
                user=user,
                password=password,
                db=database
            )
            return connection
        except Exception as err:
            print(f"Database connection attempt {attempt + 1} failed: {err}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)  # 재시도 전에 대기
            else:
                print("All retries failed. Raising exception.")
                raise  # 최대 재시도 후 예외 발생


def get_db_connection(retries=3, delay=5):
    """
    데이터베이스 연결을 시도하며, 실패할 경우 지정된 횟수만큼 재시도합니다.

    Args:
        retries (int): 최대 재시도 횟수
        delay (int): 재시도 간격(초)

    Returns:
        mysql.connector.connection.MySQLConnection: MySQL 연결 객체
    Raises:
        mysql.connector.Error: 최대 재시도 후에도 실패 시 에러 발생
    """
    host = os.getenv("MYSQL_HOST", "mysql")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    for attempt in range(retries):
        try:
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            return connection
        except mysql.connector.Error as err:
            print(f"Database connection attempt {attempt + 1} failed: {err}")
            if attempt < retries - 1:
                time.sleep(delay)  # 재시도 전에 대기
            else:
                print("All retries failed. Raising exception.")
                raise  # 최대 재시도 후 예외 발생

async def get_redis():
    redis_url = os.getenv("REDIS_URL")
    redis = await aioredis.from_url(redis_url)
    return redis