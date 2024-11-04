import mysql.connector
import os

# 도커
# def get_db_connection():
#     connection = mysql.connector.connect(
#         host=os.environ.get("MYSQL_HOST"),
#         user=os.environ.get("MYSQL_USER"),
#         password=os.environ.get("MYSQL_PASSWORD"),
#         database=os.environ.get("MYSQL_DATABASE"),
#         port=3306
#     )
#     return connection


# 쿠버네티스
def get_db_connection():
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    # 디버깅용 출력
    print(f"Connecting to MySQL with USER: {user}, PASSWORD: {password}, DATABASE: {database}")

    connection = mysql.connector.connect(
        # host="stockDB",
        host="mysql",
        user=user,
        password=password,
        database=database
    )
    return connection