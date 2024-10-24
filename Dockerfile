# 베이스 이미지로 Python 3.9 사용
FROM python:3.9-slim

# 작업 디렉토리 설정
WORKDIR /Stock-Service

# Git 설치
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# requirements.txt 복사 후 필요한 패키지 설치
COPY src/requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# 서비스 소스 코드 복사
COPY src/ ./src

# 기본적으로 Kafka 컨슈머를 실행
CMD ["python3", "src/consumer.py"]