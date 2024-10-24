from fastapi import FastAPI, APIRouter
from starlette.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from .consumer import kafka_consumer
import asyncio
import json
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def hello():
    return {"message": "stock-service-consumer의 메인페이지입니다"}

# SSE 엔드포인트 (Kafka 데이터를 클라이언트에 스트리밍)
@app.get("/stream", response_class=StreamingResponse)
async def sse_stream():
    consumer = kafka_consumer()

    async def event_generator():
        for message in consumer:
            stock_data = message.value
            yield f"data: {json.dumps(stock_data)}\n\n"
            await asyncio.sleep(1)  # SSE 속도 조절

    return StreamingResponse(event_generator(), media_type="text/event-stream")