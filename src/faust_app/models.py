import faust

class Stock(faust.Record, serializer='json'):
    id: int
    name: str
    symbol: str
    timestamp: str
    open: float
    close: float
    high: float
    low: float
    rate_price: float
    rate: float
    volume: int
    trading_value: float