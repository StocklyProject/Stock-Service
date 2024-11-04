from .database import get_db_connection
from typing import List

def get_symbols_for_page(page: int, page_size: int = 20) -> List[str]:
    start_index = (page - 1) * page_size
    database = get_db_connection()
    cursor = database.cursor()

    query = """
        SELECT symbol
        FROM company
        WHERE is_deleted = 0
        ORDER BY id
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, (page_size, start_index))
    # 심볼만 리스트로 반환
    symbol = [row[0] for row in cursor.fetchall()]

    cursor.close()
    database.close()

    return symbol