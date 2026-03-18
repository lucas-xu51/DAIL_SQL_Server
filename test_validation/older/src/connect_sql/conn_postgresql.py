import psycopg2
from func_timeout import func_timeout, FunctionTimedOut
def connect_postgresql():
    # Open database connection
    # Connect to the database
    db = psycopg2.connect(
        "dbname=BIRD user=user host=localhost password=123456 port=5432"
    )
    return db

def execute_mysql_query_safe(query: str, timeout_seconds: int = 30):
    """安全执行MySQL查询，返回结果和错误信息"""
    db = None
    cursor = None
    try:
        # db = connect_mysql()
        db = connect_postgresql()
        cursor = db.cursor()
        
        # 使用 func_timeout 进行超时控制
        result = func_timeout(
            timeout_seconds,
            cursor.execute,
            args=(query,)
        )
        
        query_result = cursor.fetchall()
        return query_result, None
        
    except FunctionTimedOut:
        return f'Error executing SQL', "timeout"
    except Exception as e:
        
        return f'Error executing SQL:{str(e)}', f"error: {str(e)}"
    finally:
        if cursor:
            cursor.close()
        if db:
            db.close()