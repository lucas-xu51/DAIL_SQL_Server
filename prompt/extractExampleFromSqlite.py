import sqlite3

def extract_examples_from_sqlite(db_path, table_names, sample_size=3):
    """
    从sqlite数据库中提取每张表每个字段的示例数据，限制每字段最多sample_size个示例
    
    返回格式：
    {
        "table.column": [val1, val2, val3],
        ...
    }
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    examples = {}

    for table in table_names:
        # 获取表结构，取出列名
        cur.execute(f"PRAGMA table_info({table})")
        columns_info = cur.fetchall()  # [(cid, name, type, notnull, dflt_value, pk), ...]
        columns = [col[1] for col in columns_info]

        for col in columns:
            # 取sample_size条示例（非空），防止表太大
            query = f"""
                SELECT DISTINCT {col} FROM {table} 
                WHERE {col} IS NOT NULL 
                LIMIT {sample_size}
            """
            cur.execute(query)
            rows = cur.fetchall()
            # flatten tuple
            vals = [row[0] for row in rows]
            examples[f"{table}.{col}"] = vals

    cur.close()
    conn.close()
    return examples