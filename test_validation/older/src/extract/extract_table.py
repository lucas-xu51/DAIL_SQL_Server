def extract_tables(parsed):
    """直接使用 SQLParse 的 get_tables 方法提取表名"""
    # 检查 parsed 是否具有 get_tables 方法
    if hasattr(parsed, 'get_tables'):
        return set(parsed.get_tables())
    
    # 如果没有，尝试导入并使用 extract_tables 函数
    try:
        from sqlparse.sql import extract_tables
        return set(extract_tables(parsed))
    except ImportError:
        # 如果都不可用，使用上面定义的手动提取方法
        return None  # 您需要定义此函数