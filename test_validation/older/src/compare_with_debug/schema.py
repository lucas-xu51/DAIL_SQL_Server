import sqlite3
from typing import Dict, List

def get_schema(db: str) -> str:
    """
    提取SQLite数据库的简化schema，专用于text-to-sql任务
    修复版本：正确处理保留关键字作为表名的情况
    
    Args:
        db (str): SQLite数据库文件路径
        
    Returns:
        str: 格式化的schema字符串，适合作为LLM prompt
    """
    
    schema_lines = []
    
    try:
    # if 1:
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            # 获取所有表名（排除系统表）
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            for table_name in tables:
                # 使用双引号转义表名
                escaped_table = f'"{table_name}"'
                schema_lines.append(f"TABLE {escaped_table} (")
                
                # 修复：转义表名
                cursor.execute(f"PRAGMA table_info({escaped_table})")
                columns = cursor.fetchall()
                column_definitions = []
                primary_keys = []
                
                for col in columns:
                    col_name = col[1]
                    is_pk = col[5]
                    
                    # 构建列定义 - 列名也可能需要转义
                    escaped_col_name = f'"{col_name}"' if col_name in ['order', 'group', 'select', 'from', 'where'] else col_name
                    col_def = f"  {escaped_col_name}"
                    column_definitions.append(col_def)
                    if is_pk:
                        primary_keys.append(escaped_col_name)
                
                # 添加列定义
                schema_lines.extend([col + "," for col in column_definitions[:-1]])
                schema_lines.append(column_definitions[-1])
                
                # 添加主键约束（如果有多个主键列）
                if len(primary_keys) > 1:
                    schema_lines.append(f"  PRIMARY KEY ({', '.join(primary_keys)})")
                elif len(primary_keys) == 1:
                    # 单个主键已在列定义中标注，这里可以选择性添加
                    pass
                
                schema_lines.append(");")
                schema_lines.append("")  # 空行分隔
        
        return "\n".join(schema_lines).strip()
    
    except sqlite3.Error as e:
        return f"Database Error: {e}"

def get_schema_simple(db: str) -> str:
    """
    获取更简洁的表格式schema，适合token限制严格的场景
    修复版本：正确处理保留关键字作为表名的情况
    
    Returns:
        str: 表格式的schema字符串
    """
    
    schema_lines = []
    
    try:
    # if 1:
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            
            for table_name in tables:
                # 使用双引号转义表名
                escaped_table = f'"{table_name}"'
                schema_lines.append(f"Table: {table_name}")
                
                # 修复：转义表名
                cursor.execute(f"PRAGMA table_info({escaped_table})")
                columns = cursor.fetchall()
                
                for col in columns:
                    col_name = col[1]
                    col_type = col[2]
                    is_pk = col[5]
                    
                    col_name = f'"{col_name}"' if col_name in ['order', 'group', 'select', 'from', 'where'] else col_name
                    pk_indicator = " (Primary Key)" if is_pk else ""
                    schema_lines.append(f"- {col_name}: {col_type}{pk_indicator}")
                
                schema_lines.append("")  # 空行分隔
        
        return "\n".join(schema_lines).strip()
    
    except sqlite3.Error as e:
        raise sqlite3.Error(f"数据库操作错误: {e}")

def get_schema_with_sample_data(db: str, sample_rows: int = 3) -> str:
    """
    获取带示例数据的schema，帮助LLM更好理解数据内容
    修复版本：正确处理保留关键字作为表名的情况
    
    Args:
        db (str): SQLite数据库文件路径
        sample_rows (int): 每个表显示的示例行数
        
    Returns:
        str: 包含示例数据的schema字符串
    """
    
    schema_lines = []
    
    try:
    # if 1:
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            print(f"[DEBUG] 找到的表: {tables}")  # 调试信息
            
            for table_name in tables:
                # 使用双引号转义表名
                escaped_table = f'"{table_name}"'
                schema_lines.append(f"Table: {table_name}")
                
                # 修复：转义表名
                print(f"[DEBUG] 执行 PRAGMA: PRAGMA table_info({escaped_table})")
                cursor.execute(f"PRAGMA table_info({escaped_table})")
                columns = cursor.fetchall()
                
                column_names = []
                for col in columns:
                    col_name = col[1]
                    col_type = col[2]
                    is_pk = col[5]
                    
                    pk_indicator = " (PK)" if is_pk else ""
                    schema_lines.append(f"- {col_name}: {col_type}{pk_indicator}")
                    column_names.append(col_name)
                
                # 修复：转义表名
                print(f"[DEBUG] 执行计数: SELECT COUNT(*) FROM {escaped_table}")
                cursor.execute(f"SELECT COUNT(*) FROM {escaped_table}")
                row_count = cursor.fetchone()[0]
                
                if row_count > 0:
                    print(f"[DEBUG] 执行查询: SELECT * FROM {escaped_table} LIMIT {sample_rows}")
                    cursor.execute(f"SELECT * FROM {escaped_table} LIMIT {sample_rows}")
                    sample_data = cursor.fetchall()
                    
                    schema_lines.append(f"Sample data ({row_count} total rows):")
                    for i, row in enumerate(sample_data, 1):
                        row_str = ", ".join([str(val) if val is not None else "NULL" for val in row])
                        schema_lines.append(f"  Row {i}: ({row_str})")
                else:
                    schema_lines.append("Sample data: (empty table)")
                
                schema_lines.append("")  # 空行分隔
        
        return "\n".join(schema_lines).strip()
    
    except sqlite3.Error as e:
        print(f"[DEBUG] SQLite错误: {e}")  # 调试信息
        raise sqlite3.Error(f"数据库操作错误: {e}")

# 辅助函数：检查是否为保留关键字
def is_sql_keyword(name: str) -> bool:
    """检查是否为SQL保留关键字"""
    keywords = {
        'order', 'group', 'select', 'from', 'where', 'having', 'union',
        'insert', 'update', 'delete', 'create', 'drop', 'alter', 'table',
        'index', 'view', 'trigger', 'database', 'schema', 'primary', 'foreign',
        'key', 'constraint', 'unique', 'not', 'null', 'default', 'check',
        'references', 'on', 'cascade', 'restrict', 'set', 'action'
    }
    return name.lower() in keywords

def escape_identifier(name: str) -> str:
    """转义SQL标识符"""
    if is_sql_keyword(name) or ' ' in name or '-' in name:
        return f'"{name}"'
    return name