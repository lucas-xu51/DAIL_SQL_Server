from modules_all_ds import FormalSQLValidator, connect_sql, correct_sql_using_schema, re_try, extract_sql
from typing import List, Dict
import json
import sqlite3, os
import re

import pymysql
from func_timeout import func_timeout, FunctionTimedOut
from typing import Dict, List, Tuple, Any

from extract.struct import get_all_function
from check_type.check_round import detect_round_type_issues
from check_type.check_function import check_dialect_function


import psycopg2
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
        return f'Error executing SQL:', "timeout"
    except Exception as e:
        
        return f'Error executing SQL:{str(e)}', f"error: {str(e)}"
    finally:
        if cursor:
            cursor.close()
        if db:
            db.close()
            
def connect_mysql():
    """连接到BIRD数据库，设置超时参数"""
    return pymysql.connect(
        host="localhost",
        port=3306,
        user="root", 
        password="123456",
        database="BIRD",
        charset='utf8mb4',
        autocommit=True,
        connect_timeout=5,
        read_timeout=10,
    )
    
    
"======================================================================"

db_root_path = '/postfix_extend/minidev/MINIDEV/dev_databases'


    

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sqls = json.load(file)
    return sqls

def send_sql():
    with open('/TA-SQL/error_log.txt', 'r') as f:
        lines = f.readlines()
    sqls = read_sql_file('/postfix_extend/dataset/predict_mini_dev_gpt-4-turbo_postgresql.json')
    mode = 'dev'
    validator = FormalSQLValidator(db_root_path, mode)
    import time
    # try:
    if 1:
        start_time = time.time()
        for id in lines:
        # if 1:
            # id ='103'
            # id ='158'
            print("第%s条sql" % id)
            id = str(id.strip())
            sql, db_id = sqls[id].split('\t----- bird -----\t')
            sql = extract_sql(sql)
            sql = sql.replace('\n', ' ')
            sql = sql.replace('\t', ' ')
            if 'give up!' in sql:
                print(f"SQL {id} 已经放弃修正")
                continue
            print("[DEBUG] 原始SQL: ", sql)
            sqls[id] = str(fix_error(id, sql, db_id, validator) + '\t----- bird -----\t' + db_id)
            print(f"修正后的SQL: {sqls[id]}")
    # except Exception as e:
    #     print(f"Error processing SQL: {e}")
    #     pass
    end_time = time.time()
    print(f"处理完成, 耗时: {end_time - start_time}秒")
    with open('/TA-SQL/rq2/postgresql-debugger2.json', 'w') as f:
        json.dump(sqls, f, indent=4)

def edit_join_sql(sql: str, path: str) -> str:
    """
    编辑SQL语句，移除错误JOIN，添加正确的连接条件
    """
    print(f"[DEBUG] 编辑SQL语句，添加缺失的连接条件: {path}")
    
    # 1. 移除所有无效的JOIN条件
    # 这里需要更智能的识别，移除所有包含不存在列的JOIN
    
    # 方法1：移除特定的错误JOIN
    from remove.remove_join import SQLJoinRemover
    remover = SQLJoinRemover()
    
    sql_cleaned = remover.remove_joins_from_sql(sql)
    
    # 插入新的JOIN条件
    where_pos = sql_cleaned.upper().find('WHERE')
    if where_pos != -1:
        before_where = sql_cleaned[:where_pos].strip()
        where_part = sql_cleaned[where_pos:]
        new_sql = f"{before_where} {path} {where_part}"
    else:
        new_sql = f"{sql_cleaned.strip()} {path}"
    
    print(f"[DEBUG] 修正后的SQL: {new_sql}")
    return new_sql

def replace_table_by_alias(sql: str, res_sql: str, tmp_parse, formal_schema) -> str:
    res = res_sql.split('.')
    alias_res = res[0].split('no such column:')[1]
    
    print("[DEBUG] tmp_parse: ", tmp_parse)
    # tmp_parse:tuple
    alias = tmp_parse[0]['alias']
    for table, alias_table in alias:
        print(f"[DEBUG] table: {table}, alias_table: {alias_table}")
        print(alias_res)
        if alias_res.strip() == alias_table.strip():
            print("[DEBUG] 找到别名: ", alias_table)
            alias_res = table
    
    # alias_res 现在就是正确的表名
    # 遍历 formal_schema, 找到 res[1] 对应的表名
    # formal_schema['Cols'] {表: {列1, 列2, ...}}, 找到 res[1] 对应的表名
    
    column_name = res[1] if len(res) > 1 else ""
    found_tables = []
    
    # 遍历所有表的列
    for table_name, columns in formal_schema.get('Cols', {}).items():
        if column_name in columns:
            found_tables.append(table_name)
    
    if found_tables:
        # 如果找到多个表都有这个列，优先返回当前别名对应的表
        if alias_res in found_tables:
            return f"Does '/{column_name}/' exist in table '/{alias_res}/'? Found in tables: '/{found_tables}/'"
        else:
            return f"Column '/{column_name}/'  found in tables: '/{found_tables}/', but table '/{alias_res}/' does not contain this column. Try to replace {found_tables} -> table '/{alias_res}/'"
    else:
        return f"Column '/{column_name}/'  not found in any table in schema"

import re

def fix_sql_keywords(sql):
    """
    修复SQL语句中的关键字冲突，带详细debug信息
    """
    print(f"Debug: 输入SQL = '{sql}'")
    
    # SQL关键字列表
    keywords = {
        'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'BACKUP', 'BETWEEN', 'CASE',
        'CHECK', 'COLUMN', 'CONSTRAINT', 'CREATE', 'DATABASE', 'DEFAULT', 'DELETE',
        'DESC', 'DISTINCT', 'DROP', 'EXEC', 'EXISTS', 'FOREIGN', 'FROM', 'FULL',
        'GROUP', 'HAVING', 'IN', 'INDEX', 'INNER', 'INSERT', 'INTO', 'IS', 'JOIN',
        'KEY', 'LEFT', 'LIKE', 'LIMIT', 'NOT', 'NULL', 'OR', 'ORDER', 'OUTER',
        'PRIMARY', 'PROCEDURE', 'RIGHT', 'ROWNUM', 'SELECT', 'SET', 'TABLE', 'TOP',
        'TRUNCATE', 'UNION', 'UNIQUE', 'UPDATE', 'VALUES', 'VIEW', 'WHERE', 'WITH',
        'USER', 'ROLE', 'SCHEMA', 'FUNCTION', 'TRIGGER', 'CURSOR', 'FETCH', 'OPEN', 
        'CLOSE', 'CAST', 'CONVERT', 'MERGE', 'WHEN', 'THEN', 'MATCH', 'REFERENCES', 
        'ON', 'CASCADE', 'RESTRICT', 'ACTION'
    }
    
    print(f"Debug: ORDER是否在关键字列表中? {'ORDER' in keywords}")
    
    # 使用更精确的模式来匹配不同位置的标识符
    # 1. FROM后面的表名
    from_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    # 2. JOIN后面的表名  
    join_pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    # 3. UPDATE后面的表名
    update_pattern = r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    # 4. INTO后面的表名
    into_pattern = r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    
    result = sql
    replacements_made = []
    
    # 处理各种模式
    patterns = [
        (from_pattern, 'FROM后的表名'),
        (join_pattern, 'JOIN后的表名'),
        (update_pattern, 'UPDATE后的表名'),
        (into_pattern, 'INTO后的表名')
    ]
    
    for pattern, description in patterns:
        matches = list(re.finditer(pattern, result, re.IGNORECASE))
        print(f"Debug: 查找{description}的模式: {pattern}")
        
        # 从后往前替换，避免位置偏移
        for match in reversed(matches):
            table_name = match.group(1)
            start = match.start(1)  # 只获取捕获组的位置
            end = match.end(1)
            
            print(f"Debug: 找到{description}: '{table_name}' (位置: {start}-{end})")
            
            if table_name.upper() in keywords:
                # 检查是否已经被转义
                has_backtick_before = start > 0 and result[start-1] == '`'
                has_backtick_after = end < len(result) and result[end] == '`'
                
                print(f"Debug: '{table_name}' 是关键字，检查转义状态 - 前: {has_backtick_before}, 后: {has_backtick_after}")
                
                if not (has_backtick_before and has_backtick_after):
                    # 进行替换
                    result = result[:start] + f'`{table_name}`' + result[end:]
                    replacements_made.append(f"{table_name} ({description})")
                    print(f"Debug: 替换 '{table_name}' -> '`{table_name}`'")
            else:
                print(f"Debug: '{table_name}' 不是关键字，无需转义")
    
    print(f"Debug: 进行的替换: {replacements_made}")
    print(f"Debug: 最终结果: '{result}'")
    
    return result


def fix_error(id, sql, db_id, validator:FormalSQLValidator) -> str:
    sql = fix_sql_keywords(sql)
    
    sql_for_path = sql
    formal_schema = validator.formalize_schema(db_id)
    tmp_parse = validator.parse_sql(sql_for_path)
    print("[DEBUG] formal_schema: ", formal_schema)
    # res_sql = connect_sql(sql, db_id)
    res_sql, _ = execute_mysql_query_safe(sql)
    
    print("[DEBUG] 原先SQL执行结果: ", res_sql)
    # conn = sqlite3.connect(db_path)
    
    if 'Error executing SQL:' in res_sql:
    # if 0:
        print("SQL验证失败, validator重新生成SQL...")
        sql_feedback = validator.integrate_with_talog(formal_schema, sql=sql, miss = False)
        if sql_feedback is None:
            print("[DEBUG] SQL验证失败, Unsupported SQL or no feedback received.")
            return "give up!"
        
        print(f"sql 第一次验证结果: {sql_feedback.get('errors')}")
        check_errors = sql_feedback.get('errors') 
        print(f"[DEBUG] SQL验证失败, 错误信息: {check_errors}")
        if check_errors is None or check_errors == []:
            check_errors = res_sql
        else:
            check_errors:set[str]
            check_errors.add(res_sql)
        
        # check dialect function
        error = check_dialect_function(sql, dialect='postgres') or sql
        if error is not None and 'Error' in error:
            if isinstance(check_errors, str):
                tmp = []
                tmp.append(check_errors)
                tmp.append(error)
                check_errors = set(tmp)
        error = detect_round_type_issues(sql, source_dialect='postgres')
        if error is not None and len(error) > 0:
            for e in error:
                issue_with_suggestion = f"{e['issue']}. Suggestion: {e['suggestion']}"
                check_errors.add(issue_with_suggestion)
        
        # from compare_with_debug.schema import get_schema_postgresql
        schema = get_schema_postgresql()
        if res_sql != 'Error executing SQL: No result returned.':
            if 'no such column:' in res_sql:
                # 说明有别名问题, 还原别名
                if '.' in res_sql:
                    prompts = replace_table_by_alias(sql, res_sql, tmp_parse, formal_schema)
                    # if isinstance(check_errors, str):
                    #     check_errors += prompts
                    # else:
                    #     check_errors.add(prompts)
            sql = re_try(db_id, formal_schema, sql, check_errors, validator=validator,schema=schema)
            
        if sql is None or 'give up!' in sql:
            sql_feedback = validator.integrate_with_talog(formal_schema, sql=sql, miss = True)
            print("[DEBUG] sql_feedback: ", sql_feedback)
            if sql_feedback is None or  'errors' not in sql_feedback:
                return 'give up!'
            errors = sql_feedback.get('errors') 
            if errors == 'missing_joins':
                print("[DEBUG] SQL验证失败, 可能是缺少连接条件")
            paths = sql_feedback.get('missing_joins')
            paths: list[str]
            if paths is None or len(paths) == 0:
                print("[DEBUG] SQL验证失败, 没有找到缺失的连接条件")
                return 'give up!'
            print(f"[DEBUG] 缺少连接条件: {str(paths)}")

            # 🔥 保存原始SQL
            original_sql = sql_for_path

            if paths is not None and len(paths) > 0:
                # paths 去重
                paths = list(set(paths))
                if len(paths) > 3:
                    paths = paths[:2]
                for path in paths:
                    print(f"[DEBUG] 尝试修正路径: {path}")
                    # 🔥 每次都从原始SQL开始修改
                    # modified_sql = edit_join_sql(original_sql, path)  
                    prompts = f""" The ProgreSQL is missing some join conditions. Please add the missing join conditions to the SQL query. We found the soultion: '/{path}/'. Note about the alia problem.
                    """
                    modified_sql = re_try(db_id, formal_schema, original_sql, prompts, validator=validator,schema=schema, miss=True)
                    print(f"[DEBUG] 修正后的SQL: {modified_sql}")

                    res_sql = execute_mysql_query_safe(modified_sql)
                    if res_sql is not None and 'Error executing SQL:' not in res_sql:
                        print("[DEBUG] 最终选择的SQL: ", modified_sql)
                        sql = modified_sql
                        return sql
                return "give up!"
            return "give up!" 
        
        # print("开始替换sql...")
        # sql = correct_sql_using_schema(sql, formal_schema, conn, validator)
        if sql is None:
            return "give up!" 
        print("修正后的SQL验证通过编译和表验证!")
        return sql
    else:
        # sql = correct_sql_using_schema(sql, formal_schema, conn, validator)
        print(f"SQL验证通过编译和表验证!{sql}")
        return sql


import psycopg2

def get_schema_postgresql() -> str:
    schema_lines = []
    
    # PostgreSQL连接配置
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="user",  
        password="123456",
        database="BIRD",
        connect_timeout=5
    )
    
    try:
        cursor = conn.cursor()
        
        # 获取所有表名（排除系统表）
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)

        tables = [row[0] for row in cursor.fetchall()]
        
        for table_name in tables:
            # PostgreSQL使用双引号转义表名
            escaped_table = f'"{table_name}"'
            schema_lines.append(f"CREATE TABLE {escaped_table} (")
            
            # 获取列信息
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, 
                       column_default, character_maximum_length,
                       numeric_precision, numeric_scale
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = cursor.fetchall()
            
            # 获取主键信息
            cursor.execute("""
                SELECT column_name
                FROM information_schema.key_column_usage k
                JOIN information_schema.table_constraints t
                ON k.constraint_name = t.constraint_name
                WHERE t.table_schema = 'public' 
                AND t.table_name = %s 
                AND t.constraint_type = 'PRIMARY KEY'
                ORDER BY k.ordinal_position
            """, (table_name,))
            
            primary_key_columns = [row[0] for row in cursor.fetchall()]
            
            column_definitions = []
            
            for col in columns:
                col_name = col[0]
                data_type = col[1]
                is_nullable = col[2]  # 'YES' or 'NO'
                column_default = col[3]
                char_max_length = col[4]
                numeric_precision = col[5]
                numeric_scale = col[6]
                
                # 构建列定义 - PostgreSQL关键字列表
                postgresql_keywords = ['order', 'group', 'select', 'from', 'where', 'table', 
                                     'index', 'key', 'primary', 'foreign', 'references', 
                                     'constraint', 'check', 'unique', 'default', 'null',
                                     'database', 'schema', 'user', 'password', 'grant',
                                     'revoke', 'create', 'drop', 'alter', 'insert', 
                                     'update', 'delete', 'replace', 'into', 'values',
                                     'limit', 'offset', 'fetch', 'window', 'over']
                
                escaped_col_name = f'"{col_name}"' if col_name.lower() in postgresql_keywords else col_name
                
                # 处理PostgreSQL数据类型
                if data_type == 'character varying' and char_max_length:
                    col_type = f"VARCHAR({char_max_length})"
                elif data_type == 'character' and char_max_length:
                    col_type = f"CHAR({char_max_length})"
                elif data_type == 'numeric' and numeric_precision:
                    if numeric_scale and numeric_scale > 0:
                        col_type = f"NUMERIC({numeric_precision},{numeric_scale})"
                    else:
                        col_type = f"NUMERIC({numeric_precision})"
                elif data_type == 'timestamp without time zone':
                    col_type = "TIMESTAMP"
                elif data_type == 'timestamp with time zone':
                    col_type = "TIMESTAMPTZ"
                else:
                    col_type = data_type.upper()
                
                col_def = f"  {escaped_col_name} {col_type}"
                
                # 处理NOT NULL
                if is_nullable == 'NO' and col_name not in primary_key_columns:
                    col_def += " NOT NULL"
                
                # 处理DEFAULT值
                if column_default is not None and col_name not in primary_key_columns:
                    # PostgreSQL的默认值处理
                    if column_default.startswith('nextval('):
                        # 序列默认值，通常是SERIAL类型
                        col_def += " DEFAULT " + column_default
                    elif "'" in column_default:
                        # 字符串默认值
                        col_def += f" DEFAULT {column_default}"
                    else:
                        # 数字或其他类型默认值
                        col_def += f" DEFAULT {column_default}"
                
                column_definitions.append(col_def)
            
            # 添加列定义
            if column_definitions:
                schema_lines.extend([col + "," for col in column_definitions[:-1]])
                
                # 最后一列的处理取决于是否有主键约束要添加
                if len(primary_key_columns) > 1:
                    schema_lines.append(column_definitions[-1] + ",")
                    escaped_pk_cols = [f'"{col}"' if col.lower() in postgresql_keywords else col 
                                     for col in primary_key_columns]
                    schema_lines.append(f"  PRIMARY KEY ({', '.join(escaped_pk_cols)})")
                else:
                    schema_lines.append(column_definitions[-1])
            
            schema_lines.append(");")
            schema_lines.append("")  # 空行分隔
    
    finally:
        conn.close()
    
    return "\n".join(schema_lines).strip()


if __name__ == '__main__':
    send_sql()



