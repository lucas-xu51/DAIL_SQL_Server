import os
import sqlite3
from typing import Tuple, Any
from sqlparse import sql, tokens as T
import json
from datetime import datetime
import re
# import logging
# 

# 配置日志
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# 全局调试开关
DEBUG_MODE = True

def debug_print(message, data=None, level="INFO"):
    """统一的调试输出函数"""
    if DEBUG_MODE:
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"\n[{timestamp}] [{level}] {message}")
        if data is not None:
            print(f"    Data: {json.dumps(data, indent=2, default=str) if isinstance(data, (dict, list)) else data}")

def find_column(schema:dict, operator:str, condition_value:str, db_connection, allowed_tables=None)-> list:
    """
    在数据库中查找满足条件的列
    参数:
    schema: dict - 数据库架构
    operator: str - 比较操作符(=, >, <, LIKE等)
    condition_value: 任意类型 - 要查找的值
    db_connection - 数据库连接对象
    allowed_tables: list - 允许搜索的表列表（如果为None，搜索所有表）

    返回:
    list - 匹配的列，格式为 [(table_name, column_name), ...]
    """
    debug_print("=== FIND_COLUMN 开始 ===", level="START")
    debug_print("查找参数", {
        "operator": operator,
        "condition_value": condition_value,
        "value_type": type(condition_value).__name__,
        "allowed_tables": allowed_tables
    })
    
    matching_columns = []

    # 从schema中获取表名和列名
    _dict = schema['Cols']
    
    # 如果指定了允许的表，只搜索这些表
    if allowed_tables:
        tables = [t for t in _dict.keys() if t in allowed_tables]
        debug_print("限制搜索范围", f"只搜索表: {tables}")
    else:
        tables = list(_dict.keys())
    
    debug_print("数据库中的表", tables)

    # 检查操作符是否安全
    valid_operators = ["=", ">", "<", ">=", "<=", "<>", "!=", "LIKE", "IN", "NOT IN", "IS", "IS NOT", "BETWEEN"]
    if operator.upper().strip() not in valid_operators:
        debug_print("不安全的操作符", operator, level="ERROR")
        # raise ValueError(f"不安全的操作符: {operator}")

    # 处理BETWEEN操作符的特殊情况
    if operator.upper() == "BETWEEN":
        # BETWEEN需要两个值，condition_value应该是一个包含两个值的字符串，如 "100 AND 500"
        debug_print("处理BETWEEN操作符", f"值: {condition_value}")
        # 简化处理：暂时跳过BETWEEN的完整实现
        formatted_value = condition_value
    elif isinstance(condition_value, str):
        # 对于字符串值，需要正确转义单引号
        # 注意：这里不应该截断包含单引号的值
        escaped_value = condition_value.replace("'", "''")
        formatted_value = f"'{escaped_value}'"
        debug_print(f"格式化后的查找条件: {formatted_value}，操作符: {operator}")
    else:
        formatted_value = condition_value
        debug_print(f"非字符串查找条件: {formatted_value}，操作符: {operator}")

    total_checks = 0
    for table_name in tables:
        columns = _dict[table_name]
        debug_print(f"检查表 {table_name}", f"列数: {len(columns)}")
        
        for column_name in columns:
            total_checks += 1
            # 对SQLite特别处理列名中的空格，使用引号括起来
            safe_column_name = f'"{column_name}"' if ' ' in column_name else column_name
            
            # 根据操作符构建查询
            if operator.upper() == "BETWEEN" and "AND" in str(condition_value).upper():
                # BETWEEN特殊处理
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {safe_column_name} {operator} {condition_value}"
            elif operator.upper() in ["IN", "NOT IN"]:
                # IN/NOT IN 需要特殊处理
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {safe_column_name} {operator} ({formatted_value})"
            else:
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {safe_column_name} {operator} {formatted_value}"
            
            if total_checks % 10 == 0:  # 每10个查询输出一次进度
                debug_print(f"查询进度", f"已检查 {total_checks} 个列")
            
            try:
                cursor = db_connection.cursor()
                cursor.execute(query)
                count = cursor.fetchone()[0]
                
                if count > 0:
                    match_info = {
                        'table': table_name,
                        'column': column_name,
                        'operator': operator,
                        'value': condition_value,
                    }
                    matching_columns.append(match_info)
                    debug_print(f"找到匹配列!", f"{table_name}.{column_name} (匹配数: {count})", level="SUCCESS")
            except Exception as e:
                if DEBUG_MODE and "no such column" not in str(e).lower():
                    debug_print(f"查询异常", f"表: {table_name}, 列: {column_name}, 错误: {str(e)}", level="WARNING")
                continue

    debug_print(f"=== FIND_COLUMN 完成 ===", f"总计检查: {total_checks} 列, 找到: {len(matching_columns)} 个匹配", level="END")
    return matching_columns

replace_index = 0

def correct_sql_using_schema(infer_sql, schema, db_connection, varify):
    """
    使用schema信息修正SQL查询
    """
    global replace_index
    
    debug_print("=== CORRECT_SQL_USING_SCHEMA 开始 ===", level="START")
    debug_print("输入SQL", infer_sql)
    debug_print("Schema信息", f"表数量: {len(schema.get('Cols', {}))}")
    
    # 1. 解析SQL
    formal_sql_dict, error = varify.parse_sql(infer_sql)  # 注意: parse_sql的self参数在这里用None
    
    if error is not None:
        debug_print(f"SQL解析错误: {error}", level="ERROR")
        debug_print("返回原始SQL", level="INFO")
        return infer_sql  # 返回原始SQL
    
    if formal_sql_dict is None:
        debug_print("解析结果为None，返回原始SQL", level="WARNING")
        return infer_sql
    
    debug_print("解析结果", formal_sql_dict)
        
    # 2. 提取过滤条件
    filter_conditions = formal_sql_dict.get('F', {})  # 改为字典默认值
    tables = formal_sql_dict.get('T', set())
    debug_print("提取的表", list(tables))
    
    if not filter_conditions:
        debug_print("没有过滤条件，返回原始SQL", level="WARNING")
        return infer_sql  # 没有过滤条件，返回原始SQL

    # 确定逻辑操作符并提取条件
    logic_op = None
    conditions_list = []

    # 处理复杂的条件结构
    if isinstance(filter_conditions, dict):
        if 'AND' in filter_conditions:
            logic_op = 'AND'
            conditions_list = filter_conditions['AND']
        elif 'OR' in filter_conditions:
            logic_op = 'OR'
            conditions_list = filter_conditions['OR']
        elif 'conditions' in filter_conditions:
            # 简单条件，没有逻辑操作符
            conditions_list = filter_conditions['conditions']
            logic_op = None
        elif 'complex' in filter_conditions:
            # 复杂条件（包含括号等）
            debug_print("检测到复杂条件结构", filter_conditions, level="WARNING")
            # 尝试提取所有条件
            conditions_list = []
            # 递归提取所有条件
            def extract_all_conditions(obj):
                if isinstance(obj, dict):
                    if 'column' in obj and 'value' in obj:
                        conditions_list.append(obj)
                    else:
                        for key, value in obj.items():
                            if isinstance(value, list):
                                for item in value:
                                    extract_all_conditions(item)
                            elif isinstance(value, dict):
                                extract_all_conditions(value)
                elif isinstance(obj, list):
                    for item in obj:
                        extract_all_conditions(item)
            
            extract_all_conditions(filter_conditions)
            debug_print(f"从复杂条件中提取了 {len(conditions_list)} 个条件", conditions_list)
    
    # 确保conditions_list是列表
    if not isinstance(conditions_list, list):
        conditions_list = [conditions_list] if conditions_list else []
        
    debug_print(f"提取的过滤条件", {
        "逻辑操作符": logic_op if logic_op else "无",
        "条件数量": len(conditions_list),
        "条件列表": conditions_list
    })

    if not conditions_list:
        debug_print("没有具体条件，返回原始SQL", level="WARNING")
        return infer_sql  # 如果没有具体条件，返回原始SQL

    # 3. 根据逻辑操作符处理条件
    corrected_sql = infer_sql
    total_replacements = 0
    
    # 按照现有逻辑处理每个条件
    for idx, condition in enumerate(conditions_list):
        debug_print(f"\n处理条件 {idx+1}/{len(conditions_list)}", condition)
        
        value = condition['value']
        operator = condition.get('operator', '=')
        
        # 找到可能匹配的列 - 只在原SQL包含的表中查找
        matching_columns = find_column(schema, operator, value, db_connection, allowed_tables=list(tables))
        if not matching_columns:
            debug_print(f"在允许的表中没有找到匹配的列", condition, level="WARNING")
            continue
            
        debug_print(f"找到 {len(matching_columns)} 个候选列", 
                   [f"{m['table']}.{m['column']}" for m in matching_columns])
        
        # 检查是否与已有条件重复
        flag = 0
        
        for matching_column in matching_columns:
            for filter_condition in conditions_list:
                if matching_column == filter_condition:
                    debug_print(f"跳过已存在的过滤条件", filter_condition)
                    flag = 1
                    break
            if flag == 1:
                break
        
        if flag == 0:
            # 选择最可能的列 - 必须在原SQL的表中
            best_match = None
            best_semantic_match = None
            candidate_info = []
            for i, matching_column in enumerate(matching_columns):
                # 所有匹配的列都应该在允许的表中（因为find_column已经过滤了）
                candidate_info.append(f"{matching_column['table']}.{matching_column['column']}")
                
                # 记录第一个匹配作为默认选择
                if best_match is None:
                    best_match = matching_column
                
                # 对于数值比较操作符，优先选择可能是数值类型的列
                if operator in ['>', '<', '>=', '<='] and best_semantic_match is None:
                    # 检查列名是否暗示数值类型
                    col_name_lower = matching_column['column'].lower()
                    if any(keyword in col_name_lower for keyword in ['age', 'price', 'amount', 'count', 'id', 'number', 'qty', 'quantity', 'year', 'month', 'day']):
                        best_semantic_match = matching_column
                        debug_print(f"找到语义匹配的数值列", matching_column, level="INFO")
            
            debug_print("候选列评估", candidate_info)
            
            # 选择策略：
            # 1. 对于数值比较，优先使用语义匹配的列
            # 2. 否则使用第一个匹配
            if operator in ['>', '<', '>=', '<='] and best_semantic_match:
                best_match = best_semantic_match
                debug_print("使用语义匹配的数值列", best_match, level="INFO")
            
            if not best_match:
                debug_print(f"没有找到任何可用的匹配列", level="WARNING")
                continue
            
            debug_print("选择的最佳匹配", best_match)
            
            # 替换SQL中的列名
            # 构建可能的旧列名格式
            old_patterns = []
            
            # 检查是否使用了别名
            table_or_alias = condition.get('table', '')
            
            # 如果table实际上是别名，需要特殊处理
            is_alias = False
            for real_table, alias in formal_sql_dict.get('alias', []):
                if table_or_alias == alias:
                    is_alias = True
                    debug_print(f"检测到使用了别名 {alias} (实际表名: {real_table})", level="INFO")
                    # 为别名创建替换模式
                    old_patterns.append(f"{alias}.{condition['column']}")
                    break
            
            # 1. table.column 格式
            if condition.get('table') and not is_alias:
                old_patterns.append(f"{condition['table']}.{condition['column']}")
            
            # 2. 仅列名格式
            old_patterns.append(condition['column'])
            
            # 3. 带引号的格式
            if condition.get('table'):
                old_patterns.append(f'"{condition["table"]}"."{condition["column"]}"')
                old_patterns.append(f"`{condition['table']}`.`{condition['column']}`")
            old_patterns.append(f'"{condition["column"]}"')
            old_patterns.append(f'`{condition["column"]}`')
            
            # 构建新列名
            # 检查原条件的表引用
            original_table_ref = condition.get('table', '')
            
            # 如果原查询使用了别名，需要找到对应的别名
            new_table_ref = best_match['table']
            use_alias = False
            
            # 检查best_match的表是否在原查询中有别名
            for real_table, alias in formal_sql_dict.get('alias', []):
                if real_table == best_match['table']:
                    # 如果原条件使用的就是这个表的别名，继续使用别名
                    if original_table_ref == alias:
                        new_table_ref = alias
                        use_alias = True
                        debug_print(f"保持使用别名 {alias} 代替表名 {real_table}", level="INFO")
                        break
                    # 如果原条件的表有别名，但best_match是同一个表，也使用别名
                    elif original_table_ref and real_table == condition.get('table'):
                        new_table_ref = alias
                        use_alias = True
                        debug_print(f"使用相同表的别名 {alias}", level="INFO")
                        break
            
            # 构建正确的列引用
            if new_table_ref and original_table_ref:
                # 有表引用的情况
                new_column = f"{new_table_ref}.{best_match['column']}"
            else:
                # 无表引用的情况
                new_column = best_match['column']
            
            # 如果列名包含空格，使用引号
            if ' ' in best_match['column']:
                if new_table_ref and original_table_ref:
                    new_column = f'{new_table_ref}."{best_match["column"]}"'
                else:
                    new_column = f'"{best_match["column"]}"'
            
            debug_print(f"构建的新列引用", new_column)
            # return new_column
            # 尝试所有可能的替换模式
            replaced = False
            original_corrected_sql = corrected_sql
            
            for old_pattern in old_patterns:
                # 使用正则表达式进行精确替换
                # 确保是完整的列引用，而不是部分字符串
                pattern = r'\b' + re.escape(old_pattern) + r'\b'
                temp_sql = re.sub(pattern, new_column, corrected_sql, flags=re.IGNORECASE)
                
                if temp_sql != corrected_sql:
                    corrected_sql = temp_sql
                    replaced = True
                    debug_print(f"成功替换", f"{old_pattern} -> {new_column}")
                    break
            
            if replaced:
                total_replacements += 1
                debug_print("替换成功，新SQL", corrected_sql, level="SUCCESS")
            else:
                debug_print("替换失败，尝试的所有模式", old_patterns, level="WARNING")
                debug_print("当前SQL", corrected_sql)

    # 4. 特殊处理：如果没有成功替换，可能需要修改查询结构
    if total_replacements == 0 and conditions_list:
        debug_print("\n没有找到可替换的列，保持原SQL不变", level="INFO")
        
        # 记录为什么没有找到替换
        for condition in conditions_list:
            value = condition['value']
            operator = condition.get('operator', '=')
            debug_print(f"条件 {condition['column']} {operator} '{value}' 在允许的表中没有找到匹配", level="INFO")
    
    debug_print(f"\n总计进行了 {total_replacements} 次替换")
    
    # 5. 验证修正后的SQL是否可执行
    try:
        debug_print("\n验证修正后的SQL", corrected_sql)
        cursor = db_connection.cursor()
        cursor.execute(corrected_sql)
        msg = cursor.fetchone()
        
        if msg is not None:
            replace_index += 1
            debug_print(f'验证成功！替换了第 {replace_index} 个SQL', level="SUCCESS")
            debug_print(f"查询结果示例", msg[0] if msg else "无结果")
            return corrected_sql
        else:
            # 查询结果为空，但SQL语法正确
            debug_print("查询结果为空，但SQL语法正确", level="INFO")
            return corrected_sql
            
    except Exception as e:
        debug_print(f"修正后的SQL不可执行", str(e), level="ERROR")
        debug_print(f"失败的SQL", corrected_sql)
        
        # 如果失败了，但进行了修改，可能是替换导致的问题
        if total_replacements > 0:
            debug_print("替换后的SQL执行失败，返回原始SQL", level="WARNING")
            return infer_sql
        
        # 如果没有进行任何替换，返回原始SQL
        return infer_sql
    

def check_sql_executability(generated_sql: str, db: str):
    """检查SQL是否可执行"""
    debug_print("检查SQL可执行性", generated_sql)
    
    if generated_sql.strip() == "":
        return "Error: empty string"
    try:
        # use EXPLAIN QUERY PLAN to avoid actually executing
        success, res = exec_on_db(db, "EXPLAIN QUERY PLAN " + generated_sql)
        if success:
            debug_print("SQL可执行", level="SUCCESS")
            return None, res
        else:
            execution_error = str(res)
            debug_print("SQL执行错误", execution_error, level="ERROR")
            return str(execution_error), None
    except Exception as e:
        debug_print("检查过程异常", str(e), level="ERROR")
        return str(e), None

def get_cursor_from_path(sqlite_path):
    try:
        if not os.path.exists(sqlite_path):
            print("Openning a new connection %s" % sqlite_path)
        connection = sqlite3.connect(sqlite_path, check_same_thread = False)
    except Exception as e:
        print(sqlite_path)
        raise e
    connection.text_factory = lambda b: b.decode(errors="ignore")
    cursor = connection.cursor()
    return cursor

def exec_on_db(sqlite_path: str, query: str) -> Tuple[bool, Any]:
    cursor = get_cursor_from_path(sqlite_path)
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return True, result
    except Exception as e:
        return False, e
    finally:
        cursor.close()
        cursor.connection.close()
