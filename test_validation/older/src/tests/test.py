def parse_sql(sql_string):
    from sql_metadata import Parser
    """将SQL字符串解析为形式化表示: S = (T, C, J, F, G, O, L)"""
    import re
    import sqlparse
    
    def strip_ansi_codes(s):
        return re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', s)
    
    sql_string = strip_ansi_codes(sql_string)

    try:
        parser = Parser(sql_string)
        # 使用sqlparse进行辅助解析
        parsed = sqlparse.parse(sql_string)[0]
    except Exception as e:
        return None, f"SQL parse error: {str(e)}"
    
    try:
        # 提取查询组件
        formal_sql = {
            'T': set(parser.tables),  # 表集合
            'C': set(),  # 列集合
            'J': set(),  # 连接条件
            'F': set(),  # 过滤条件
            'G': set(),  # 分组条件
            'O': set(),  # 排序条件
            'L': None    # 限制条件
        }

        # 提取列
        for column in parser.columns:
            # 如果列名中包含表名（如 table.column 格式）
            if '.' in column:
                table, col = column.split('.')
                formal_sql['C'].add((table, col))
            else:
                # 对于没有表前缀的列，如果只有一个表，则假定它们属于该表
                if len(parser.tables) == 1:
                    table = list(parser.tables)[0]
                    formal_sql['C'].add((table, column))
                else:
                    # 如果有多个表，我们无法确定，保留为None
                    formal_sql['C'].add((None, column))
        
        # 使用正则表达式提取GROUP BY子句
        group_by_match = re.search(r'GROUP\s+BY\s+(.*?)(?:ORDER\s+BY|LIMIT|$)', sql_string, re.IGNORECASE | re.DOTALL)
        if group_by_match:
            group_by_columns = [col.strip() for col in group_by_match.group(1).split(',')]
            for column in group_by_columns:
                if '.' in column:
                    table, col = column.split('.')
                    formal_sql['G'].add((table, col))
                else:
                    if len(parser.tables) == 1:
                        table = list(parser.tables)[0]
                        formal_sql['G'].add((table, column))
                    else:
                        formal_sql['G'].add((None, column))
        
        # 使用正则表达式提取ORDER BY子句
        order_by_match = re.search(r'ORDER\s+BY\s+(.*?)(?:LIMIT|$)', sql_string, re.IGNORECASE | re.DOTALL)
        if order_by_match:
            order_by_clause = order_by_match.group(1).strip()
            order_by_items = [item.strip() for item in order_by_clause.split(',')]
            
            for item in order_by_items:
                # 处理可能带有ASC/DESC的情况
                parts = item.split()
                column = parts[0]
                
                if '.' in column:
                    table, col = column.split('.')
                    # 保存排序方向信息（ASC/DESC）
                    direction = parts[1] if len(parts) > 1 else 'ASC'
                    formal_sql['O'].add((table, col, direction))
                else:
                    if len(parser.tables) == 1:
                        table = list(parser.tables)[0]
                        direction = parts[1] if len(parts) > 1 else 'ASC'
                        formal_sql['O'].add((table, column, direction))
                    else:
                        direction = parts[1] if len(parts) > 1 else 'ASC'
                        formal_sql['O'].add((None, column, direction))
        
        # 使用正则表达式提取LIMIT子句
        limit_match = re.search(r'LIMIT\s+(\d+)(?:\s*,\s*(\d+))?', sql_string, re.IGNORECASE)
        if limit_match:
            if limit_match.group(2):  # LIMIT x, y 格式
                formal_sql['L'] = (int(limit_match.group(1)), int(limit_match.group(2)))
            else:  # LIMIT x 格式
                formal_sql['L'] = int(limit_match.group(1))
        
        # JOIN条件和WHERE条件的提取可能需要更复杂的逻辑
        # 这里只给出一个简单的示例
        
        return formal_sql, None
        
    except Exception as e:
        return None, f"SQL parse error: {str(e)}"

if __name__ == '__main__':
    simple_sql = "SELECT district.A2 FROM client JOIN disp ON client.client_id = disp.client_id JOIN account ON disp.account_id = account.account_id WHERE client.gender = 'F' GROUP BY district.A2 ORDER BY COUNT(*) DESC LIMIT 9;"
    result, error = parse_sql(simple_sql)
    print(result if result else error)