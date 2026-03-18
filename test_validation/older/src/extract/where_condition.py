import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Comparison, Where, Token
from sqlparse.tokens import Keyword, DML, Punctuation, Operator, Literal, Name # Added Name for backticks

# --- extract_identifier_parts function remains the same ---
def extract_identifier_parts(identifier:Identifier):
    """
    Extracts table and column name from an Identifier token.
    Handles names with spaces/quotes/backticks if parsed correctly.
    """
    if not isinstance(identifier, Identifier):
        # Handle cases where it might be parsed differently, e.g., backticked name
        if identifier.ttype == Name.Quoted:
             # If it's directly a quoted name, assume it's the column
             return None, identifier.value.strip('`') # Strip backticks specifically
        return None, identifier.value # Return raw value if not an identifier

    # Check if the identifier itself represents a multipart name (table.column)
    table_name = identifier.get_parent_name()
    column_name = identifier.get_real_name() # Strips common quotes (`"`)
    
    # Special handling for cases where get_real_name() returns empty
    if not column_name:
        # Check the raw string representation for patterns like "table.'column'"
        raw_str = str(identifier)
        
        # Try to extract the column part after the last dot with quotes
        if "." in raw_str and ("'" in raw_str or '"' in raw_str):
            parts = raw_str.split(".")
            # The column part would be the last segment after splitting by dot
            last_part = parts[-1]
            
            # If it's wrapped in quotes, clean it
            if (last_part.startswith("'") and last_part.endswith("'")) or \
               (last_part.startswith('"') and last_part.endswith('"')):
                column_name = last_part[1:-1]  # Remove quotes
            else:
                column_name = last_part
                
            # If we have found the column this way, ensure table_name doesn't include the column part
            if len(parts) > 1:
                # Join all parts except the last one to get the table name
                table_name = ".".join(parts[:-1])
                
                # Clean up potential quotes in table_name
                if table_name.startswith(("'", '"')) and table_name.endswith(("'", '"')):
                    table_name = table_name[1:-1]

    return table_name, column_name


# --- clean_value function remains the same ---
def clean_value(token):
    """Cleans the value token, removing surrounding quotes."""
    value = token.value
    # Check standard string literals
    if token.ttype == Literal.String.Single:
        return value.strip("'")
    elif token.ttype == Literal.String.Double:
         return value.strip('"')
    # Handle backticked strings if used as values (uncommon but possible)
    elif token.ttype == Name.Quoted:
         return value.strip('`')
    # Check numbers
    elif token.ttype in (Literal.Number.Integer, Literal.Number.Float):
        return value
    # Sometimes dates/strings are parsed as Identifiers if quotes are missing or non-standard
    elif isinstance(token, Identifier):
        if value.startswith("'") and value.endswith("'"):
            return value.strip("'")
        elif value.startswith('"') and value.endswith('"'):
            return value.strip('"')
        elif value.startswith('`') and value.endswith('`'):
            return value.strip('`')
    # Add handling for other types if needed (e.g., boolean keywords)
    return value # Return as is otherwise

# --- MODIFIED find_comparisons_recursive function ---
def find_comparisons_recursive(from_table, token_list) -> dict:
    found_conditions = []   # 是list, 每个元素是一个字典
    conditions = {}         # 是字典, key是AND/OR, value是list
    
    # 用于检测逻辑操作符
    current_logic_op = None
    
    for token in token_list:
        # 检查是否为逻辑操作符(AND/OR)
        if token.value.upper() in ('AND', 'OR'):
            current_logic_op = token.value.upper()
            # 初始化该逻辑操作符下的条件列表
            if current_logic_op not in conditions:
                conditions[current_logic_op] = []
        
        elif isinstance(token, Comparison):
            # Basic structure: left operator right
            left = token.left
            right = token.right
            operator_token = None # Initialize

            # Find the operator token WITHIN the comparison's tokens
            for sub_token in token.tokens:
                # Check if the token type is a comparison operator
                # Operator.Comparison is a 'parent' type for >, <, =, etc.
                if sub_token.ttype in Operator.Comparison:
                    operator_token = sub_token
                    break # Assume first comparison operator is the main one

            # Ensure all parts were identified
            if left and operator_token and right:
                table, column = extract_identifier_parts(left)
                if table is None:
                    table = from_table
                operator = operator_token.value # Get the actual operator string
                value = clean_value(right)

                # Basic validation (optional but good)
                if column and operator and value is not None:
                    condition = {
                        'table': table,
                        'column': column,
                        'operator': operator,
                        'value': value
                    }
                    
                    # 添加到当前逻辑操作符的列表中，如果有
                    if current_logic_op:
                        conditions[current_logic_op].append(condition)
                    # 否则添加到found_conditions
                    else:
                        found_conditions.append(condition)

        # Recursively check subgroups, but avoid re-processing the internals of a Comparison itself
        elif hasattr(token, 'tokens') and not isinstance(token, Comparison):
            sub_conditions = find_comparisons_recursive(from_table, token.tokens)
            
            # 合并递归结果
            for key, value in sub_conditions.items():
                if key in conditions:
                    conditions[key].extend(value)
                else:
                    conditions[key] = value
    
    # 如果条件少于2个且没有逻辑操作符，直接返回found_conditions
    if len(found_conditions) <= 2 and not conditions:
        return {'conditions': found_conditions}
    
    # 否则，将found_conditions添加到conditions中的一个默认键
    if found_conditions:
        if not conditions:  # 如果没有逻辑操作符，默认使用AND
            conditions['AND'] = found_conditions
        else:  # 否则，添加到第一个发现的逻辑操作符
            first_key = list(conditions.keys())[0]
            conditions[first_key] = found_conditions + conditions[first_key]
    
    return conditions


def extract_where_conditions(sql):
    """
    Extracts WHERE clause conditions from a SQL query using sqlparse.
    Handles table prefixes and complex column names more reliably.
    """
    parsed = sqlparse.parse(sql)
    if not parsed:
        return []

    stmt = parsed[0] # Assuming a single statement
    conditions = {}

    # Find the WHERE clause
    where_clause = None
    from_seen = False # Track if we are past the FROM clause
    for token in stmt.tokens:
         # Simple check for FROM token to avoid matching keywords in SELECT list etc.
        if token.is_keyword and token.normalized == 'FROM':
            from_seen = True
        # Find the WHERE clause token *after* FROM
        if from_seen and isinstance(token, Where):
            where_clause = token
            break
        # Handle cases where WHERE might be nested inside JOINs (less common for top-level filtering)
        # More complex logic might be needed for deeply nested structures if required.

    # 找到sql中 from 的table名字
    # 修复:where中不指定表名的情况
    # 例如: select * from a where b = 1
    table = None
    for i, token in enumerate(stmt.tokens):
        # 找到 FROM 关键字
        if token.is_keyword and token.normalized == 'FROM':
            # 查看下一个非空白的 token，应该是表名
            for next_token in stmt.tokens[i+1:]:
                if next_token.ttype is None:  # 非空白的标识符
                    if isinstance(next_token, Identifier):
                        # 单个表
                        table = next_token.get_real_name()
                        break
                    elif isinstance(next_token, IdentifierList):
                        # 多个表的情况，我们取第一个
                        identifiers = list(next_token.get_identifiers())
                        if identifiers:
                            table = identifiers[0].get_real_name()
                        break
                elif not next_token.is_whitespace:
                    # 如果下一个非空白标记不是标识符，则可能是子查询或其他结构
                    break
            break

    if not where_clause:
        # Check if conditions are maybe in JOIN ON clauses (if specifically needed)
        # For now, assume we only want WHERE conditions
        return [] # No WHERE clause found

    # Process tokens within the WHERE clause using the recursive function
    conditions = find_comparisons_recursive(table, where_clause.tokens)

    return conditions
