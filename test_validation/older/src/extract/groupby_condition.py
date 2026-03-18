import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Function, TokenList, Comment # Ensure necessary imports
from sqlparse.tokens import Keyword, DML, Punctuation, Operator, Comment as CommentToken # Ensure necessary imports

# --- Standalone Helper Function to Resolve Identifiers ---
# (Moved outside the class method for broader use by helpers)
def _resolve_identifier(token, alias_map):
    """
    Resolves a sqlparse Identifier or Function token to (table, column/expression).
    Handles aliases. Returns (None, col/expr) if table is not specified or resolved.
    """
    if isinstance(token, Identifier):
        parent_name = token.get_parent_name()
        real_name = token.get_real_name()
        resolved_table = alias_map.get(parent_name, parent_name) if parent_name else None
        return resolved_table, real_name
    elif isinstance(token, Function):
        resolved_table = None
        for param in token.get_parameters():
            if isinstance(param, Identifier):
                parent_name = param.get_parent_name()
                if parent_name:
                    resolved_table = alias_map.get(parent_name, parent_name)
                    break
        return resolved_table, str(token)
    elif isinstance(token, TokenList): # Handle cases like Parenthesis, Function parameters, etc.
        # Attempt to simplify if it's a list containing just one significant item (Identifier or Function)
        # Useful for cases like GROUP BY (col1) or SELECT (t1.col2)
        significant_tokens = []
        # Check if token has 'tokens' attribute (Parenthesis, Function etc. do)
        if hasattr(token, 'tokens'):
            for sub_token in token.tokens:
                 # Ignore whitespace, comments, and the parentheses/commas themselves if inside a list
                 if sub_token.is_whitespace or isinstance(sub_token, Comment) or \
                    sub_token.match(Punctuation, '(') or sub_token.match(Punctuation, ')') or \
                    sub_token.match(Punctuation, ','):
                     continue
                 significant_tokens.append(sub_token)
    
        # If we found exactly one significant token inside, try resolving that token instead
        if len(significant_tokens) == 1:
            # Recursive call to resolve the inner token
            inner_token = significant_tokens[0]
            # Use the correct function name here (_resolve_identifier or _resolve_identifier)
            return _resolve_identifier(inner_token, alias_map)
        else:
            # Otherwise, return the string representation of the whole TokenList
            # This handles complex expressions like (col1 + col2) or lists like function parameters
            return None, str(token)
    else:
        return None, None

# --- Helper for GROUP BY ---
def extract_groupby_conditions(parsed, alias_map, resolved_tables):
    """
    提取 GROUP BY 子句中的条件
    """
    from sqlparse.sql import Identifier, Function, TokenList, Comment
    from sqlparse.tokens import Keyword
    
    groupby_set = set()
    
    # 直接寻找 "GROUP BY" 关键字
    group_by_idx = None
    for i, token in enumerate(parsed.tokens):
        if token.ttype is Keyword and token.value.upper() == 'GROUP BY':
            group_by_idx = i
            break
    
    # 如果找到 GROUP BY 关键字
    if group_by_idx is not None:
        # 获取 GROUP BY 子句之后的内容
        next_idx = group_by_idx + 1
        while next_idx < len(parsed.tokens):
            token = parsed.tokens[next_idx]
            
            # 跳过空白和注释
            if token.is_whitespace or isinstance(token, Comment):
                next_idx += 1
                continue
            
            # 如果找到标识符，这就是我们要的 GROUP BY 列
            if isinstance(token, Identifier):
                # 解析标识符以获取表和列
                parent_name = token.get_parent_name()
                real_name = token.get_real_name()
                
                # 处理表别名
                resolved_table = None
                if parent_name:
                    resolved_table = alias_map.get(parent_name, parent_name)
                
                # 如果标识符没有指定表，但查询只有一个表，则可以确定表
                elif len(resolved_tables) == 1:
                    resolved_table = next(iter(resolved_tables))
                
                # 添加到结果集
                if real_name:
                    groupby_set.add((resolved_table, real_name))
                break
            
            # 如果遇到其他关键字，则 GROUP BY 子句结束
            if token.ttype is Keyword:
                break
            
            next_idx += 1
    
    return groupby_set

# --- Helper for ORDER BY ---
def extract_orderby_conditions(parsed, alias_map, resolved_tables):
    """
    Extracts ORDER BY conditions from a parsed SQL statement.

    Args:
        parsed: The parsed sqlparse Statement object.
        alias_map: Dictionary mapping aliases to real table names.
        resolved_tables: Set of resolved table names involved in the query.

    Returns:
        A set of tuples {(resolved_table, column_or_expression, direction)}.
    """
    orderby_set = set()
    order_by_clause_content = None

    # Find the ORDER BY keyword and the content that follows
    for token in parsed.tokens:
        if token.ttype is Keyword and token.value.upper() == 'ORDER BY':
            idx = parsed.token_index(token) + 1
            while idx < len(parsed.tokens):
                next_token = parsed.tokens[idx]
                if next_token.is_whitespace or isinstance(next_token, Comment):
                    idx += 1
                    continue
                if isinstance(next_token, (IdentifierList, Identifier, Function, TokenList)):
                    order_by_clause_content = next_token
                    print("order_by_clause_content:")
                    print(order_by_clause_content)
                    break
                else:
                    break
            break # Found ORDER BY keyword

    if order_by_clause_content:
        items_to_process = []
        # Get the list of tokens to iterate through, skipping outer list structure if present
        if isinstance(order_by_clause_content, (IdentifierList, TokenList)):
             items_to_process = order_by_clause_content.tokens
        elif isinstance(order_by_clause_content, (Identifier, Function)):
             items_to_process = [order_by_clause_content] # Treat single item as list

        current_identifier = None
        direction = 'ASC' # Default direction

        for item in items_to_process:
            if item.is_whitespace or isinstance(item, Comment) or item.match(Punctuation, ','):
                # If we hit a separator and had an identifier, record it
                if current_identifier:
                    table, col_expr = _resolve_identifier(current_identifier, alias_map)
                    if table is None and len(resolved_tables) == 1:
                        single_table = next(iter(resolved_tables))
                        if not isinstance(single_table, str) or not single_table.endswith("(subquery)"):
                            table = single_table
                    if col_expr:
                        orderby_set.add((table, col_expr, direction))
                    # Reset for next item
                    current_identifier = None
                    direction = 'ASC'
                continue # Move to next token

            # If it's an identifier, function, or list (complex expr), store it
            if isinstance(item, (Identifier, Function, TokenList)):
                # If we already have one stored, previous one finished (default ASC)
                if current_identifier:
                    table, col_expr = _resolve_identifier(current_identifier, alias_map)
                    if table is None and len(resolved_tables) == 1:
                         single_table = next(iter(resolved_tables))
                         if not isinstance(single_table, str) or not single_table.endswith("(subquery)"):
                              table = single_table
                    if col_expr:
                        orderby_set.add((table, col_expr, direction))
                    direction = 'ASC' # Reset direction

                current_identifier = item # Store the new item

            # Check for ASC/DESC keywords
            elif item.ttype is Keyword and item.value.upper() in ('ASC', 'DESC'):
                direction = item.value.upper()
                # ASC/DESC applies to the identifier we just stored. Record it now.
                if current_identifier:
                    table, col_expr = _resolve_identifier(current_identifier, alias_map)
                    if table is None and len(resolved_tables) == 1:
                        single_table = next(iter(resolved_tables))
                        if not isinstance(single_table, str) or not single_table.endswith("(subquery)"):
                             table = single_table
                    if col_expr:
                        orderby_set.add((table, col_expr, direction))
                    # Reset, ASC/DESC consumed this identifier
                    current_identifier = None
                    direction = 'ASC'

        # Add the last item if it wasn't followed by ASC/DESC and explicitly handled
        if current_identifier:
            table, col_expr = _resolve_identifier(current_identifier, alias_map)
            if table is None and len(resolved_tables) == 1:
                 single_table = next(iter(resolved_tables))
                 if not isinstance(single_table, str) or not single_table.endswith("(subquery)"):
                      table = single_table
            if col_expr:
                orderby_set.add((table, col_expr, direction)) # Use last known direction

    return orderby_set