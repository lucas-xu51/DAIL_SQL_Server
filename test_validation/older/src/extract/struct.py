from sqlglot import parse_one, exp
import sqlglot

def get_ast(sql, dialect='mysql') -> exp.Select:
    try:
        ast = parse_one(sql, dialect=dialect)
        
        # 确保返回的是Select类型
        if isinstance(ast, exp.Select):
            return ast
        else:
            print(f"Warning: Expected SELECT statement, got {type(ast).__name__}")
            return None
            
    except Exception as e:
        return None

def print_ast(ast: exp.Expression):
    print(repr(ast))

def get_projections(ast: exp.Select) :
    """Find all projections in a select statement."""
    projections = []
    projection: exp.Expression
    for projection in ast.expressions:
        projections.append(projection.alias_or_name)
    return projections

def get_group_by_columns(sql, dialect='mysql'):
    """Extract all column references within the GROUP BY clause."""
    group_by_cols = []
    ast = get_ast(sql, dialect=dialect)
    if hasattr(ast, 'args') and 'group' in ast.args and ast.args['group'] is not None:
        expr: exp.Expression
        for expr in ast.args['group'].expressions:
            # 使用 find_all 来处理表达式，如 GROUP BY YEAR(date)
            for col in expr.find_all(exp.Column):
                group_by_cols.append(col.alias_or_name)
    return list(set(group_by_cols)) # 使用 set 去重

def get_order_by_columns(sql):
    """Extract all column references within the ORDER BY clause."""
    order_by_cols = []
    ast = get_ast(sql)
    if hasattr(ast, 'args') and 'order' in ast.args and ast.args['order'] is not None:
        ordered_expr: exp.Expression
        for ordered_expr in ast.args['order'].expressions:
            column_expr = ordered_expr.this
            # 使用 find_all 来处理表达式，如 ORDER BY a + b
            for col in column_expr.find_all(exp.Column):
                order_by_cols.append(col.alias_or_name)
    return list(set(order_by_cols)) # 使用 set 去重

def get_having_columns(ast: exp.Select):
    """Extract all column references within the HAVING clause."""
    having_cols = set()
    if hasattr(ast, 'args') and 'having' in ast.args and ast.args['having'] is not None:
        expr: exp.Expression
        for expr in ast.args['having'].find_all(exp.Column):
            having_cols.add(expr.alias_or_name)
    return having_cols

def has_aggregation_in_having(ast: exp.Select) -> bool:
    """检查HAVING中是否有聚合函数"""
    if 'having' in ast.args and ast.args['having'] is not None:
        for func in ast.args['having'].find_all(exp.Func):
            if func.is_aggregate:
                return True
    return False

def has_aggregation_functions(ast: exp.Select) -> bool:
    """检查SELECT中是否有聚合函数"""
    agg_functions = {"COUNT", "SUM", "AVG", "MAX", "MIN", "GROUP_CONCAT"}
    
    for expr in ast.expressions:
        for func in expr.find_all(exp.Anonymous):
            if func.this.upper() in agg_functions:
                return True
    return False


# {('schools', 'County'), ('satscores', 'cds'), ('schools', 'School'), ('schools', 'CDSCode'), ('satscores', 'NumTstTakr')}
def get_all_columns(sql, dialect='mysql'):
    """
    从 SQL 语句中提取所有列的引用，并以 (表, 列) 的元组形式返回。

    Args:
        sql: The SQL query string.

    Returns:
        A set of tuples, where each tuple is (table_name, column_name).
    """
    columns_with_tables = set()
    try:
        # 使用 parse_one 解析 SQL
        ast = parse_one(sql, dialect=dialect)

        # ast.find_all(exp.Column) 会找到 SQL 中所有使用到列的地方
        # (SELECT, WHERE, JOIN ON, GROUP BY, ORDER BY, etc.)
        for col in ast.find_all(exp.Column):
            # col.table 属性是表名或别名
            # col.name 属性是列名
            # 我们只添加那些明确指定了表的列，以匹配你的输出要求
            if col.table:
                columns_with_tables.add((col.table, col.name))
            # 注意：如果 SQL 中有不带表前缀的列（例如 SELECT id FROM table），
            # col.table 会是 None。根据需求，你可以选择如何处理这种情况。
            # 在这里，我们忽略它们，因为你的期望输出中都带有表名。

    except Exception as e:
        print(f"解析 SQL 时出错: {e}")
        return set() # 出错时返回一个空集合

    return columns_with_tables


def get_join_tables(sql, dialect='mysql'):
    """Extract all tables involved in JOIN clauses."""
    join_tables = set()
    ast = get_ast(sql, dialect)
    for join in ast.find_all(exp.Join):
        table = join.this
        if isinstance(table, exp.Table):
            join_tables.add(table.name)
    return list(join_tables)


def get_all_function(sql: str, dialect='mysql') -> set:
    """
    使用 sqlglot 从一个SQL查询中解析并提取所有用到的函数名称。
    
    这个版本能够正确处理像 MAX, MIN 等被解析为特殊类的函数。
    """
    ast = sqlglot.parse_one(sql, dialect=dialect)
    function_nodes = ast.find_all(exp.Func)
    
    function_names = {type(f).__name__.upper() for f in function_nodes}
    
    return function_names


get_all_function("""SELECT COUNT(member.member_id) AS medium_tshirt_count FROM member JOIN attendance ON member.member_id = attendance.link_to_member JOIN event ON attendance.link_to_event = event.event_id WHERE event.event_name = 'Women''s Soccer' AND member.t_shirt_size = 'Medium'""")