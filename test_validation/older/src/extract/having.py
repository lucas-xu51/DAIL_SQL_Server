from sqlglot import parse_one, exp

OPERATOR_MAP = {
    exp.LT: '<',
    exp.GT: '>',
    exp.EQ: '=',
    exp.LTE: '<=',
    exp.GTE: '>=',
    exp.NEQ: '!=',
    exp.Like: 'LIKE',
    # 你可以根据需要添加更多操作符，例如 IN, BETWEEN 等
}

def get_having_structure(sql: str):
    """
    从 SQL 语句中提取 HAVING 子句，并将其解析为结构化的 Python 字典。
    """
    try:
        ast = parse_one(sql)
    except Exception as e:
        print(f"SQL 解析失败: {e}")
        return None

    # 从 AST 中获取 having 节点
    having_node = ast.args.get('having')

    if having_node:
        # having_node 是一个 Having 对象, 其内容在 .this 属性中
        return parse_condition_recursive(having_node.this)
    else:
        # 没有 HAVING 子句
        return None
    
def parse_expression_to_str(expression: exp.Expression) -> str:
    """
    将 sqlglot 表达式对象转换为可读的字符串。
    例如，将 COUNT(id) 的 AST 节点转换为 "COUNT(id)" 字符串。
    .sql() 方法是实现此功能的最佳方式。
    """
    if expression:
        return expression.sql()
    return ""

def parse_condition_recursive(expression: exp.Expression):
    """
    递归解析条件表达式 (适用于 WHERE 和 HAVING)。
    """
    # 基础情况：处理基本的比较表达式，例如 COUNT(id) > 10
    if isinstance(expression, exp.Binary) and type(expression) in OPERATOR_MAP:
        # 左侧通常是列或聚合函数
        left_expr_str = parse_expression_to_str(expression.left)
        
        # 右侧通常是字面量
        value = None
        if isinstance(expression.right, exp.Literal):
            value = expression.right.this
        else:
            # 右侧也可能是另一个表达式，我们也将其转换为字符串
            value = parse_expression_to_str(expression.right)

        return {
            'expression': left_expr_str,
            'operator': OPERATOR_MAP[type(expression)],
            'value': value,
        }

    # 递归步骤：处理 AND 逻辑
    elif isinstance(expression, exp.And):
        left_condition = parse_condition_recursive(expression.left)
        right_condition = parse_condition_recursive(expression.right)

        conditions = []
        if left_condition and 'AND' in left_condition:
            conditions.extend(left_condition['AND'])
        elif left_condition:
            conditions.append(left_condition)

        if right_condition:
            conditions.append(right_condition)
        
        return {'AND': conditions}

    # 递归步骤：处理 OR 逻辑
    elif isinstance(expression, exp.Or):
        left_condition = parse_condition_recursive(expression.left)
        right_condition = parse_condition_recursive(expression.right)
        
        conditions = []
        if left_condition and 'OR' in left_condition:
            conditions.extend(left_condition['OR'])
        elif left_condition:
            conditions.append(left_condition)
            
        if right_condition:
            conditions.append(right_condition)
            
        return {'OR': conditions}

    # 如果遇到不支持的表达式类型，返回其字符串形式
    return {'unsupported_expression': parse_expression_to_str(expression)}

# sql = "SELECT schools.School  FROM satscores  JOIN schools ON satscores.cds = schools.CDSCode  WHERE schools.County = 'Contra Costa'  ORDER BY satscores.NumTstTakr having COUNT(id) >= 10 AND country != 'USA' "
# print(get_having_structure(sql))