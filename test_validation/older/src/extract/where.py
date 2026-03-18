from sqlglot import parse_one, exp
import json

# 定义一个从 sqlglot 表达式类型到字符串操作符的映射
# 这使得代码更清晰，也更容易扩展
OPERATOR_MAP = {
    exp.LT: '<',
    exp.GT: '>',
    exp.EQ: '=',
    exp.LTE: '<=',
    exp.GTE: '>=',
    exp.NEQ: '!=',
    exp.Like: 'LIKE',
    exp.In: 'IN',
    # 你可以根据需要添加更多操作符，例如 IN, BETWEEN 等
}

def parse_condition(expression, table_name):
    """
    递归解析 WHERE 子句中的表达式。
    """
    # 递归基础：处理基本的比较表达式，例如 a < 10
    # exp.Binary 是所有二元操作符（如 <, >, =）的基类
    if isinstance(expression, exp.Binary) and type(expression) in OPERATOR_MAP:
        # 确保是 "列 操作符 字面量" 的形式
        if isinstance(expression.left, exp.Column) and isinstance(expression.right, exp.Literal):
            return {
                'table': table_name,
                'column': expression.left.name,
                'operator': OPERATOR_MAP[type(expression)],
                'value': expression.right.this,  # .this 获取字面量的实际值
            }
        # 如果是其他形式，例如 column > column，我们暂时不支持
        return None

    # *** 新增：处理 IN 子句 ***
    elif isinstance(expression, exp.In):
        left_node = expression.this
        # 确保 IN 的左边是一个列
        if not isinstance(left_node, exp.Column):
            return None # 暂时不支持更复杂的情况，例如 (a, b) IN (...)

        # 检查 expressions 列表是否为空
        if not expression.expressions:
            return None
        
        # IN 的右边可以是一个子查询，也可以是一个值的列表
        value_type = "unknown"
        value_payload = None
        
        # 处理不同类型的IN表达式
        if len(expression.expressions) == 1:
            expr = expression.expressions[0]
            if isinstance(expr, exp.Subquery):
                value_type = "subquery"
                value_payload = expr.this.sql()
            elif isinstance(expr, exp.Tuple):
                # 处理 IN (val1, val2, val3) 的情况
                value_type = "list"
                value_payload = [lit.this for lit in expr.expressions if isinstance(lit, exp.Literal)]
            elif isinstance(expr, exp.Literal):
                value_type = "list"
                value_payload = [expr.this]
            else:
                # 处理其他可能的表达式类型
                value_type = "unknown"
                value_payload = str(expr)
        else:
            # 多个expressions的情况（这种情况下通常是字面量列表）
            value_type = "list"
            value_payload = []
            for expr in expression.expressions:
                if isinstance(expr, exp.Literal):
                    value_payload.append(expr.this)
                elif isinstance(expr, exp.Subquery):
                    value_type = "subquery"
                    value_payload = expr.this.sql()
                    break

        return {
            'table': table_name,
            'column': left_node.name,
            'operator': 'IN',
            'value_type': value_type,
            'value': value_payload
        }

    # 递归步骤：处理 AND 逻辑
    elif isinstance(expression, exp.And):
        # 递归处理 AND 的左右两边
        left_condition = parse_condition(expression.left, table_name)
        right_condition = parse_condition(expression.right, table_name)

        conditions = []
        # sqlglot 会将 a AND b AND c 解析为 (a AND b) AND c
        # 所以我们需要检查左侧是否也是一个AND，如果是，就将其中的条件列表展开
        if left_condition and 'AND' in left_condition:
            conditions.extend(left_condition['AND'])
        elif left_condition:
            conditions.append(left_condition)

        # 右侧的处理也需要考虑嵌套的AND情况
        if right_condition:
            if 'AND' in right_condition:
                conditions.extend(right_condition['AND'])
            else:
                conditions.append(right_condition)
        
        return {'AND': conditions}

    # 递归步骤：处理 OR 逻辑 (为了完整性，也实现了 OR 的解析)
    elif isinstance(expression, exp.Or):
        left_condition = parse_condition(expression.left, table_name)
        right_condition = parse_condition(expression.right, table_name)
        
        # OR 的逻辑与 AND 类似
        conditions = []
        if left_condition and 'OR' in left_condition:
            conditions.extend(left_condition['OR'])
        elif left_condition:
            conditions.append(left_condition)
            
        if right_condition:
            if 'OR' in right_condition:
                conditions.extend(right_condition['OR'])
            else:
                conditions.append(right_condition)
            
        return {'OR': conditions}

    # 如果遇到不支持的表达式类型，返回 None
    return None

def get_where_structure(sql: str):
    """
    从 SQL 语句中提取 WHERE 子句，并将其解析为结构化的 Python 字典。
    """
    try:
        ast = parse_one(sql)
    except Exception as e:
        print(f"SQL 解析失败: {e}")
        return None

    # 1. 找到表名 (对于简单的单表查询)
    # 对于 JOIN，逻辑会更复杂，需要将列与它们各自的表关联起来
    table_node = ast.find(exp.Table)
    table_name = table_node.name if table_node else None

    # 2. 获取 WHERE 子句的表达式
    where_expr = ast.args.get('where')

    if where_expr:
        # 3. 开始递归解析
        # where_expr 是一个 Where 对象, 其内容在 .this 属性中
        return parse_condition(where_expr.this, table_name)
    else:
        # 没有 WHERE 子句
        return None


if __name__ == "__main__":
    # --- 测试 ---
    sql_with_subqueries = """
    SELECT
        c.customer_name,
        c.country,
        (SELECT MAX(o.order_date) FROM orders AS o WHERE o.customer_id = c.customer_id) AS last_order_date,
        recent_orders.total_amount
    FROM
        customers AS c
    JOIN
        (
            SELECT
                customer_id,
                SUM(amount) AS total_amount
            FROM orders
            WHERE order_date > '2023-01-01'
            GROUP BY customer_id
        ) AS recent_orders
    ON
        c.customer_id = recent_orders.customer_id
    WHERE
        c.country IN (SELECT country_name FROM active_countries)
    ORDER BY
        last_order_date DESC;
    """
    
    # 额外的测试用例
    test_cases = [
        sql_with_subqueries,
        "SELECT * FROM users WHERE id IN (1, 2, 3)",
        "SELECT * FROM users WHERE name IN ('Alice', 'Bob')",
        "SELECT * FROM users WHERE age > 25 AND city IN (SELECT city FROM popular_cities)",
        "SELECT * FROM satscores WHERE County = 'Contra Costa'"  # 添加原始格式的测试用例
    ]
    
    print("测试结果：")
    for i, sql in enumerate(test_cases, 1):
        print(f"\n--- 测试用例 {i} ---")
        result = get_where_structure(sql)
        if result:
            print(json.dumps(result, indent=4, ensure_ascii=False))
        else:
            print("无 WHERE 子句或解析失败")