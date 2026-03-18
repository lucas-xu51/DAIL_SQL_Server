from sqlglot import parse_one, exp
import json

def _get_subquery_context(node: exp.Subquery) -> str:
    """
    通过检查子查询节点的父节点来判断其上下文。
    """
    parent = node.parent
    if isinstance(parent, exp.From):
        return "FROM (Derived Table)"
    elif isinstance(parent, exp.Where):
        return "WHERE"
    elif isinstance(parent, exp.In):
        return "WHERE (IN)"
    elif isinstance(parent, exp.Select):
        return "SELECT (Scalar)"
    elif isinstance(parent, exp.Join):
        return "JOIN"
    # 可以根据需要添加更多上下文检查，例如 HAVING, EXISTS 等
    return "Unknown"

def get_subqueries(sql: str):
    """
    从 SQL 语句中提取所有子查询。
    
    返回一个列表，其中每个元素都是一个字典，包含子查询的别名、SQL内容和上下文。
    """
    subqueries_info = []
    try:
        ast = parse_one(sql)
    except Exception as e:
        print(f"SQL 解析失败: {e}")
        return []

    # 使用 find_all 查找所有 exp.Subquery 类型的节点
    for subquery in ast.find_all(exp.Subquery):
        # subquery.this 指的是子查询内部的 SELECT 语句
        # subquery.alias 是子查询的别名 (例如 AS recent_orders)
        subquery_data = {
            "alias": subquery.alias or None,
            "sql": subquery.this.sql(pretty=True), # 使用 pretty=True 格式化输出
            "context": _get_subquery_context(subquery)
        }
        subqueries_info.append(subquery_data)
        
    return subqueries_info

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
