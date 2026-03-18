import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Function, TokenList, Comment
from sqlparse.tokens import Keyword, DML, Punctuation, Comment as CommentToken

def extract_aliases(parsed):
    """
    从sqlparse解析结果中提取表别名。
    仅查找 FROM 和 JOIN 子句中的表别名。
    """
    aliases = set()
    from_seen = False
    join_seen = False 

    # 遍历语句的顶层标记
    i = 0
    while i < len(parsed.tokens):
        token = parsed.tokens[i]

        # 跳过空白和注释
        if token.is_whitespace or isinstance(token, Comment):
            i += 1
            continue

        # 检测主要子句以跟踪上下文
        token_value_upper = token.value.upper()

        # 检测 FROM 关键字
        if token.ttype is Keyword and token_value_upper == 'FROM':
            from_seen = True
            i += 1
            continue

        # 检测任何 JOIN 关键字
        if token.ttype is Keyword and 'JOIN' in token_value_upper:
            join_seen = True
            i += 1
            continue

        # 当遇到其他子句关键字时停止搜索表/别名
        if token.ttype is Keyword and token_value_upper in ('WHERE', 'GROUP', 'ORDER', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT'):
            from_seen = False
            join_seen = False
            i += 1
            continue

        # 如果我们在 FROM 或 JOIN 之后，寻找标识符（潜在的表）
        if from_seen or join_seen:
            # 检查当前标记是否是表标识符
            if isinstance(token, Identifier):
                # 直接使用 Identifier 类的 has_alias 和 get_alias 方法
                if token.has_alias():
                    real_name = token.get_real_name()
                    alias = token.get_alias()
                    aliases.add((real_name, alias))

            # 处理后重置标志
            if token.ttype is Keyword and token_value_upper == 'ON':
                # 如果是 ON 关键字，保持 from_seen 和 join_seen 不变
                pass
            else:
                # 否则，重置标志
                from_seen = False
                join_seen = False

        i += 1  # 移动到下一个顶层标记

    return aliases

if __name__ == "__main__":

    # --- Test ---
    sql_ok = "SELECT e.name FROM employees e JOIN departments d ON e.dept_id = d.dept_id"
    sql_cast_issue = 'SELECT `Free Meal Count (K-12)` / `Enrollment (K-12)` FROM frpm WHERE `County Name` = \'Alameda\' ORDER BY (CAST(`Free Meal Count     (K-12)` AS REAL) / `Enrollment (K-12)`) DESC LIMIT 1'
    sql_subquery = "SELECT name FROM (SELECT id, name FROM users WHERE active = 1) AS active_users WHERE id > 10"
    sql_multi_alias = "FROM table1 t1 JOIN table2 AS t2 ON t1.id = t2.id LEFT JOIN table3 t3 ON t2.ref = t3.id"

    parsed_ok = sqlparse.parse(sql_ok)[0]
    parsed_cast = sqlparse.parse(sql_cast_issue)[0]
    parsed_subquery = sqlparse.parse(sql_subquery)[0]
    parsed_multi = sqlparse.parse(sql_multi_alias)[0]


    print(f"SQL: {sql_ok}")
    print(f"Aliases: {extract_aliases(parsed_ok)}") # Expected: {('employees', 'e'), ('departments', 'd')}

    print(f"\nSQL: {sql_cast_issue}")
    print(f"Aliases: {extract_aliases(parsed_cast)}") # Expected: set()

    print(f"\nSQL: {sql_subquery}")
    print(f"Aliases: {extract_aliases(parsed_subquery)}") # Expected: set() - Subquery alias 'active_users' isn't a TABLE alias

    print(f"\nSQL: {sql_multi_alias}")
    print(f"Aliases: {extract_aliases(parsed_multi)}") # Expected: {('table1', 't1'), ('table2', 't2'), ('table3', 't3')}