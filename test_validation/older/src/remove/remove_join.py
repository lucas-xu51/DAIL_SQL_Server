import sqlparse
from sqlparse.sql import Statement, Token, TokenList
from sqlparse.tokens import Keyword, Name, Punctuation
import re
import sys
from typing import List, Optional

class SQLJoinRemover:

    
    def __init__(self):
        self.join_keywords = {
            'JOIN', 'INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN',
            'RIGHT JOIN', 'RIGHT OUTER JOIN', 'FULL JOIN', 
            'FULL OUTER JOIN', 'CROSS JOIN', 'NATURAL JOIN'
        }
    
    def remove_joins_from_sql(self, sql_text: str) -> str:

        try:
            # 解析SQL语句
            parsed = sqlparse.parse(sql_text)
            
            if not parsed:
                return sql_text
            
            # 处理每个语句
            result_statements = []
            for statement in parsed:
                cleaned_statement = self._process_statement(statement)
                result_statements.append(str(cleaned_statement))
            
            return '\n'.join(result_statements)
            
        except Exception as e:
            return sql_text
    
    def _process_statement(self, statement: Statement) -> Statement:
        new_tokens = []
        skip_until_next_clause = False
        i = 0
        
        while i < len(statement.tokens):
            token = statement.tokens[i]
            
            if skip_until_next_clause:
                if self._is_sql_clause_start(token):
                    skip_until_next_clause = False
                    new_tokens.append(token)
                else:
                    pass
            else:
                if self._is_join_token(token):
                    skip_until_next_clause = True
                else:
                    new_tokens.append(token)
            
            i += 1
        
        new_statement = Statement(new_tokens)
        return new_statement
    
    def _is_join_token(self, token: Token) -> bool:
        if token.ttype is Keyword:
            token_value = str(token).upper().strip()
            
            if token_value in ['JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL']:
                return True
            
            for join_keyword in self.join_keywords:
                if token_value.startswith(join_keyword.split()[0]):
                    return True
        
        if isinstance(token, TokenList):
            token_str = str(token).upper().strip()
            for join_keyword in self.join_keywords:
                if join_keyword in token_str:
                    return True
        
        return False
    
    def _is_sql_clause_start(self, token: Token) -> bool:
        if token.ttype is Keyword:
            clause_keywords = {
                'SELECT', 'FROM', 'WHERE', 'GROUP', 'HAVING', 
                'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 
                'EXCEPT', 'WITH'
            }
            token_value = str(token).upper().strip()
            return token_value in clause_keywords
        
        return False
    
    def process_file(self, input_file: str, output_file: str) -> None:

        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            cleaned_sql = self.remove_joins_from_sql(sql_content)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(cleaned_sql)
            
            print(f"处理完成: {input_file} -> {output_file}")
            
        except FileNotFoundError:
            print(f"文件未找到: {input_file}", file=sys.stderr)
        except Exception as e:
            print(f"文件处理错误: {e}", file=sys.stderr)


class RegexJoinRemover:

    
    def __init__(self):
        # 构建JOIN匹配的正则表达式
        self.join_pattern = re.compile(
            r'\b(?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|'
            r'FULL\s+(?:OUTER\s+)?|CROSS\s+|NATURAL\s+)?JOIN\b'
            r'.*?(?=\bWHERE\b|\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|'
            r'\bLIMIT\b|\bUNION\b|\bINTERSECT\b|\bEXCEPT\b|$)',
            re.IGNORECASE | re.DOTALL
        )
    
    def remove_joins_simple(self, sql_text: str) -> str:
        return self.join_pattern.sub('', sql_text).strip()


def main():
    if len(sys.argv) < 2:
        print("使用方法:")
        print("python sql_join_remover.py <SQL语句>")
        print("或者:")
        print("python sql_join_remover.py -f <输入文件> <输出文件>")
        return
    
    remover = SQLJoinRemover()
    
    if sys.argv[1] == '-f' and len(sys.argv) >= 4:
        # 文件处理模式
        input_file = sys.argv[2]
        output_file = sys.argv[3]
        remover.process_file(input_file, output_file)
    else:
        # 直接SQL处理模式
        sql_input = ' '.join(sys.argv[1:])
        result = remover.remove_joins_from_sql(sql_input)
        print("原始SQL:")
        print(sql_input)
        print("\n移除JOIN后:")
        print(result)


# 示例用法和测试用例
if __name__ == "__main__":
    # 如果作为脚本运行
    if len(sys.argv) > 1:
        main()
    else:
        # 测试用例
        test_cases = [
            """
            SELECT u.name, p.title, c.content 
            FROM users u 
            INNER JOIN posts p ON u.id = p.user_id 
            LEFT JOIN comments c ON p.id = c.post_id 
            WHERE u.active = 1 
            ORDER BY p.created_at DESC
            """,
            
            """
            SELECT * FROM table1 t1
            LEFT OUTER JOIN table2 t2 ON t1.id = t2.ref_id
            RIGHT JOIN table3 t3 ON t2.id = t3.ref_id
            WHERE t1.status = 'active'
            """,
            
            """
            SELECT COUNT(*) 
            FROM orders o
            INNER JOIN customers c ON o.customer_id = c.id
            CROSS JOIN products p
            WHERE o.order_date > '2023-01-01'
            GROUP BY c.country
            """
            
            """
            SELECT COUNT(*) 
            FROM orders o -- 这里是一个错误的注释
            INNER JOIN customers c ON o.customer_id = c.id
            CROSS JOIN products p
            WHERE o.order_date > '2023-01-01'
            GROUP BY c.country
            """
            
            """
            SELECT COUNT(*) 
            FROM orders o
            INNER JOIN customers c ON o.customer_id = c.id
            INNER JOIN customers c ON o.customer_id = c.id
            INNER JOIN customers c ON o.customer_id = c.id
            
            CROSS JOIN products p
            WHERE o.order_date > '2023-01-01'
            INNER JOIN customers c ON o.customer_id = c.id
            
            GROUP BY c.country
            """
        ]
        
        remover = SQLJoinRemover()
        
        for i, test_sql in enumerate(test_cases, 1):
            print(f"\n=== 测试用例 {i} ===")
            print("原始SQL:")
            print(test_sql.strip())
            print("\n移除JOIN后:")
            result = remover.remove_joins_from_sql(test_sql)
            print(result)
            print("-" * 50)