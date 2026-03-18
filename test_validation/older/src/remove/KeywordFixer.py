import re
from typing import Set, List

class SQLKeywordFixer:
    
    def __init__(self):
        self.reserved_keywords = {
            'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'BACKUP', 'BETWEEN', 'CASE',
            'CHECK', 'COLUMN', 'CONSTRAINT', 'CREATE', 'DATABASE', 'DEFAULT', 'DELETE',
            'DESC', 'DISTINCT', 'DROP', 'EXEC', 'EXISTS', 'FOREIGN', 'FROM', 'FULL',
            'GROUP', 'HAVING', 'IN', 'INDEX', 'INNER', 'INSERT', 'INTO', 'IS', 'JOIN',
            'KEY', 'LEFT', 'LIKE', 'LIMIT', 'NOT', 'NULL', 'OR', 'ORDER', 'OUTER',
            'PRIMARY', 'PROCEDURE', 'RIGHT', 'ROWNUM', 'SELECT', 'SET', 'TABLE', 'TOP',
            'TRUNCATE', 'UNION', 'UNIQUE', 'UPDATE', 'VALUES', 'VIEW', 'WHERE', 'WITH',
            'DECLARE', 'IF', 'ELSE', 'WHILE', 'FOR', 'BREAK', 'CONTINUE', 'RETURN',
            'BEGIN', 'END', 'TRY', 'CATCH', 'THROW', 'TRANSACTION', 'COMMIT', 'ROLLBACK',
            'GRANT', 'REVOKE', 'DENY', 'USER', 'ROLE', 'SCHEMA', 'FUNCTION', 'TRIGGER',
            'CURSOR', 'FETCH', 'OPEN', 'CLOSE', 'DEALLOCATE', 'CAST', 'CONVERT',
            'MERGE', 'WHEN', 'THEN', 'MATCH', 'PARTIAL', 'FULL', 'SIMPLE', 'CASCADE',
            'RESTRICT', 'ACTION', 'REFERENCES', 'ON', 'DEFERRABLE', 'INITIALLY',
            'DEFERRED', 'IMMEDIATE'
        }
    
    def is_keyword(self, identifier: str) -> bool:
        return identifier.upper() in self.reserved_keywords
    
    def escape_identifier(self, identifier: str) -> str:
        if identifier.startswith('`') and identifier.endswith('`'):
            return identifier
        return f'`{identifier}`'
    
    def extract_identifiers(self, sql: str) -> List[dict]:

        identifiers = []
        
        patterns = [
            (r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)', 'table'),
            (r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)', 'table'),
            (r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)', 'table'),
            (r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)', 'table'),
            (r'\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)', 'table'),
            (r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', 'identifier')
        ]
        
        for pattern, id_type in patterns:
            for match in re.finditer(pattern, sql, re.IGNORECASE):
                identifier = match.group(1) if '(' in pattern else match.group(0)
                
                if id_type == 'identifier':
                    context_before = sql[max(0, match.start()-20):match.start()].upper()
                    context_after = sql[match.end():match.end()+20].upper()
                    
                    if any(kw in context_before for kw in ['SELECT', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING']):
                        if identifier.upper() in ['BY', 'FROM', 'WHERE', 'HAVING', 'GROUP', 'ORDER']:
                            continue
                
                identifiers.append({
                    'original': identifier,
                    'start': match.start(1) if '(' in pattern else match.start(),
                    'end': match.end(1) if '(' in pattern else match.end(),
                    'type': id_type
                })
        
        return identifiers
    
    def fix_sql_keywords(self, sql: str) -> str:

        sql = sql.strip()
        
        identifiers = self.extract_identifiers(sql)
        
        identifiers.sort(key=lambda x: x['start'], reverse=True)
        
        conflicts_found = []
        
        for identifier_info in identifiers:
            identifier = identifier_info['original']
            
            if self.is_keyword(identifier):
                start_pos = identifier_info['start']
                end_pos = identifier_info['end']
                
                has_backtick_before = start_pos > 0 and sql[start_pos-1] == '`'
                has_backtick_after = end_pos < len(sql) and sql[end_pos] == '`'
                
                if not (has_backtick_before and has_backtick_after):
                    escaped_identifier = self.escape_identifier(identifier)
                    sql = sql[:start_pos] + escaped_identifier + sql[end_pos:]
                    conflicts_found.append(identifier)
        
        return sql, conflicts_found
    
    def analyze_sql(self, sql: str) -> dict:

        fixed_sql, conflicts = self.fix_sql_keywords(sql)
        
        return {
            'original_sql': sql,
            'fixed_sql': fixed_sql,
            'conflicts_found': conflicts,
            'has_conflicts': len(conflicts) > 0,
            'total_conflicts': len(conflicts)
        }

# 使用示例
def main():
    fixer = SQLKeywordFixer()
    
    # 测试用例
    test_cases = [
        "SELECT account.district_id FROM order WHERE order_id = 33333",
        "SELECT * FROM user WHERE user.name = 'test'",
        "INSERT INTO order (order_id, user_id) VALUES (1, 2)",
        "UPDATE table SET column = 'value' WHERE id = 1",
        "SELECT user.name, order.date FROM user JOIN order ON user.id = order.user_id",
        "CREATE TABLE group (id INT, name VARCHAR(50))"
    ]
    
    print("=" * 80)
    
    for i, sql in enumerate(test_cases, 1):
        
        result = fixer.analyze_sql(sql)
        
        if result['has_conflicts']:
            print(f"fix: {result['fixed_sql']}")
            print(f"find: {', '.join(result['conflicts_found'])}")
        else:
            print("correct")
        
        print("-" * 80)

if __name__ == "__main__":
    main()