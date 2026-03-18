from extract.struct import get_all_function

def check_dialect_function(sql: str, dialect='mysql') -> str:
    try:
        functions = get_all_function(sql, dialect=dialect)
        dialect_functions = set()
        print(f"Debug: SQL中使用的函数: {functions}")

        if dialect == 'mysql':
            dialect_functions = {
                # 聚合函数
                'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT', 'STD', 'STDDEV',
                # 日期时间函数
                'NOW', 'CURDATE', 'CURTIME', 'CURRENT_TIMESTAMP', 'UNIX_TIMESTAMP',
                'DATEDIFF', 'DATE_ADD', 'DATE_SUB', 'TIMESTAMPDIFF', 'DATE_FORMAT',
                'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
                # 条件函数
                'IF', 'IFNULL', 'ISNULL', 'COALESCE', 'NULLIF',
                # 字符串函数
                'LENGTH', 'CHAR_LENGTH', 'SUBSTRING', 'SUBSTR', 'LEFT', 'RIGHT',
                'UPPER', 'LOWER', 'CONCAT', 'CONCAT_WS', 'TRIM', 'LTRIM', 'RTRIM',
                'REPLACE', 'LOCATE', 'POSITION', 'INSTR',
                # 数学函数
                'ROUND', 'FLOOR', 'CEIL', 'CEILING', 'ABS', 'MOD', 'POWER', 'SQRT',
                'RAND', 'GREATEST', 'LEAST',
                # 类型转换和条件
                'CAST', 'CONVERT', 'CASE'
            }
        elif dialect == 'postgres':
            dialect_functions = {
                # 聚合函数
                'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STRING_AGG', 'ARRAY_AGG', 
                'STDDEV', 'STDDEV_POP', 'STDDEV_SAMP', 'VARIANCE', 'VAR_POP', 'VAR_SAMP',
                # 日期时间函数
                'NOW', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CLOCK_TIMESTAMP',
                'AGE', 'DATE_PART', 'DATE_TRUNC', 'EXTRACT', 'TO_DATE', 'TO_TIMESTAMP',
                'TO_CHAR', 'INTERVAL',
                # 条件函数
                'COALESCE', 'NULLIF', 'GREATEST', 'LEAST',
                # 字符串函数
                'LENGTH', 'CHAR_LENGTH', 'CHARACTER_LENGTH', 'SUBSTRING', 'SUBSTR', 
                'LEFT', 'RIGHT', 'UPPER', 'LOWER', 'CONCAT', 'TRIM', 'LTRIM', 'RTRIM',
                'REPLACE', 'POSITION', 'SPLIT_PART', 'REPEAT', 'REVERSE',
                # 数学函数
                'ROUND', 'FLOOR', 'CEIL', 'CEILING', 'ABS', 'MOD', 'POWER', 'SQRT',
                'RANDOM', 'TRUNC',
                # 类型转换和条件表达式
                'CAST', 'CASE',
                # PostgreSQL特有函数
                'GENERATE_SERIES', 'UNNEST', 'ROW_NUMBER', 'RANK', 'DENSE_RANK',
                'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE'
            }

        unsupported_functions = functions - dialect_functions
        if unsupported_functions:
            print(f"Warning: SQL中包含不属于{dialect}的函数: {unsupported_functions}")
            return f"Error: SQL not include {dialect} function: {unsupported_functions}"
    except Exception as e:
        return None
    return None

