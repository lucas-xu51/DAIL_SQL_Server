import sqlglot
from sqlglot import expressions as exp

def detect_round_type_issues(sql, source_dialect="mysql"):
    """专门检测ROUND函数的类型兼容性问题"""
    try:
        parsed = sqlglot.parse_one(sql, dialect=source_dialect)
    except Exception as e:
        print(f"SQL解析失败: {e}")
        return []
    
    issues = []
    for node in parsed.walk():
        if isinstance(node, exp.Round):
            # 检查参数
            if len(node.args) > 1:  # 有精度参数的ROUND
                first_arg = node.this
                
                # 检查是否包含CAST到FLOAT/DOUBLE PRECISION
                for cast_node in first_arg.walk():
                    if isinstance(cast_node, exp.Cast):
                        # 正确获取类型名称
                        target_type_obj = cast_node.to
                        
                        # 方法1: 使用sql()方法获取类型字符串
                        target_type_str = target_type_obj.sql()
                        
                        # 方法2: 或者检查类型对象的类名
                        type_class_name = type(target_type_obj).__name__
                        
                        print(f"Debug: 类型字符串={target_type_str}, 类型类名={type_class_name}")
                        
                        if target_type_str.upper() in ['FLOAT', 'DOUBLE PRECISION', 'REAL', 'DOUBLE']:
                            issues.append({
                                'issue': f'ROUND with {target_type_str} not supported in PostgreSQL',
                                'suggestion': f'Use CAST(... AS NUMERIC) instead of CAST(... AS {target_type_str})',
                                'node': str(cast_node),
                                'original_type': target_type_str
                            })
    
    return issues
