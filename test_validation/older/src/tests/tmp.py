import sqlparse
from sqlparse.sql import Identifier, Comparison, Where, Function, TokenList, Comment
from sqlparse.tokens import Keyword, DML, Punctuation, Operator, Comment as CommentToken

class JoinStateMachine:
    """
    状态机管理单个TokenList层级内的JOIN上下文
    状态转换: NONE → JOIN_DETECTED → ON_CLAUSE → (WHERE等重置到NONE)
    """
    
    def __init__(self, debug=False):
        self.state = 'NONE'
        self.join_type = None
        self.debug = debug
        
    def reset(self, reason=""):
        """重置状态机"""
        if self.debug and self.state != 'NONE':
            print(f"    [STATE] RESET: {self.state} → NONE ({reason})")
        self.state = 'NONE'
        self.join_type = None
    
    def process_token(self, token, position, path):
        """处理单个token，返回是否发生状态变化"""
        old_state = self.state
        old_join_type = self.join_type
        
        if hasattr(token, 'ttype') and token.ttype is Keyword:
            keyword = token.value.upper().strip()
            
            # JOIN关键字检测 - 支持组合关键字
            if keyword in ('JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN'):
                # 提取JOIN类型
                if keyword == 'JOIN':
                    new_join_type = 'INNER'
                elif keyword == 'INNER JOIN':
                    new_join_type = 'INNER'
                elif keyword == 'LEFT JOIN':
                    new_join_type = 'LEFT'
                elif keyword == 'RIGHT JOIN':
                    new_join_type = 'RIGHT'
                elif keyword == 'FULL JOIN':
                    new_join_type = 'FULL'
                elif keyword == 'CROSS JOIN':
                    new_join_type = 'CROSS'
                else:
                    new_join_type = 'INNER'  # 默认
                
                self.state = 'JOIN_DETECTED'
                self.join_type = new_join_type
                if self.debug:
                    print(f"    [STATE] {path}: {old_state} → JOIN_DETECTED (type: {new_join_type})")
                return True
            
            # 单独的JOIN类型关键字（用于 LEFT JOIN 这样的分开写法）
            elif keyword in ('INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS'):
                new_join_type = keyword
                self.state = 'JOIN_DETECTED'
                self.join_type = new_join_type
                if self.debug:
                    print(f"    [STATE] {path}: {old_state} → JOIN_DETECTED (type: {new_join_type})")
                return True
                
            # ON关键字检测
            elif keyword == 'ON':
                if self.state == 'JOIN_DETECTED':
                    self.state = 'ON_CLAUSE'
                    if self.debug:
                        print(f"    [STATE] {path}: JOIN_DETECTED → ON_CLAUSE (type: {self.join_type})")
                    return True
                else:
                    if self.debug:
                        print(f"    [STATE] {path}: ON keyword ignored (current state: {self.state})")
                    
            # 终止关键字检测
            elif keyword in ('WHERE', 'GROUP', 'ORDER', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT'):
                if self.state != 'NONE':
                    self.reset(f"terminated by {keyword}")
                return True
        
        return False
    
    def in_on_clause(self):
        """当前是否在ON子句中"""
        return self.state == 'ON_CLAUSE'
    
    def get_join_type(self):
        """获取当前JOIN类型"""
        return self.join_type
    
    def get_state(self):
        """获取当前状态"""
        return self.state


def _resolve_identifier(token, alias_map, debug=False):
    """解析标识符 - 保持原有逻辑"""
    if debug:
        print(f"      [RESOLVE] Processing: {repr(token)} (type: {type(token).__name__})")
    
    if isinstance(token, Identifier):
        parent_name = token.get_parent_name()
        real_name = token.get_real_name()
        
        resolved_table = None
        if parent_name:
            resolved_table = alias_map.get(parent_name, parent_name)
        
        result = (resolved_table, real_name)
        if debug:
            print(f"      [RESOLVE] Result: {result}")
        return result

    elif isinstance(token, Function):
        for sub_token in token.get_parameters():
             if isinstance(sub_token, Identifier):
                 parent_name = sub_token.get_parent_name()
                 if parent_name:
                      resolved_table = alias_map.get(parent_name, parent_name)
                      return (resolved_table, str(token))
        return (None, str(token))

    elif isinstance(token, TokenList):
        significant_tokens = []
        if hasattr(token, 'tokens'):
            for sub_token in token.tokens:
                 if not (sub_token.is_whitespace or isinstance(sub_token, Comment) or \
                        sub_token.match(Punctuation, '(') or sub_token.match(Punctuation, ')') or \
                        sub_token.match(Punctuation, ',')):
                     significant_tokens.append(sub_token)

        if len(significant_tokens) == 1:
            return _resolve_identifier(significant_tokens[0], alias_map, debug)
        else:
            return (None, str(token))

    return None, None


def _canonicalize_join(join_tuple, debug=False):
    """标准化JOIN元组"""
    join_type, (t1, c1), op, (t2, c2) = join_tuple
    
    symmetric_ops = {'=', '<>', '!='}
    
    if t1 is None or t2 is None or c1 is None or c2 is None:
        return join_tuple
    
    if op in symmetric_ops and (t1, c1) > (t2, c2):
        if debug:
            print(f"      [CANONICAL] Swapping: ({t1}, {c1}) ↔ ({t2}, {c2})")
        return (join_type, (t2, c2), op, (t1, c1))
    
    return join_tuple


def analyze_join_contexts_layered(token_list, path="", debug=False):
    """
    分层状态机：在每个TokenList层级内维护独立的状态机
    """
    join_contexts = {}
    
    if debug:
        print(f"\n  [LAYER] Analyzing layer: {path or 'root'}")
        print(f"  [LAYER] TokenList type: {type(token_list).__name__}")
        if hasattr(token_list, 'tokens'):
            print(f"  [LAYER] Direct children count: {len(token_list.tokens)}")
    
    # 检查是否有直接子tokens
    if not hasattr(token_list, 'tokens') or not token_list.tokens:
        if debug:
            print(f"  [LAYER] No tokens to process")
        return join_contexts
    
    # 为当前层级创建状态机
    state_machine = JoinStateMachine(debug=debug)
    
    # 线性扫描当前层级的直接子tokens
    for i, token in enumerate(token_list.tokens):
        current_path = f"{path}[{i}]" if path else f"[{i}]"
        
        if debug:
            print(f"  [SCAN] {current_path}: {type(token).__name__} = '{str(token).strip()}'")
            print(f"  [SCAN] Current state: {state_machine.get_state()}")
        
        # 跳过空白和注释
        if token.is_whitespace or isinstance(token, Comment):
            if debug:
                print(f"  [SCAN] Skipping whitespace/comment")
            continue
        
        # 处理当前token，检查状态变化
        state_changed = state_machine.process_token(token, i, current_path)
        
        # 如果当前token是Comparison且状态机在ON子句中，标记为显式JOIN
        if isinstance(token, Comparison):
            if debug:
                print(f"  [COMPARISON] Found at {current_path}")
                print(f"  [COMPARISON] State machine in ON clause: {state_machine.in_on_clause()}")
                print(f"  [COMPARISON] JOIN type: {state_machine.get_join_type()}")
            
            if state_machine.in_on_clause():
                join_contexts[current_path] = {
                    'type': state_machine.get_join_type(),
                    'in_on': True
                }
                if debug:
                    print(f"  [MARK] *** EXPLICIT JOIN CONTEXT: {current_path} → {state_machine.get_join_type()} ***")
        
        # 递归处理子TokenList（嵌套结构）
        if hasattr(token, 'tokens') and token.tokens:
            if debug:
                print(f"  [RECURSE] Recursing into {current_path}")
            child_contexts = analyze_join_contexts_layered(token, current_path, debug)
            join_contexts.update(child_contexts)
    
    if debug:
        print(f"  [LAYER] Layer {path or 'root'} completed. Contexts found: {len(join_contexts)}")
        for ctx_path, ctx_info in join_contexts.items():
            if ctx_path.startswith(path or ""):  # 只显示当前层级的上下文
                print(f"    {ctx_path}: {ctx_info}")
    
    return join_contexts


def extract_join_conditions_state_machine(parsed, aliases=None, debug=False):
    """
    使用分层状态机的JOIN条件提取
    """
    if debug:
        print(f"\n{'='*80}")
        print(f"LAYERED STATE MACHINE JOIN EXTRACTION")
        print(f"{'='*80}")
    
    join_conditions = set()
    
    # 处理aliases参数
    if aliases is None:
        aliases = set()
    elif isinstance(aliases, dict):
        aliases = set((k, v) for k, v in aliases.items())
    
    alias_map = {alias: real for real, alias in aliases}
    
    # 简化的表提取（专注于JOIN逻辑验证）
    def extract_tables_simple(token_list):
        tables = {}
        def scan(token):
            if isinstance(token, Identifier):
                real_name = token.get_real_name()
                alias_name = token.get_alias()
                if real_name:
                    tables[real_name] = alias_name
                    if alias_name:
                        alias_map[alias_name] = real_name
            elif hasattr(token, 'tokens'):
                for sub_token in token.tokens:
                    if sub_token is not None:
                        scan(sub_token)
        scan(token_list)
        return tables
    
    table_info = extract_tables_simple(parsed)
    if debug:
        print(f"Tables extracted: {table_info}")
        print(f"Alias map: {alias_map}")
    
    # 分层状态机分析JOIN上下文
    if debug:
        print(f"\n[PHASE 1] JOIN Context Analysis")
        print(f"-" * 40)
    
    join_contexts = analyze_join_contexts_layered(parsed, "", debug)
    
    if debug:
        print(f"\n[PHASE 1 RESULT] Total JOIN contexts: {len(join_contexts)}")
        for path, context in join_contexts.items():
            print(f"  {path}: {context}")
    
    # 查找所有Comparison对象
    if debug:
        print(f"\n[PHASE 2] Comparison Discovery")
        print(f"-" * 40)
    
    def find_all_comparisons(token_list, path=""):
        comparisons = []
        if isinstance(token_list, Comparison):
            comparisons.append((path, token_list))
            if debug:
                print(f"  [COMP] Found: {path}")
        elif hasattr(token_list, 'tokens'):
            for i, token in enumerate(token_list.tokens):
                if token is not None:
                    sub_path = f"{path}[{i}]" if path else f"[{i}]"
                    comparisons.extend(find_all_comparisons(token, sub_path))
        return comparisons
    
    all_comparisons = find_all_comparisons(parsed)
    
    if debug:
        print(f"\n[PHASE 2 RESULT] Total comparisons: {len(all_comparisons)}")
        for path, comp in all_comparisons:
            print(f"  {path}: {comp}")
    
    # 分类和处理Comparison对象
    if debug:
        print(f"\n[PHASE 3] Comparison Classification")
        print(f"-" * 40)
    
    for comp_path, comparison in all_comparisons:
        if debug:
            print(f"\n  [CLASSIFY] Processing: {comp_path}")
        
        # 查找对应的JOIN上下文
        join_info = join_contexts.get(comp_path)
        
        if debug:
            print(f"  [CLASSIFY] JOIN context: {join_info}")
        
        # 从tokens中正确提取操作符和操作数（解决注释干扰问题）
        op = None
        left_operand = None
        right_operand = None
        
        # 分析comparison的tokens来正确识别操作数和操作符
        significant_tokens = []
        for token in comparison.tokens:
            if not (token.is_whitespace or isinstance(token, Comment)):
                significant_tokens.append(token)
        
        if debug:
            print(f"  [CLASSIFY] Significant tokens: {[str(t) for t in significant_tokens]}")
        
        # 查找操作符
        op_index = -1
        for i, token in enumerate(significant_tokens):
            if hasattr(token, 'ttype') and token.ttype in Operator.Comparison:
                op = token.value
                op_index = i
                break
        
        if op_index == -1 or len(significant_tokens) < 3:
            if debug:
                print(f"  [CLASSIFY] Invalid comparison structure, skipping")
            continue
        
        # 操作符左边的第一个非空白非注释token是左操作数
        left_operand = significant_tokens[0] if op_index > 0 else None
        
        # 操作符右边的第一个非空白非注释token是右操作数  
        right_operand = significant_tokens[op_index + 1] if op_index + 1 < len(significant_tokens) else None
        
        if debug:
            print(f"  [CLASSIFY] Extracted: Left: {left_operand}, Op: {op}, Right: {right_operand}")
        
        if not (left_operand and op and right_operand):
            if debug:
                print(f"  [CLASSIFY] Missing operands or operator, skipping")
            continue
        
        # 解析左右操作数
        l_table, l_col = _resolve_identifier(left_operand, alias_map, debug)
        r_table, r_col = _resolve_identifier(right_operand, alias_map, debug)
        
        if debug:
            print(f"  [CLASSIFY] Resolved: ({l_table}, {l_col}) {op} ({r_table}, {r_col})")
        
        # 检查是否为表间比较（潜在JOIN）
        if l_col and r_col and l_table and r_table and l_table != r_table:
            # 确定JOIN类型
            if join_info and join_info.get('in_on'):
                join_type = join_info['type']
                if debug:
                    print(f"  [CLASSIFY] *** EXPLICIT {join_type} JOIN ***")
            else:
                join_type = 'IMPLICIT'
                if debug:
                    print(f"  [CLASSIFY] *** IMPLICIT JOIN ***")
            
            # 创建JOIN元组
            join_tuple = (join_type, (l_table, l_col), op, (r_table, r_col))
            canonical = _canonicalize_join(join_tuple, debug)
            join_conditions.add(canonical)
            
            if debug:
                print(f"  [CLASSIFY] *** ADDED JOIN: {canonical} ***")
        else:
            if debug:
                print(f"  [CLASSIFY] Not a table-to-table comparison, skipping")
    
    if debug:
        print(f"\n{'='*80}")
        print(f"FINAL RESULT")
        print(f"{'='*80}")
        print(f"Total JOIN conditions: {len(join_conditions)}")
        for join in join_conditions:
            print(f"  {join}")
    
    return join_conditions


if __name__ == "__main__":
    # 使用简化函数名以便调用
    def extract_join_conditions(parsed, aliases):
        return extract_join_conditions_state_machine(parsed, aliases, debug=False)
    
    print("🎉 COMPREHENSIVE JOIN EXTRACTION TEST SUITE")
    print("="*80)
    
    # Test Case 1 (Complex Multiple Explicit Joins)
    print("\n📋 TEST CASE 1: Complex Multiple Explicit Joins")
    print("-" * 60)
    sql = 'SELECT e.emp_id, e.emp_name, d.dept_name, p.project_name, a.role, a.assign_date FROM employees e INNER JOIN departments d ON e.dept_id = d.dept_id INNER JOIN assignments a ON e.emp_id = a.emp_id INNER JOIN projects p ON a.project_id = p.project_id WHERE p.end_date > CURRENT_DATE ORDER BY d.dept_name, e.emp_name;'
    parsed = sqlparse.parse(sql)[0]
    aliases = {('employees', 'e'), ('departments', 'd'), ('assignments', 'a'), ('projects', 'p')}
    joins = extract_join_conditions(parsed, aliases.copy())
    print(f"SQL: {sql}")
    print(f"Extracted Joins: {joins}")
    print(f"Expected: Multiple INNER JOINs between employees-departments, employees-assignments, assignments-projects")
    
    # Test Case 2 (Implicit Join in WHERE clause)
    print("\n📋 TEST CASE 2: Implicit Join in WHERE clause")
    print("-" * 60)
    sql2 = "SELECT t1.name, t3.city FROM table1 t1, table2 t2, table3 t3 WHERE t1.id = t2.ref_id AND t2.city_id = t3.id AND t1.age > 30"
    parsed2 = sqlparse.parse(sql2)[0]
    aliases2 = {('table1', 't1'), ('table2', 't2'), ('table3', 't3')}
    joins2 = extract_join_conditions(parsed2, aliases2.copy())
    print(f"SQL: {sql2}")
    print(f"Extracted Joins: {joins2}")
    print(f"Expected: IMPLICIT JOINs between table1-table2 and table2-table3")
    
    # Test Case 3 (Explicit LEFT JOIN)
    print("\n📋 TEST CASE 3: Explicit LEFT JOIN")
    print("-" * 60)
    sql3 = "SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE c.country = 'USA'"
    parsed3 = sqlparse.parse(sql3)[0]
    aliases3 = {('orders', 'o'), ('customers', 'c')}
    joins3 = extract_join_conditions(parsed3, aliases3.copy())
    print(f"SQL: {sql3}")
    print(f"Extracted Joins: {joins3}")
    print(f"Expected: LEFT JOIN between orders and customers")
    
    # Test Case 4 (Multiple Explicit JOINs with formatting)
    print("\n📋 TEST CASE 4: Multiple Explicit JOINs with extra spaces")
    print("-" * 60)
    sql4 = "SELECT p.name, c.category_name, s.supplier_name FROM products p JOIN categories c ON p.category_id = c.id JOIN suppliers s ON p.supplier_id = s.id WHERE c.category_name = 'Electronics';"
    parsed4 = sqlparse.parse(sql4)[0]
    aliases4 = {('products', 'p'), ('categories', 'c'), ('suppliers', 's')}
    joins4 = extract_join_conditions(parsed4, aliases4.copy())
    print(f"SQL: {sql4}")
    print(f"Extracted Joins: {joins4}")
    print(f"Expected: INNER JOINs between products-categories and products-suppliers")
    
    # Test Case 5 (JOIN with comments)
    print("\n📋 TEST CASE 5: JOIN with comments and AS keywords")
    print("-" * 60)
    sql5 = """
    SELECT o.order_id, c.customer_name -- Get order and customer name
    FROM orders AS o -- Use alias o for orders
    INNER JOIN customers AS c -- Use alias c for customers
      ON o.customer_id = c.customer_id -- Join on customer ID
    WHERE o.order_date > '2023-01-01';
    """
    parsed5 = sqlparse.parse(sql5)[0]
    aliases5 = {('orders', 'o'), ('customers', 'c')}
    joins5 = extract_join_conditions(parsed5, aliases5.copy())
    print(f"SQL: {sql5}")
    print(f"Extracted Joins: {joins5}")
    print(f"Expected: INNER JOIN between orders and customers")
    
    # Bonus: Original problem case
    print("\n📋 BONUS TEST: Original Problem Case")
    print("-" * 60)
    original_sql = "SELECT account.account_id FROM card JOIN account ON card.account_id = account.account_id WHERE card.type = 'gold';"
    parsed_orig = sqlparse.parse(original_sql)[0]
    aliases_orig = set()
    joins_orig = extract_join_conditions(parsed_orig, aliases_orig)
    print(f"SQL: {original_sql}")
    print(f"Extracted Joins: {joins_orig}")
    expected_orig = {('INNER', ('account', 'account_id'), '=', ('card', 'account_id'))}
    print(f"Expected: {expected_orig}")
    
    if joins_orig == expected_orig:
        print("✅ ORIGINAL PROBLEM: SOLVED!")
    else:
        print("❌ ORIGINAL PROBLEM: Still has issues")
        