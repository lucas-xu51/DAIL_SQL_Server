#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import re
import json
from collections import defaultdict
from typing import Set, List, Tuple, Dict, Optional, Optional
from difflib import SequenceMatcher

class ImprovedSQLValidator:
    """改进的 SQL 验证器 - 支持别名识别和大小写不敏感"""
    
    def __init__(self, db_path: str, schema_sql_path: Optional[str] = None):
        self.db_path = db_path
        self.schema_sql_path = schema_sql_path
        self.conn = None
        self.formal_schema = {'Tabs': set(), 'Cols': {}, 'FKs': set()}
        self.alias_map = {}  # 别名 -> 真实表名
        self._build_schema()
    
    def _build_schema(self):
        """从数据库提取 schema，并补充 SQL schema 文件中的外键"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()
            
            # 获取所有表名（小写存储）
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            self.formal_schema['Tabs'] = set(t.lower() for t in tables)
            
            # 获取每个表的列
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table})")
                cols = [row[1] for row in cursor.fetchall()]
                self.formal_schema['Cols'][table.lower()] = set(c.lower() for c in cols)
            
            # 第一步：从 SQLite PRAGMA 获取外键关系
            for table in tables:
                cursor.execute(f"PRAGMA foreign_key_list({table})")
                for row in cursor.fetchall():
                    local_col = row[3]
                    ref_table = row[2]
                    ref_col = row[4]
                    self.formal_schema['FKs'].add((
                        table.lower(),
                        local_col.lower(),
                        ref_table.lower(),
                        ref_col.lower()
                    ))
            
            # print(f"✓ 从 SQLite PRAGMA 获取 {len(self.formal_schema['FKs'])} 个外键关系")
            
            # 第二步：从 SQL schema 文件补充外键定义
            if self.schema_sql_path:
                schema_fks = self._extract_fks_from_sql_file(self.schema_sql_path)
                # print(f"✓ 从 SQL 文件获取 {len(schema_fks)} 个外键定义")
                self.formal_schema['FKs'].update(schema_fks)
            
            # print(f"✓ 数据库加载成功: {len(self.formal_schema['Tabs'])} 表")
            # for table, cols in self.formal_schema['Cols'].items():
            #     print(f"  - {table}: {len(cols)} 列")
            # print(f"✓ 总共 {len(self.formal_schema['FKs'])} 个外键关系:")
            # for src_t, src_c, tgt_t, tgt_c in sorted(self.formal_schema['FKs']):
            #     print(f"  - {src_t}.{src_c} -> {tgt_t}.{tgt_c}")
            
        except Exception as e:
            print(f"✗ 数据库加载失败: {e}")
            raise
    
    def _extract_fks_from_sql_file(self, sql_file_path: str) -> Set[Tuple[str, str, str, str]]:
        """从 SQL schema 文件中提取外键定义"""
        fks = set()
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 提取 CREATE TABLE 块
            # 模式: CREATE TABLE "table_name" (...) ;
            create_table_pattern = r'CREATE\s+TABLE\s+"?(\w+)"?\s*\(([^)]+)\);'
            
            for match in re.finditer(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL):
                table_name = match.group(1).lower()
                table_definition = match.group(2)
                
                # 在表定义中查找 FOREIGN KEY 声明
                # 模式: FOREIGN KEY (local_col) REFERENCES ref_table(ref_col)
                fk_pattern = r'FOREIGN\s+KEY\s*\(\s*"?(\w+)"?\s*\)\s*REFERENCES\s+"?(\w+)"?\s*\(\s*"?(\w+)"?\s*\)'
                
                for fk_match in re.finditer(fk_pattern, table_definition, re.IGNORECASE):
                    local_col = fk_match.group(1).lower()
                    ref_table = fk_match.group(2).lower()
                    ref_col = fk_match.group(3).lower()
                    
                    fks.add((table_name, local_col, ref_table, ref_col))
        
        except Exception as e:
            print(f"⚠️  从 SQL 文件提取外键失败: {e}")
        
        return fks
    
    def _extract_table_alias_map(self, sql: str) -> Dict[str, str]:
        """
        从 SQL 提取表别名映射，包括主查询和所有子查询
        返回: {别名 -> 真实表名} (都是小写)
        """
        alias_map = {}
        
        # FROM table [AS] alias - 匹配所有层级的查询
        from_pattern = r'FROM\s+`?(\w+)`?(?:\s+(?:AS\s+)?`?(\w+)`?)?'
        for match in re.finditer(from_pattern, sql, re.IGNORECASE):
            real_table = match.group(1).lower()
            alias = match.group(2)
            if alias:
                alias_map[alias.lower()] = real_table
        
        # JOIN table [AS] alias - 匹配所有层级的查询
        join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?(?:OUTER\s+)?JOIN\s+`?(\w+)`?(?:\s+(?:AS\s+)?`?(\w+)`?)?'
        for match in re.finditer(join_pattern, sql, re.IGNORECASE):
            real_table = match.group(1).lower()
            alias = match.group(2)
            if alias:
                alias_map[alias.lower()] = real_table
        
        return alias_map
    
    def _remove_subqueries(self, sql: str) -> str:
        """
        移除 SQL 中的子查询（括号内容），保留顶层的 FROM/JOIN 结构。
        用于提取顶层表和检查顶层连接。
        
        对于 EXCEPT/UNION/INTERSECT 等集合操作符，只保留第一个 SELECT 语句，
        因为集合操作符将查询分为独立部分，每部分有独立的表和 JOIN 结构。
        """
        result = sql
        
        # 处理集合操作符：只保留第一个 SELECT 语句
        # EXCEPT, UNION, INTERSECT 都是集合操作，不应强制 JOIN 连接跨越两个部分
        set_operators = r'\b(EXCEPT|UNION|INTERSECT)\b'
        match = re.search(set_operators, result, re.IGNORECASE)
        if match:
            # 找到第一个集合操作符的位置，只保留它之前的部分
            pos = match.start()
            result = result[:pos]
        
        # 迭代移除最内层的括号及其内容
        max_iterations = 100
        iteration = 0
        while '(' in result and iteration < max_iterations:
            # 匹配最内层的括号（不包含其他括号的括号对）
            result = re.sub(r'\([^()]*\)', ' ', result)
            iteration += 1
        return result
    
    def _validate_subquery_columns(self, sql: str) -> List[str]:
        """
        验证 subquery 中的列引用是否有效。
        从 SQL 中提取所有 subquery，并检查其中的列是否存在于相应的表中。
        只检查限定列名（table.column）引用，避免误判SQL关键字为列名。
        """
        errors = []
        
        # 提取所有 subquery（括号内的 SELECT 语句）
        # 匹配 (SELECT ...FROM...) 模式
        subquery_pattern = r'\(\s*SELECT\s+.*?\s+FROM\s+`?(\w+)`?.*?\)'
        
        for match in re.finditer(subquery_pattern, sql, re.IGNORECASE | re.DOTALL):
            subquery = match.group(0)
            
            # 从 subquery 中提取 FROM 后的表名
            from_match = re.search(r'FROM\s+`?(\w+)`?', subquery, re.IGNORECASE)
            if from_match:
                table_name = from_match.group(1).lower()
                
                # 检查表是否存在
                if table_name not in self.formal_schema['Tabs']:
                    continue  # 表名错误会在主查询中捕捉
                
                # 只检查限定列名（table.column）引用，避免误判SQL关键字
                col_refs = self._extract_table_column_references(subquery)
                
                for table_or_alias, column in col_refs:
                    # 对于 subquery 中的列，检查是否是有效的列引用
                    if table_or_alias and table_or_alias.lower() == table_name:
                        if column not in self.formal_schema['Cols'].get(table_name, set()):
                            # 查找该列是否在其他表中存在
                            found_in_other = []
                            for t in self.formal_schema['Tabs']:
                                if column in self.formal_schema['Cols'].get(t, set()):
                                    found_in_other.append(t)
                            
                            if found_in_other:
                                errors.append(
                                    f"LOGIC: In subquery, table '{table_name}' does not have column '{column}'. "
                                    f"Column exists in: {', '.join(sorted(found_in_other))}"
                                )
                            else:
                                errors.append(
                                    f"LOGIC: In subquery, table '{table_name}' does not have column '{column}'."
                                )
        
        return errors
    
    def _resolve_table_name(self, name: str) -> Optional[str]:
        """
        解析表名或别名到真实表名 (小写)
        """
        name_lower = name.lower()
        
        # 先检查是否是别名
        if name_lower in self.alias_map:
            return self.alias_map[name_lower]
        
        # 再检查是否是真实表名
        if name_lower in self.formal_schema['Tabs']:
            return name_lower
        
        return None
    
    def _extract_table_column_references(self, sql: str) -> List[Tuple[Optional[str], str]]:
        """
        从 SQL 提取所有的 table.column 或 alias.column 引用
        返回: [(table_or_alias, column), ...]
        """
        references = []
        
        # 匹配 table.column 或 alias.column 或 column
        # 这个模式捕捉所有可能的列引用
        pattern = r'`?(\w+)`?\.`?(\w+)`?'
        
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            prefix = match.group(1)
            column = match.group(2)
            # 过滤掉明显不是列名的情况（如 CAST(x AS int)）
            if column.lower() not in ('and', 'or', 'not', 'in', 'is', 'as', 'int', 'text', 'real', 'blob', 'date'):
                references.append((prefix.lower(), column.lower()))
        
        return references
    
    def validate_syntax(self, sql: str) -> Tuple[bool, List[str]]:
        """第一层：语法检查"""
        errors = []
        
        # 1. 括号匹配
        open_parens = sql.count('(')
        close_parens = sql.count(')')
        if open_parens != close_parens:
            errors.append(f"SYNTAX: 括号不匹配 (open: {open_parens}, close: {close_parens})")
        
        # 2. 关键字拼写
        common_typos = {
            'SLECT': 'SELECT',
            'FORM': 'FROM',
            'WEHRE': 'WHERE',
            'GROPU': 'GROUP',
        }
        
        sql_upper = sql.upper()
        for wrong, correct in common_typos.items():
            if f' {wrong} ' in f' {sql_upper} ':
                errors.append(f"SYNTAX: 关键字拼写错误 '{wrong}' -> '{correct}'")
        
        return (len(errors) == 0, errors)
    
    def validate_logic(self, sql: str) -> Tuple[bool, List[str]]:
        """第二层：逻辑检查（支持别名和大小写不敏感）"""
        errors = []
        
        # 第一步：移除子查询 / 集合操作符，使用第一个 SELECT 部分提取表结构
        # 这样可以避免在使用 INTERSECT/UNION/EXCEPT 等情况下，不同部分使用相同别名互相覆盖
        sql_without_subqueries = self._remove_subqueries(sql)

        # 提取别名映射（从完整SQL中提取，包括子查询中的别名）
        self.alias_map = self._extract_table_alias_map(sql)

        # 第二步：提取查询中的表（只从顶层 FROM/JOIN，不包括子查询）
        # 从 FROM 和 JOIN 提取表
        tables_in_query = set()
        
        
        from_pattern = r'FROM\s+`?(\w+)`?'
        from_match = re.search(from_pattern, sql_without_subqueries, re.IGNORECASE)
        if from_match:
            table = from_match.group(1).lower()
            tables_in_query.add(table)
        
        join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?(?:OUTER\s+)?JOIN\s+`?(\w+)`?'
        for match in re.finditer(join_pattern, sql_without_subqueries, re.IGNORECASE):
            table = match.group(1).lower()
            tables_in_query.add(table)
        
        if not tables_in_query:
            errors.append("LOGIC: 无法从 SQL 中提取表名")
            return (False, errors)
        
        # 第三步：检查表是否存在
        for table in tables_in_query:
            if table not in self.formal_schema['Tabs']:
                errors.append(f"LOGIC: 表不存在 '{table}' | 可用表: {', '.join(sorted(self.formal_schema['Tabs']))}")
        
        if errors:
            return (False, errors)
        
        # 第三点五步：检查 subquery 中的列
        subquery_col_errors = self._validate_subquery_columns(sql)
        errors.extend(subquery_col_errors)
        
        if errors:
            return (False, errors)
        
        # 第四步：检查列引用
        col_references = self._extract_table_column_references(sql)
        
        for table_or_alias, column in col_references:
            # 解析表名或别名到真实表
            resolved_table = self._resolve_table_name(table_or_alias)
            
            if resolved_table is None:
                errors.append(f"LOGIC: 表或别名不存在 '{table_or_alias}'")
            else:
                # 检查列是否存在
                if column not in self.formal_schema['Cols'].get(resolved_table, set()):
                    # 检查是否在其他表中存在
                    found_in_other_tables = []
                    for t in self.formal_schema['Tabs']:
                        cols = self.formal_schema['Cols'].get(t.lower(), set())
                        if column in cols and t != resolved_table:
                            found_in_other_tables.append(t)
                    
                    if found_in_other_tables:
                        errors.append(
                            f"LOGIC: Table '{resolved_table}' does not have column '{column}'. "
                            f"This column exists in table(s): {sorted(found_in_other_tables)}."
                        )
                    else:
                        errors.append(
                            f"LOGIC: Table '{resolved_table}' does not have column '{column}'."
                        )
        # 第四点半步：检查未限定列（SELECT 中未使用 table.column 形式的列）
        # 如果用户在 SELECT 中使用未限定列名，我们在 SQLite 环境下尝试判断该列属于哪个表：
        # - 如果在查询表集合中找不到该列 -> 报错
        # - 如果出现在多个表中 -> 发出警告，建议加上表名前缀
        unqualified_errors = self._check_unqualified_columns(sql, tables_in_query)
        errors.extend(unqualified_errors)

        # 第四点七五步：检查WHERE子查询中的列关系
        where_subquery_errors = self._check_where_subquery_column_relationships(sql)
        errors.extend(where_subquery_errors)

        # 第四点八步：检查WHERE语句中的字符串值匹配
        where_value_errors = self._check_where_string_values(sql, tables_in_query)
        errors.extend(where_value_errors)

        # 第五步：检查 JOIN 连通性（只在顶层）- 使用改进的逻辑
        if len(tables_in_query) > 1:
            join_errors = self._check_join_connectivity(sql_without_subqueries, tables_in_query)
            errors.extend(join_errors)
        
        return (len(errors) == 0, errors)
    
    def _check_join_connectivity(self, sql: str, tables: Set[str]) -> List[str]:
        """改进的 JOIN 连通性检查 - 考虑外键、中间表和自定义连接"""
        errors = []
        
        # 提取 JOIN 条件
        join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+)?JOIN\s+`?(\w+)`?.*?ON\s+([^,;]+?)(?=(?:JOIN|WHERE|GROUP|ORDER|LIMIT|$))'
        joins = list(re.finditer(join_pattern, sql, re.IGNORECASE | re.DOTALL))
        
        if not joins:
            return errors
        
        # 获取FROM表
        from_match = re.search(r'FROM\s+`?(\w+)`?', sql, re.IGNORECASE)
        if not from_match:
            return errors
        
        start_table = from_match.group(1).lower()
        all_tables = {start_table}
        
        # 分析每个JOIN
        for join_match in joins:
            join_table = join_match.group(1).lower()
            join_condition = join_match.group(2).strip()
            all_tables.add(join_table)
            
            # 将JOIN条件中的别名替换为真实表名
            condition_with_real_names = join_condition.lower()
            for alias, real_table in self.alias_map.items():
                condition_with_real_names = re.sub(
                    rf'\b{re.escape(alias)}\.(\w+)',
                    rf'{real_table}.\1',
                    condition_with_real_names,
                    flags=re.IGNORECASE
                )
            
            # 检查这个JOIN是否有问题
            join_error = self._analyze_single_join(join_table, condition_with_real_names, all_tables)
            if join_error:
                errors.append(join_error)
        
        return errors
    
    def _analyze_single_join(self, join_table: str, join_condition: str, all_tables: Set[str]) -> str:
        """分析单个JOIN是否合理"""
        
        # 1. 检查是否使用了外键连接
        direct_fk_used = self._check_direct_foreign_key_usage(join_table, join_condition)
        if direct_fk_used:
            return None  # 使用了外键，没问题
        
        # 2. 检查两表间是否有直接外键关系但没有使用
        direct_fk_available = self._find_direct_foreign_keys(join_table, all_tables)
        if direct_fk_available:
            # 进一步分析：是连接条件错了还是没有连接
            wrong_condition_info = self._analyze_wrong_join_condition(join_table, join_condition, direct_fk_available)
            
            if wrong_condition_info:
                return wrong_condition_info
            else:
                # 没有检测到错误条件，给出一般建议
                fk_suggestions = []
                target_tables = set()
                for src_t, src_c, tgt_t, tgt_c in direct_fk_available:
                    fk_suggestions.append(f"{src_t}.{src_c} = {tgt_t}.{tgt_c}")
                    if src_t == join_table:
                        target_tables.add(tgt_t)
                    else:
                        target_tables.add(src_t)
                
                target_list = sorted(target_tables)
                return (f"JOIN_SUGGESTION: Table '{join_table}' and table(s) {target_list} have direct foreign key relationships "
                       f"but the JOIN is not using them. Consider using: {' or '.join(fk_suggestions)}")
        
        # 3. 检查是否可以通过中间表连接
        intermediate_paths = self._find_intermediate_table_paths(join_table, all_tables)
        if intermediate_paths:
            path_suggestions = []
            target_tables = set()
            for path in intermediate_paths[:2]:  # 只显示前2个路径避免过多信息
                path_str = " -> ".join([f"{t1}.{c1} = {t2}.{c2}" for t1, c1, t2, c2 in path])
                path_suggestions.append(path_str)
                # 提取目标表
                for t1, c1, t2, c2 in path:
                    if t1 != join_table:
                        target_tables.add(t1)
                    if t2 != join_table:
                        target_tables.add(t2)
            
            target_list = sorted(target_tables - all_tables)  # 排除已在查询中的表
            return (f"JOIN_VIA_INTERMEDIATE: Table '{join_table}' and table(s) {sorted(all_tables - {join_table})} can be connected via intermediate table(s) {target_list}. "
                   f"Consider using: {' or '.join(path_suggestions)}")
        
        # 4. 如果既没有直接外键也没有中间表路径，允许自定义连接（不报错）
        return None
    
    def _analyze_wrong_join_condition(self, join_table: str, join_condition: str, direct_fks: List[Tuple[str, str, str, str]]) -> str:
        """分析是否是连接条件写错了"""
        
        # 提取JOIN条件中使用的列
        condition_columns = self._extract_join_condition_columns(join_condition)
        
        for src_t, src_c, tgt_t, tgt_c in direct_fks:
            # 检查是否涉及了相关的表和列，但连接条件不正确
            involved_tables = {src_t, tgt_t}
            
            for table_col_pair in condition_columns:
                table_name, col_name = table_col_pair
                
                # 如果条件中涉及了这些表但使用了错误的列
                if table_name in involved_tables:
                    # 检查是否使用了错误的列进行连接
                    correct_fk = f"{src_t}.{src_c} = {tgt_t}.{tgt_c}"
                    other_table = tgt_t if src_t == join_table else src_t
                    
                    return (f"JOIN_WRONG_CONDITION: Table '{join_table}' is being joined with '{other_table}' "
                           f"but using incorrect columns. The correct foreign key relationship is: {correct_fk}")
        
        return None
    
    def _extract_join_condition_columns(self, join_condition: str) -> List[Tuple[str, str]]:
        """从JOIN条件中提取table.column对"""
        columns = []
        
        # 匹配 table.column 模式
        pattern = r'(\w+)\.(\w+)'
        matches = re.findall(pattern, join_condition, re.IGNORECASE)
        
        for table, column in matches:
            columns.append((table.lower(), column.lower()))
        
        return columns
    
    def _check_where_subquery_column_relationships(self, sql: str) -> List[str]:
        """检查WHERE子查询中的列关系"""
        errors = []
        
        # 查找WHERE子查询模式
        # 匹配: WHERE column IN (subquery) 或 WHERE column = (subquery) 或其他比较操作符
        where_subquery_patterns = [
            r'WHERE\s+(\w+(?:\.\w+)?)\s+IN\s*\(\s*(SELECT\s+.*?)\)',
            r'WHERE\s+(\w+(?:\.\w+)?)\s*([=<>!]+)\s*\(\s*(SELECT\s+.*?)\)',
            r'WHERE\s+.*?\s+IN\s*\(\s*SELECT\s+(\w+(?:\.\w+)?)\s+FROM\s+(\w+).*?\)',
            r'WHERE\s+(\w+(?:\.\w+)?)\s+IN\s*\(\s*SELECT\s+(\w+(?:\.\w+)?)\s+FROM\s+(\w+).*?\)'
        ]
        
        # 更精确的WHERE子查询分析
        where_subqueries = self._extract_where_subqueries(sql)
        
        for where_info in where_subqueries:
            error = self._analyze_where_subquery_relationship(where_info)
            if error:
                errors.append(error)
        
        return errors
    
    def _extract_where_subqueries(self, sql: str) -> List[Dict]:
        """提取WHERE子查询信息"""
        subqueries = []
        
        # 匹配 WHERE ... IN (SELECT column FROM table ...)
        pattern1 = r'WHERE\s+(\w+(?:\.\w+)?)\s+IN\s*\(\s*SELECT\s+(\w+(?:\.\w+)?)\s+FROM\s+(\w+)(?:\s+AS\s+\w+)?.*?\)'
        matches1 = re.finditer(pattern1, sql, re.IGNORECASE | re.DOTALL)
        
        for match in matches1:
            where_column = match.group(1).lower()
            select_column = match.group(2).lower()
            from_table = match.group(3).lower()
            
            # 解析WHERE列的表信息
            where_table = None
            if '.' in where_column:
                where_table, where_column = where_column.split('.', 1)
            
            # 解析SELECT列的表信息
            select_table = from_table
            if '.' in select_column:
                select_table, select_column = select_column.split('.', 1)
            
            subqueries.append({
                'type': 'IN',
                'where_column': where_column,
                'where_table': where_table,
                'select_column': select_column, 
                'select_table': select_table,
                'subquery_tables': {from_table}
            })
        
        # 匹配其他比较操作符的子查询
        pattern2 = r'WHERE\s+(\w+(?:\.\w+)?)\s*([=<>!]+)\s*\(\s*SELECT\s+(\w+(?:\.\w+)?)\s+FROM\s+(\w+)(?:\s+AS\s+\w+)?.*?\)'
        matches2 = re.finditer(pattern2, sql, re.IGNORECASE | re.DOTALL)
        
        for match in matches2:
            where_column = match.group(1).lower()
            operator = match.group(2)
            select_column = match.group(3).lower()
            from_table = match.group(4).lower()
            
            # 解析WHERE列的表信息
            where_table = None
            if '.' in where_column:
                where_table, where_column = where_column.split('.', 1)
            
            # 解析SELECT列的表信息
            select_table = from_table
            if '.' in select_column:
                select_table, select_column = select_column.split('.', 1)
            
            subqueries.append({
                'type': f'COMPARE_{operator}',
                'where_column': where_column,
                'where_table': where_table,
                'select_column': select_column,
                'select_table': select_table,
                'subquery_tables': {from_table}
            })
        
        return subqueries
    
    def _analyze_where_subquery_relationship(self, where_info: Dict) -> str:
        """分析WHERE子查询中列之间的关系"""
        
        where_column = where_info['where_column']
        where_table = where_info['where_table']
        select_column = where_info['select_column']
        select_table = where_info['select_table']
        
        # 如果WHERE列没有指定表名，尝试从主查询推断
        if not where_table:
            # 从主查询的FROM和JOIN中推断WHERE列所属的表
            main_query_tables = self._extract_main_query_tables(where_info)
            where_table = self._infer_column_table(where_column, main_query_tables)
        
        if not where_table:
            return None  # 无法推断WHERE列所属表，跳过检查
        
        # 检查两个列是否有直接外键关系
        has_direct_fk = self._check_columns_foreign_key_relationship(
            where_table, where_column, select_table, select_column
        )
        
        if has_direct_fk:
            return None  # 有直接外键关系，没问题
        
        # 检查是否可以通过中间表建立关系
        intermediate_path = self._find_column_intermediate_relationship(
            where_table, where_column, select_table, select_column
        )
        
        if intermediate_path:
            path_str = " -> ".join([f"{t1}.{c1} = {t2}.{c2}" for t1, c1, t2, c2 in intermediate_path])
            return (f"WHERE_SUBQUERY_INDIRECT: Column '{where_table}.{where_column}' and '{select_table}.{select_column}' "
                   f"have no direct relationship but can be connected via: {path_str}")
        
        # 检查两个表之间是否有任何关系
        table_relationship = self._check_table_relationship(where_table, select_table)
        if table_relationship:
            return (f"WHERE_SUBQUERY_SUGGESTION: Column '{where_table}.{where_column}' and '{select_table}.{select_column}' "
                   f"have no direct relationship. Tables '{where_table}' and '{select_table}' are connected via: {table_relationship}")
        
        # 如果没有任何关系，不报错（允许自定义WHERE条件）
        return None
    
    def _extract_main_query_tables(self, where_info: Dict) -> Set[str]:
        """从WHERE信息中推断主查询使用的表"""
        # 这里需要更复杂的逻辑来提取主查询的表
        # 简化处理：返回当前已知的表
        return set()
    
    def _infer_column_table(self, column: str, possible_tables: Set[str]) -> str:
        """推断列所属的表"""
        for table in possible_tables:
            if column in self.formal_schema['Cols'].get(table, set()):
                return table
        
        # 在所有表中查找
        for table in self.formal_schema['Tabs']:
            if column in self.formal_schema['Cols'].get(table, set()):
                return table
        
        return None
    
    def _check_columns_foreign_key_relationship(self, table1: str, col1: str, table2: str, col2: str) -> bool:
        """检查两个列之间是否有直接的外键关系"""
        for src_t, src_c, tgt_t, tgt_c in self.formal_schema['FKs']:
            # 检查正向关系
            if (src_t == table1 and src_c == col1 and tgt_t == table2 and tgt_c == col2) or \
               (src_t == table2 and src_c == col2 and tgt_t == table1 and tgt_c == col1):
                return True
        return False
    
    def _find_column_intermediate_relationship(self, table1: str, col1: str, table2: str, col2: str) -> List[Tuple[str, str, str, str]]:
        """查找两个列通过中间表的连接关系"""
        # 查找table1到其他表的连接
        for intermediate_table in self.formal_schema['Tabs']:
            if intermediate_table in {table1, table2}:
                continue
                
            # 查找table1到intermediate的连接
            connection1 = None
            for src_t, src_c, tgt_t, tgt_c in self.formal_schema['FKs']:
                if (src_t == table1 and tgt_t == intermediate_table) or \
                   (src_t == intermediate_table and tgt_t == table1):
                    connection1 = (src_t, src_c, tgt_t, tgt_c)
                    break
            
            if not connection1:
                continue
                
            # 查找intermediate到table2的连接
            connection2 = None
            for src_t, src_c, tgt_t, tgt_c in self.formal_schema['FKs']:
                if (src_t == intermediate_table and tgt_t == table2) or \
                   (src_t == table2 and tgt_t == intermediate_table):
                    connection2 = (src_t, src_c, tgt_t, tgt_c)
                    break
            
            if connection2:
                return [connection1, connection2]
        
        return []
    
    def _check_table_relationship(self, table1: str, table2: str) -> str:
        """检查两个表之间是否有直接外键关系"""
        for src_t, src_c, tgt_t, tgt_c in self.formal_schema['FKs']:
            if (src_t == table1 and tgt_t == table2) or (src_t == table2 and tgt_t == table1):
                return f"{src_t}.{src_c} = {tgt_t}.{tgt_c}"
        return None
    
    def _check_where_string_values(self, sql: str, tables_in_query: Set[str]) -> List[str]:
        """检查WHERE语句中的字符串值是否匹配数据库中的实际值"""
        errors = []
        similarity_threshold = 0.6  # 相似度阈值
        
        # 提取WHERE语句中的字符串条件（包括子查询）
        where_conditions = self._extract_where_string_conditions(sql)
        
        # 扩展表集合，包括子查询中的表
        all_available_tables = self._extract_all_tables_from_sql(sql)
        
        for condition in where_conditions:
            column_ref = condition['column']
            value = condition['value']
            operator = condition['operator']
            source_sql = condition.get('source_sql', sql)
            
            # 解析列引用
            if '.' in column_ref:
                table_part, column_name = column_ref.split('.', 1)
                # 解析表名或别名
                resolved_table = self._resolve_table_name(table_part)
            else:
                # 未限定的列名，尝试在所有可用表中查找，考虑上下文
                column_name = column_ref
                resolved_table = self._find_column_in_tables_with_context(column_name, all_available_tables, source_sql)
            
            if not resolved_table or resolved_table not in self.formal_schema['Tabs']:
                continue  # 表不存在的错误会在其他地方处理
            
            if column_name not in self.formal_schema['Cols'].get(resolved_table, set()):
                continue  # 列不存在的错误会在其他地方处理
            
            # 获取该列的实际值并进行匹配检查
            match_result = self._check_column_value_match(resolved_table, column_name, value, similarity_threshold)
            
            if match_result:
                errors.append(match_result)
        
        return errors
    
    def _extract_all_tables_from_sql(self, sql: str) -> Set[str]:
        """从SQL中提取所有表名，包括主查询和嵌套子查询"""
        all_tables = set()
        
        # 递归提取所有层级的表
        self._extract_tables_recursive(sql, all_tables)
        
        return all_tables
    
    def _extract_tables_recursive(self, sql: str, all_tables: Set[str]):
        """递归提取表名"""
        # 提取当前查询的表
        current_tables = self._extract_tables_from_query(sql)
        all_tables.update(current_tables)
        
        # 递归处理所有子查询
        subquery_pattern = r'\(\s*(SELECT\s+.*?)\)'
        
        # 使用平衡括号匹配来正确提取嵌套子查询
        pos = 0
        while pos < len(sql):
            # 查找下一个 (SELECT
            select_start = sql.find('(', pos)
            if select_start == -1:
                break
            
            # 检查是否是SELECT子查询
            select_match = re.match(r'\s*SELECT\s+', sql[select_start + 1:], re.IGNORECASE)
            if not select_match:
                pos = select_start + 1
                continue
            
            # 找到匹配的右括号
            paren_count = 1
            subquery_end = select_start + 1
            while subquery_end < len(sql) and paren_count > 0:
                if sql[subquery_end] == '(':
                    paren_count += 1
                elif sql[subquery_end] == ')':
                    paren_count -= 1
                subquery_end += 1
            
            if paren_count == 0:
                # 提取子查询内容
                subquery_content = sql[select_start + 1:subquery_end - 1].strip()
                # 递归处理子查询
                self._extract_tables_recursive(subquery_content, all_tables)
            
            pos = subquery_end
    
    def _extract_tables_from_query(self, sql: str) -> Set[str]:
        """从单个查询中提取表名"""
        tables = set()
        
        # FROM表
        from_pattern = r'FROM\s+`?(\w+)`?'
        from_match = re.search(from_pattern, sql, re.IGNORECASE)
        if from_match:
            tables.add(from_match.group(1).lower())
        
        # JOIN表
        join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?(?:OUTER\s+)?JOIN\s+`?(\w+)`?'
        for match in re.finditer(join_pattern, sql, re.IGNORECASE):
            tables.add(match.group(1).lower())
        
        return tables
    
    def _extract_where_string_conditions(self, sql: str) -> List[Dict]:
        """提取WHERE语句中的字符串条件，包括主查询和嵌套子查询"""
        conditions = []
        
        # 递归提取所有层级的WHERE条件
        self._extract_where_conditions_recursive(sql, conditions)
        
        return conditions
    
    def _extract_where_conditions_recursive(self, sql: str, conditions: List[Dict]):
        """递归提取WHERE条件"""
        # 提取当前查询的WHERE条件
        current_conditions = self._extract_where_conditions_from_query(sql)
        conditions.extend(current_conditions)
        
        # 递归处理所有子查询
        pos = 0
        while pos < len(sql):
            # 查找下一个 (SELECT
            select_start = sql.find('(', pos)
            if select_start == -1:
                break
            
            # 检查是否是SELECT子查询
            select_match = re.match(r'\s*SELECT\s+', sql[select_start + 1:], re.IGNORECASE)
            if not select_match:
                pos = select_start + 1
                continue
            
            # 找到匹配的右括号
            paren_count = 1
            subquery_end = select_start + 1
            while subquery_end < len(sql) and paren_count > 0:
                if sql[subquery_end] == '(':
                    paren_count += 1
                elif sql[subquery_end] == ')':
                    paren_count -= 1
                subquery_end += 1
            
            if paren_count == 0:
                # 提取子查询内容
                subquery_content = sql[select_start + 1:subquery_end - 1].strip()
                # 递归处理子查询
                self._extract_where_conditions_recursive(subquery_content, conditions)
            
            pos = subquery_end
    
    def _extract_where_conditions_from_query(self, sql: str) -> List[Dict]:
        """从单个查询中提取WHERE条件"""
        conditions = []
        
        # WHERE子句匹配模式
        where_pattern = r'WHERE\s+(.*?)(?:\s+(?:GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT)\s|;|\s*$)'
        where_match = re.search(where_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not where_match:
            return conditions
        
        where_clause = where_match.group(1).strip()
        
        # 匹配字符串比较条件 (column = 'value', column LIKE 'value', etc.)
        string_condition_patterns = [
            r"(\w+(?:\.\w+)?)\s*(=|!=|<>|LIKE|ILIKE)\s*'([^']*)'",
            r'(\w+(?:\.\w+)?)\s*(=|!=|<>|LIKE|ILIKE)\s*"([^"]*)"'
        ]
        
        for pattern in string_condition_patterns:
            matches = re.finditer(pattern, where_clause, re.IGNORECASE)
            for match in matches:
                conditions.append({
                    'column': match.group(1).lower(),
                    'operator': match.group(2).upper(),
                    'value': match.group(3),
                    'source_sql': sql  # 记录来源SQL，用于调试
                })
        
        return conditions
    
    def _find_column_in_tables(self, column_name: str, tables: Set[str]) -> str:
        """在给定表集合中查找列"""
        matching_tables = []
        for table in tables:
            if column_name in self.formal_schema['Cols'].get(table, set()):
                matching_tables.append(table)
        
        if len(matching_tables) == 1:
            return matching_tables[0]
        elif len(matching_tables) > 1:
            # 如果有多个表包含该列，优先选择具有更多匹配列的表
            # 或者基于表名字典序选择（确保一致性）
            return sorted(matching_tables)[0]
        
        return None
    
    def _find_column_in_tables_with_context(self, column_name: str, tables: Set[str], source_sql: str) -> str:
        """在给定表集合中查找列，考虑SQL上下文"""
        matching_tables = []
        for table in tables:
            if column_name in self.formal_schema['Cols'].get(table, set()):
                matching_tables.append(table)
        
        if len(matching_tables) <= 1:
            return matching_tables[0] if matching_tables else None
        
        # 如果有多个匹配的表，尝试从SQL上下文中推断正确的表
        source_sql_lower = source_sql.lower()
        
        # 检查SQL中是否明确提到某个表
        for table in matching_tables:
            if f'from {table}' in source_sql_lower or f'join {table}' in source_sql_lower:
                return table
        
        # 如果无法从上下文推断，返回字典序第一个（确保一致性）
        return sorted(matching_tables)[0]
    
    def _check_column_value_match(self, table: str, column: str, search_value: str, threshold: float) -> str:
        """检查列值匹配并提供建议"""
        try:
            cursor = self.conn.cursor()
            # 获取该列的不重复值
            cursor.execute(f"SELECT DISTINCT `{column}` FROM `{table}` WHERE `{column}` IS NOT NULL LIMIT 100")
            actual_values = [str(row[0]) for row in cursor.fetchall()]
            
            if not actual_values:
                return None  # 列为空，无法验证
            
            # 转换为小写进行比较
            search_value_lower = search_value.lower()
            actual_values_lower = [v.lower() for v in actual_values]
            
            # 1. 检查完全匹配（忽略大小写）
            exact_match = None
            for i, val_lower in enumerate(actual_values_lower):
                if val_lower == search_value_lower:
                    exact_match = actual_values[i]
                    break
            
            if exact_match:
                if exact_match != search_value:
                    # 大小写不匹配
                    return (f"WHERE_VALUE_CASE_MISMATCH: Value '{search_value}' in column '{table}.{column}' "
                           f"should be '{exact_match}' (case-sensitive match found)")
                else:
                    # 完全匹配，没有错误
                    return None
            
            # 2. 检查相似度匹配
            best_matches = []
            for i, val_lower in enumerate(actual_values_lower):
                similarity = SequenceMatcher(None, search_value_lower, val_lower).ratio()
                if similarity >= threshold:
                    best_matches.append((actual_values[i], similarity))
            
            if best_matches:
                # 按相似度排序
                best_matches.sort(key=lambda x: x[1], reverse=True)
                top_matches = [match[0] for match in best_matches[:3]]
                
                return (f"WHERE_VALUE_SIMILARITY: Value '{search_value}' in column '{table}.{column}' "
                       f"not found. Similar values: {', '.join(repr(m) for m in top_matches)}")
            
            # 3. 提供样本值
            sample_values = actual_values[:5]
            return (f"WHERE_VALUE_SAMPLES: Value '{search_value}' in column '{table}.{column}' "
                   f"not found. Sample values from this column: {', '.join(repr(v) for v in sample_values)}. "
                   f"Consider checking the exact spelling or case sensitivity.")
            
        except Exception as e:
            # 数据库查询失败，跳过检查
            return None
    
    def _check_direct_foreign_key_usage(self, join_table: str, join_condition: str) -> bool:
        """检查JOIN条件是否使用了外键"""
        for src_t, src_c, tgt_t, tgt_c in self.formal_schema['FKs']:
            # 检查正向和反向的外键使用
            pattern1 = rf'{re.escape(src_t)}\.{re.escape(src_c)}\s*=\s*{re.escape(tgt_t)}\.{re.escape(tgt_c)}'
            pattern2 = rf'{re.escape(tgt_t)}\.{re.escape(tgt_c)}\s*=\s*{re.escape(src_t)}\.{re.escape(src_c)}'
            
            if re.search(pattern1, join_condition, re.IGNORECASE) or \
               re.search(pattern2, join_condition, re.IGNORECASE):
                if src_t == join_table or tgt_t == join_table:
                    return True
        return False
    
    def _find_direct_foreign_keys(self, join_table: str, all_tables: Set[str]) -> List[Tuple[str, str, str, str]]:
        """查找join_table与all_tables中其他表的直接外键关系"""
        direct_fks = []
        other_tables = all_tables - {join_table}
        
        for src_t, src_c, tgt_t, tgt_c in self.formal_schema['FKs']:
            if (src_t == join_table and tgt_t in other_tables) or \
               (tgt_t == join_table and src_t in other_tables):
                direct_fks.append((src_t, src_c, tgt_t, tgt_c))
        
        return direct_fks
    
    def _find_intermediate_table_paths(self, join_table: str, all_tables: Set[str]) -> List[List[Tuple[str, str, str, str]]]:
        """查找通过中间表连接的路径"""
        paths = []
        other_tables = all_tables - {join_table}
        available_intermediates = self.formal_schema['Tabs'] - all_tables
        
        # 查找单跳中间表路径
        for intermediate in available_intermediates:
            for target_table in other_tables:
                path = self._find_path_via_intermediate(join_table, target_table, intermediate)
                if path:
                    paths.append(path)
        
        return paths
    
    def _find_path_via_intermediate(self, table1: str, table2: str, intermediate: str) -> List[Tuple[str, str, str, str]]:
        """查找table1 -> intermediate -> table2的路径"""
        path = []
        
        # 查找table1到intermediate的连接
        connection1 = None
        for src_t, src_c, tgt_t, tgt_c in self.formal_schema['FKs']:
            if (src_t == table1 and tgt_t == intermediate) or \
               (src_t == intermediate and tgt_t == table1):
                connection1 = (src_t, src_c, tgt_t, tgt_c)
                break
        
        # 查找intermediate到table2的连接
        connection2 = None
        for src_t, src_c, tgt_t, tgt_c in self.formal_schema['FKs']:
            if (src_t == intermediate and tgt_t == table2) or \
               (src_t == table2 and tgt_t == intermediate):
                connection2 = (src_t, src_c, tgt_t, tgt_c)
                break
        
        if connection1 and connection2:
            return [connection1, connection2]
        
        return []
    
    def validate_execution(self, sql: str) -> Tuple[bool, List[str], str]:
        """第三层：执行检查
        返回: (passed, errors, execution_status)
        execution_status: 'success', 'empty', 'failed'
        """
        errors = []
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            
            # 检查结果是否为空 - 但不视为错误，只是记录状态
            if not result:
                # 不再将空结果视为错误，只记录信息
                return (True, ["INFO: Query executed successfully but returned no results (empty result set)"], 'empty')
            
            return (True, [], 'success')
        except sqlite3.OperationalError as e:
            errors.append(f"EXECUTION: {str(e)}")
            return (False, errors, 'failed')
        except Exception as e:
            errors.append(f"EXECUTION: 未知错误 - {str(e)}")
            return (False, errors, 'failed')
    
    def validate_distinct(self, sql: str) -> Tuple[bool, List[str]]:
        """第四层：DISTINCT检查 - 检查查询结果是否有重复行
        返回: (passed, errors)
        """
        errors = []
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            
            if not result:
                # 空结果不需要检查重复
                return (True, [])
            
            # 检查是否有重复行
            unique_rows = set(result)
            total_rows = len(result)
            unique_count = len(unique_rows)
            
            if total_rows > unique_count:
                duplicate_count = total_rows - unique_count
                errors.append(
                    f"DISTINCT_NEEDED: Query returned {total_rows} rows but only {unique_count} are unique. "
                    f"{duplicate_count} duplicate rows found. Consider adding DISTINCT after SELECT if duplicates are not desired."
                )
                return (False, errors)
            
            return (True, [])
            
        except Exception as e:
            # 如果执行出错，不进行DISTINCT检查（前面的执行验证会捕获这些错误）
            return (True, [])


    def _check_unqualified_columns(self, sql: str, tables_in_query: Set[str]) -> List[str]:
        """
        检查 SELECT 列表中未限定的列名（没有 table.column 形式）在给定查询表集合中的存在性与歧义性。
        返回错误/警告列表。
        """
        messages = []

        # 提取 SELECT ... FROM 之间的字段列表（简单启发式）
        sel_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not sel_match:
            return messages

        select_list = sel_match.group(1)
        # 拆分逗号分隔的字段表达式（注意：这是启发式，复杂表达式可能包含逗号）
        parts = [p.strip() for p in re.split(r',\s*(?![^()]*\))', select_list) if p.strip()]

        # SQL keywords and functions to skip (do not treat as column candidates)
        sql_keywords = {
            'DISTINCT', 'COUNT', 'SUM', 'MAX', 'MIN', 'AVG', 'GROUP_CONCAT',
            'ABS', 'ROUND', 'LENGTH', 'UPPER', 'LOWER', 'SUBSTR', 'CAST',
            'COALESCE', 'IFNULL', 'NULLIF', 'CASE', 'WHEN', 'THEN', 'ELSE',
            'AS', 'DISTINCT'
        }

        for part in parts:
            # If contains dot, it's already qualified -> skip
            if '.' in part:
                continue

            # Remove trailing alias if present: "... AS alias" or "... alias"
            part_clean = re.sub(r"\s+(?:AS\s+)?`?([A-Za-z_]\w*)`?\s*$", "", part, flags=re.IGNORECASE)

            # If the cleaned part is just '*' or contains '*' (COUNT(*)) we skip
            if part_clean.strip() == '*' or '*' in part_clean:
                continue

            # Extract identifier candidates from the cleaned part
            col_candidates = re.findall(r"\b([A-Za-z_]\w*)\b", part_clean)

            for col_raw in col_candidates:
                col = col_raw.lower()

                # Skip SQL keywords and function names
                if col.upper() in sql_keywords:
                    continue

                # Skip numeric literals
                if col.isdigit():
                    continue

                found_in = []
                for t in tables_in_query:
                    cols = self.formal_schema['Cols'].get(t.lower(), set())
                    if col in cols:
                        found_in.append(t)

                if len(found_in) == 0:
                    # Check if this column exists in other tables (not in query tables)
                    found_in_other_tables = []
                    for t in self.formal_schema['Tabs']:
                        cols = self.formal_schema['Cols'].get(t.lower(), set())
                        if col in cols and t not in tables_in_query:
                            found_in_other_tables.append(t)

                    if found_in_other_tables:
                        messages.append(
                            f"UNQUALIFIED_COLUMN_ERROR: Column '{col}' not found in table(s) {sorted(tables_in_query)}. "
                            f"This column exists in table(s): {sorted(found_in_other_tables)}."
                        )
                    else:
                        messages.append(
                            f"UNQUALIFIED_COLUMN_ERROR: Column '{col}' not found in any of the query table(s) {sorted(tables_in_query)}."
                        )
                elif len(found_in) > 1:
                    messages.append(
                        f"UNQUALIFIED_COLUMN_AMBIGUOUS: Column '{col}' appears in multiple tables {sorted(found_in)}. Consider qualifying with table name or alias."
                    )
        return messages
    def validate_comprehensive(self, sql: str) -> Dict[str, any]:
        """完整验证"""
        # 重置别名映射
        self.alias_map = {}

        result = {
            'sql': sql,
            'stage1_syntax': {'passed': False, 'errors': []},
            'stage2_logic': {'passed': False, 'errors': []},
            'stage3_execution': {'passed': False, 'errors': [], 'status': ''},
            'stage4_distinct': {'passed': False, 'errors': []},
            'overall_passed': False,
            'error_summary': ''
        }

        # Stage 1: 语法
        syntax_passed, syntax_errors = self.validate_syntax(sql)
        result['stage1_syntax'] = {'passed': syntax_passed, 'errors': syntax_errors}

        if not syntax_passed:
            result['error_summary'] = '语法错误'
            return result

        # Stage 2: 逻辑
        logic_passed, logic_errors = self.validate_logic(sql)
        result['stage2_logic'] = {'passed': logic_passed, 'errors': logic_errors}

        if not logic_passed:
            result['error_summary'] = '逻辑错误'
            return result

        # Stage 3: 执行
        exec_passed, exec_errors, exec_status = self.validate_execution(sql)
        result['stage3_execution'] = {'passed': exec_passed, 'errors': exec_errors, 'status': exec_status}

        # 只有真正的执行错误（failed）才视为失败，空结果（empty）不视为错误
        if not exec_passed and exec_status == 'failed':
            result['error_summary'] = '执行错误'
            return result

        # Stage 4: DISTINCT检查 - 只有前面所有阶段都通过且有结果时才执行
        distinct_passed = True
        distinct_errors = []
        if exec_status == 'success':  # 只有成功执行且有结果时才检查DISTINCT
            distinct_passed, distinct_errors = self.validate_distinct(sql)
            result['stage4_distinct'] = {'passed': distinct_passed, 'errors': distinct_errors}
            
            if not distinct_passed:
                result['error_summary'] = 'DISTINCT相关建议'
                result['overall_passed'] = False  # DISTINCT问题也算作需要修复的问题
                return result
        else:
            # 如果执行结果为空或失败，跳过DISTINCT检查
            result['stage4_distinct'] = {'passed': True, 'errors': []}

        # 所有阶段都通过
        result['overall_passed'] = True
        if exec_status == 'empty':
            result['error_summary'] = '执行结果为空（但SQL有效）'
        else:
            result['error_summary'] = ''

        return result
    
    def get_natural_error_description(self, validation_result: Dict[str, any]) -> str:
        """
        Generate a natural language description of validation errors that LLMs can understand.
        Returns a formatted string suitable for augmenting the LLM prompt.
        """
        if validation_result.get('overall_passed', False):
            return ""
        
        errors_text = []
        
        # Collect errors from all stages
        all_errors = []
        for stage in ('stage1_syntax', 'stage2_logic', 'stage3_execution', 'stage4_distinct'):
            stage_res = validation_result.get(stage, {})
            if not stage_res.get('passed', True):
                stage_errors = stage_res.get('errors', [])
                # 特殊处理DISTINCT错误
                if stage == 'stage4_distinct' and stage_errors:
                    for error in stage_errors:
                        if 'DISTINCT_NEEDED' in error:
                            # 转换为更适合LLM理解的提示
                            distinct_error = ("The query result contains duplicate rows. "
                                            "Based on the natural language question, determine if DISTINCT "
                                            "should be added after SELECT to remove duplicates.")
                            all_errors.append(distinct_error)
                        else:
                            all_errors.append(error)
                else:
                    all_errors.extend(stage_errors)
        
        if not all_errors:
            return "The SQL query is invalid but the specific errors could not be determined."
        
        # Build natural description
        result_lines = [
            "The SQL query you generated has validation errors:",
            ""
        ]
        
        for i, error in enumerate(all_errors, 1):
            result_lines.append(f"  {i}. {error}")
        
        result_lines.extend([
            "",
            # Keep the error description concise for LLMs. Higher-level instructions are handled by the wrapper.
        ])
        
        return "\n".join(result_lines)
    
    def auto_fix_sql(self, sql: str) -> Tuple[str, List[str], bool]:
        """
        尝试自动修复SQL中的错误
        返回: (修复后的SQL, 修复操作列表, 是否需要LLM介入)
        """
        original_sql = sql
        fixed_sql = sql
        fix_operations = []
        needs_llm = False
        
        # 验证原始SQL
        result = self.validate_comprehensive(sql)
        if result.get('overall_passed', False):
            return fixed_sql, ['No fixes needed - SQL is valid'], False
        
        # 收集所有错误
        all_errors = []
        for stage in ('stage1_syntax', 'stage2_logic', 'stage3_execution', 'stage4_distinct'):
            stage_res = result.get(stage, {})
            if not stage_res.get('passed', True):
                all_errors.extend(stage_res.get('errors', []))
        
        # 尝试修复每个错误
        for error in all_errors:
            old_sql = fixed_sql
            fixed_sql, operation, requires_llm = self._fix_single_error(fixed_sql, error)
            if old_sql != fixed_sql:
                fix_operations.append(operation)
            if requires_llm:
                needs_llm = True
        
        # 如果有修复，验证修复后的SQL
        if fixed_sql != original_sql:
            new_result = self.validate_comprehensive(fixed_sql)
            if new_result.get('overall_passed', False):
                fix_operations.append("✓ Auto-fix successful - SQL now valid")
                return fixed_sql, fix_operations, False
            else:
                # 修复后仍有错误，需要LLM
                fix_operations.append("⚠️ Partial fix applied, but errors remain")
                needs_llm = True
        
        return fixed_sql, fix_operations, needs_llm
    
    def _fix_single_error(self, sql: str, error: str) -> Tuple[str, str, bool]:
        """
        尝试修复单个错误
        返回: (修复后的SQL, 修复操作描述, 是否需要LLM)
        """
        original_sql = sql
        
        # 1. 修复括号不匹配 - 可以自动修复
        if "括号不匹配" in error:
            sql, operation = self._fix_parentheses_mismatch(sql, error)
            return sql, operation, False
        
        # 2. 修复关键字拼写错误 - 可以自动修复
        if "关键字拼写错误" in error:
            sql, operation = self._fix_keyword_typo(sql, error)
            return sql, operation, False
        
        # 3. 修复WHERE值大小写错误 - 可以自动修复
        if "WHERE_VALUE_CASE_MISMATCH" in error:
            sql, operation = self._fix_where_value_case(sql, error)
            return sql, operation, False
        
        # 4. 修复WHERE值相似性错误 - 可以自动修复
        if "WHERE_VALUE_SIMILARITY" in error:
            sql, operation = self._fix_where_value_similarity(sql, error)
            return sql, operation, False
        
        # 5. 修复简单的未限定列名错误 - 部分可以自动修复
        if "UNQUALIFIED_COLUMN_ERROR" in error:
            sql, operation, needs_llm = self._fix_unqualified_column(sql, error)
            return sql, operation, needs_llm
        
        # 6. 修复DISTINCT问题 - 可以自动修复
        if "DISTINCT_NEEDED" in error:
            sql, operation = self._fix_distinct_needed(sql, error)
            return sql, operation, False
        
        # 7. 复杂错误需要LLM处理
        if any(keyword in error for keyword in [
            "LOGIC: 表不存在", "LOGIC: Table", "does not exist",
            "JOIN_SUGGESTION", "JOIN_VIA_INTERMEDIATE", "JOIN_WRONG_CONDITION",
            "WHERE_SUBQUERY", "EXECUTION:"
        ]):
            return sql, f"Complex error requires LLM: {error[:100]}...", True
        
        return sql, f"Unable to auto-fix: {error}", True
    
    def _fix_parentheses_mismatch(self, sql: str, error: str) -> Tuple[str, str]:
        """修复括号不匹配"""
        open_count = sql.count('(')
        close_count = sql.count(')')
        
        if open_count > close_count:
            # 缺少右括号
            fixed_sql = sql + ')' * (open_count - close_count)
            return fixed_sql, f"Added {open_count - close_count} missing closing parenthesis"
        elif close_count > open_count:
            # 多余的右括号，从末尾移除
            excess = close_count - open_count
            fixed_sql = sql
            for _ in range(excess):
                idx = fixed_sql.rfind(')')
                if idx != -1:
                    fixed_sql = fixed_sql[:idx] + fixed_sql[idx+1:]
            return fixed_sql, f"Removed {excess} excess closing parenthesis"
        
        return sql, "No parentheses fix needed"
    
    def _fix_keyword_typo(self, sql: str, error: str) -> Tuple[str, str]:
        """修复关键字拼写错误"""
        # 从错误信息中提取错误和正确的关键字
        typo_match = re.search(r"'(\w+)' -> '(\w+)'", error)
        if typo_match:
            wrong_word = typo_match.group(1)
            correct_word = typo_match.group(2)
            
            # 替换错误的关键字
            pattern = rf'\b{re.escape(wrong_word)}\b'
            fixed_sql = re.sub(pattern, correct_word, sql, flags=re.IGNORECASE)
            return fixed_sql, f"Fixed keyword typo: {wrong_word} -> {correct_word}"
        
        return sql, f"Cannot parse keyword error: {error}"
    
    def _fix_where_value_case(self, sql: str, error: str) -> Tuple[str, str]:
        """修复WHERE值大小写错误"""
        # 解析错误信息
        match = re.search(r"Value '([^']+)'.*should be '([^']+)'", error)
        if match:
            wrong_value = match.group(1)
            correct_value = match.group(2)
            
            # 在SQL中替换值
            pattern = rf"'{re.escape(wrong_value)}'"
            replacement = f"'{correct_value}'"
            
            if re.search(pattern, sql, re.IGNORECASE):
                fixed_sql = re.sub(pattern, replacement, sql)
                return fixed_sql, f"Fixed case mismatch: '{wrong_value}' -> '{correct_value}'"
        
        return sql, f"Cannot parse case mismatch error: {error}"
    
    def _fix_where_value_similarity(self, sql: str, error: str) -> Tuple[str, str]:
        """修复WHERE值相似性错误"""
        # 解析错误信息
        wrong_match = re.search(r"Value '([^']+)'", error)
        similar_match = re.search(r"Similar values: ([^.]+)", error)
        
        if wrong_match and similar_match:
            wrong_value = wrong_match.group(1)
            similar_values_str = similar_match.group(1)
            
            # 解析相似值列表，提取第一个相似值
            similar_values = re.findall(r"'([^']*)'", similar_values_str)
            
            if similar_values:
                best_match = similar_values[0]
                
                # 在SQL中替换
                pattern = rf"'{re.escape(wrong_value)}'"
                replacement = f"'{best_match}'"
                
                if re.search(pattern, sql, re.IGNORECASE):
                    fixed_sql = re.sub(pattern, replacement, sql)
                    return fixed_sql, f"Replaced with similar value: '{wrong_value}' -> '{best_match}'"
        
        return sql, f"Cannot parse similarity error: {error}"
    
    def _fix_unqualified_column(self, sql: str, error: str) -> Tuple[str, str, bool]:
        """修复未限定列名错误"""
        # 解析错误信息
        if "This column exists in table(s):" in error:
            column_match = re.search(r"Column '([^']+)'", error)
            table_match = re.search(r"This column exists in table\(s\): \[([^\]]+)\]", error)
            
            if column_match and table_match:
                column_name = column_match.group(1)
                tables_str = table_match.group(1).replace("'", "").replace(" ", "")
                tables = [t.strip() for t in tables_str.split(',')]
                
                if len(tables) == 1:
                    # 只有一个表包含此列，可以自动修复
                    suggested_table = tables[0]
                    
                    # 在SELECT中查找未限定的列名并加上表前缀
                    pattern = rf'\b{re.escape(column_name)}\b(?!\s*\.)'
                    replacement = f'{suggested_table}.{column_name}'
                    
                    if re.search(pattern, sql, re.IGNORECASE):
                        fixed_sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
                        return fixed_sql, f"Added table qualifier: {column_name} -> {suggested_table}.{column_name}", False
                else:
                    # 多个表包含此列，需要LLM判断
                    return sql, f"Ambiguous column '{column_name}' in tables {tables} - needs LLM", True
        
        return sql, f"Cannot parse unqualified column error: {error}", True
    
    def _fix_distinct_needed(self, sql: str, error: str) -> Tuple[str, str]:
        """修复DISTINCT需要的问题 - 在SELECT后添加DISTINCT"""
        # 在SELECT后添加DISTINCT
        pattern = r'\bSELECT\s+'
        replacement = 'SELECT DISTINCT '
        
        # 检查是否已经有DISTINCT
        if re.search(r'\bSELECT\s+DISTINCT\b', sql, re.IGNORECASE):
            return sql, "DISTINCT already present in query"
        
        if re.search(pattern, sql, re.IGNORECASE):
            fixed_sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
            return fixed_sql, "Added DISTINCT to remove duplicate rows"
        else:
            return sql, "Cannot find SELECT clause to add DISTINCT"


def main():
    db_path = r'C:\Users\grizz\Downloads\postsqlfix-1BD2\database\spider\car_1\car_1.sqlite'
    schema_sql_path = r'C:\Users\grizz\Downloads\postsqlfix-1BD2\database\spider\car_1\car_1.sql'
    
    # 测试用例 - 包含可自动修复的错误
    test_sqls = [
        # 1. 正确的 SQL
        "SELECT T1.ContId, T1.Continent FROM continents AS T1",
        
        # 2. 错误的 SQL (别名识别问题)
        "SELECT T1.ContId, T1.Continent, count(*) FROM continents AS T1 JOIN countries AS T2 ON T1.ContId = T2.Continent GROUP BY T1.ContId",
        
        # 3. 大小写测试
        "select t1.contid from CONTINENTS as t1",
        
        # 4. 可自动修复的错误
        "SLECT T1.ContId FROM continents AS T1",  # 关键字拼写错误
        "SELECT T1.ContId FROM continents AS T1 WHERE (T1.ContId = 1",  # 括号不匹配
        "SELECT ContId FROM continents",  # 未限定列名（假设只在一个表中存在）
        
        # 5. DISTINCT测试 - 这个查询可能返回重复行
        "SELECT T2.Continent FROM continents AS T1 JOIN countries AS T2 ON T1.ContId = T2.Continent",  # 可能有重复的大洲名
    ]
    
    validator = ImprovedSQLValidator(db_path, schema_sql_path)
    print("\n" + "="*80)
    print("SQL 验证和自动修复测试")
    print("="*80 + "\n")
    
    for i, sql in enumerate(test_sqls, 1):
        print(f"[{i}] 测试 SQL: {sql}")
        
        # 尝试自动修复
        fixed_sql, fix_operations, needs_llm = validator.auto_fix_sql(sql)
        
        print(f"    修复后 SQL: {fixed_sql}")
        print(f"    修复操作:")
        for j, operation in enumerate(fix_operations, 1):
            print(f"      {j}. {operation}")
        print(f"    需要 LLM: {'是' if needs_llm else '否'}")
        
        # 显示修复效果
        if fixed_sql != sql:
            result = validator.validate_comprehensive(fixed_sql)
            status = "✓ 有效" if result['overall_passed'] else "✗ 仍有错误"
            print(f"    最终状态: {status}")
        else:
            print(f"    最终状态: 无修复")
        
        print()


def test_auto_fix_showcase():
    """展示自动修复功能的各种场景"""
    db_path = r'C:\Users\grizz\Downloads\postsqlfix-1BD2\database\spider\car_1\car_1.sqlite'
    schema_sql_path = r'C:\Users\grizz\Downloads\postsqlfix-1BD2\database\spider\car_1\car_1.sql'
    
    validator = ImprovedSQLValidator(db_path, schema_sql_path)
    
    print("\n" + "="*80)
    print("自动修复功能展示")
    print("="*80 + "\n")
    
    # 各类可自动修复的错误
    test_cases = [
        ("拼写错误", "SLECT ContId FROM continents"),
        ("括号不匹配", "SELECT ContId FROM continents WHERE (ContId = 1"),
        ("多余括号", "SELECT ContId FROM continents WHERE ContId = 1))"),
        ("DISTINCT需要", "SELECT Continent FROM countries"),  # 假设这个查询会返回重复的大洲
        # 注意：以下测试需要实际的数据库连接和数据
        # ("大小写错误", "SELECT ContId FROM continents WHERE ContId = 'usa'"),  # 假设实际值是 'USA'
        # ("相似值", "SELECT ContId FROM continents WHERE Continent = 'america'"),  # 假设实际值是 'America'
    ]
    
    for category, sql in test_cases:
        print(f"🔧 {category}: {sql}")
        fixed_sql, operations, needs_llm = validator.auto_fix_sql(sql)
        print(f"   修复结果: {fixed_sql}")
        for op in operations:
            print(f"   - {op}")
        print(f"   需要LLM: {'是' if needs_llm else '否'}")
        print()


if __name__ == '__main__':
    main()
    # test_auto_fix_showcase()  # 取消注释来展示自动修复功能
