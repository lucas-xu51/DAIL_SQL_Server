import os
import json
import tqdm
import sqlite3
import csv
from typing import List, Dict, Tuple, Set, Optional, Union
from prompt_bank import dummy_sql_prompt, sr_examples, generate_sr, sr2sql
from llm2 import collect_response2          
from cal_token import check_token
from correct_sql_using_schema import correct_sql_using_schema
from extract.join_conditions import extract_join_conditions
from extract.where_condition import extract_where_conditions
from extract.groupby_condition import extract_groupby_conditions, extract_orderby_conditions
from extract.aliases import extract_aliases
from extract.columns import extract_columns
from extract.extract_table import extract_tables

from extract.struct import get_all_columns, get_group_by_columns, get_order_by_columns
from extract.where import get_where_structure
from extract.having import get_having_structure
from extract.subquery import get_subqueries

from collections import deque # For BFS queue used in connectivity check
from sqlparse.sql import Identifier, Function, TokenList, Comment
from sqlparse.tokens import Keyword, Name, Punctuation
from sql_metadata import Parser
import sqlparse

from connect_sql.conn_postgresql import execute_mysql_query_safe

db_root_path = '/TA-SQL/data/dev_databases'
# db_root_path = '/TA-SQL/data/spider/database'

models = 'qwen-7b'
T = 0.9

class BaseModule():
    def __init__(self, db_root_path, mode):
        self.db_root_path = db_root_path
        self.mode = mode
        table_json_path = os.path.join(db_root_path, f'{mode}_tables.json')
        question_path = os.path.join(db_root_path, f'{mode}.json')
        self.table_json = json.load(open(table_json_path, 'r'))
        self.question_json = json.load(open(question_path, 'r'))
        # self.csv_info, self.value_prompts = self._get_info_from_csv()
    
    def _get_info_from_csv(self):
        csv_info = {}
        value_prompt = {}
        for i in tqdm.tqdm(range(len(self.table_json))):
            table_info = self.table_json[i]
            db_id = table_info['db_id']
            db_path = os.path.join(self.db_root_path, db_id, f'{db_id}.sqlite')
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            csv_dir = os.path.join(self.db_root_path,db_id,'database_description')
            otn_list = table_info['table_names_original']
            tn_list = table_info['table_names']
            for otn,tn in zip(otn_list, tn_list):
                if os.path.exists(os.path.join(csv_dir, f"{tn}.csv")):
                    csv_path = os.path.join(os.path.join(csv_dir, f"{tn}.csv"))
                else:
                    csv_path = os.path.join(os.path.join(csv_dir, f"{otn}.csv"))
                csv_dict = csv.DictReader(open(csv_path, newline='', encoding="latin1"))
                column_info = {}
                
                for row in csv_dict:
                    headers = list(row.keys())
                    ocn_header = [h for h in headers if 'original_column_name' in h][0]  # remove BOM
                    ocn, cn = row[ocn_header].strip(), row['column_name']
                    column_description = row['column_description'].strip()
                    column_type = row['data_format'].strip()
                    column_name = cn if cn not in ['', ' '] else ocn
                    value_description = row['value_description'].strip()
                    column_info[ocn] = [column_name, column_description, column_type, value_description]

                    if column_type in ['text', 'date', 'datetime']:
                        sql = f'''SELECT DISTINCT "{ocn}" FROM `{otn}` where "{ocn}" IS NOT NULL ORDER BY RANDOM()'''
                        cursor.execute(sql)
                        try:
                            values = cursor.fetchall()
                        except sqlite3.OperationalError:
                            continue
                        if len(values) > 0 and len(str(values[0][0])) < 50:
                            if len(values) <= 10:
                                example_values = [v[0] for v in values]
                                value_prompt[f"{db_id}|{otn}|{ocn}"] = f"all possible values are {example_values}"
                                # value_prompt[f"{db_id}|{otn}|{ocn}"] = f"all possible values of the column are {', '.join(example_values)}."
                            else:
                                example_values = [v[0] for v in values[:3]]
                                value_prompt[f"{db_id}|{otn}|{ocn}"] = f"example values are {example_values}"
                                # value_prompt[f"{db_id}|{otn}|{ocn}"] = f"three example values of the column are {', '.join(example_values)}."
                        
                csv_info[f"{db_id}|{otn}"] = column_info
            # pdb.set_trace()
        return csv_info, value_prompt 
    
    def generate_pk_fk(self, question_id):
        question_info = self.question_json[question_id]
        db_id = question_info['db_id']
        table = [content for content in self.table_json if content['db_id'] == db_id][0]
        pk_dict = {}
        fk_dict = {}
        table_names_original = table['table_names_original']
        column_names_original = table['column_names_original']
        primary_keys = table['primary_keys']
        foreign_keys = table['foreign_keys']
        
        for _,pk_idx in enumerate(primary_keys):
            if type(pk_idx) == int:
                pk_dict[str(table_names_original[column_names_original[pk_idx][0]])] = [column_names_original[pk_idx][-1]]
            else:
                pk_dict[str(table_names_original[column_names_original[pk_idx[0]][0]])] = [column_names_original[idx][-1] for idx in pk_idx]
        
        for cur_fk in foreign_keys:
            src_col_idx, tgt_col_idx = cur_fk
            src_col_name = str(table_names_original[column_names_original[src_col_idx][0]]) + '.' + str(column_names_original[src_col_idx][-1])
            tgt_col_name = str(table_names_original[column_names_original[tgt_col_idx][0]]) + '.' + str(column_names_original[tgt_col_idx][-1])
            fk_dict[src_col_name] = tgt_col_name
        return pk_dict, fk_dict

class TASL(BaseModule):
    def __init__(self, db_root_path, mode, column_meaning_path):
        super().__init__(db_root_path, mode)
        self.column_meanings = json.load(open(column_meaning_path, 'r'))
        self.mode = mode
        self.schema_item_dic = self._reconstruct_schema()
        
    def _reconstruct_schema(self):
        schema_item_dic = {}
        db_id_list = [content['db_id'] for content in self.table_json]
        
        schema_item_dic = {}
        for db_id in db_id_list:
            content = [content for content in self.table_json if content['db_id'] == db_id][0]
            otn_list = content['table_names_original']
            schema_for_db = dict(zip(otn_list, [{} for _ in range(len(otn_list))]))
            schema_item_dic[db_id] = schema_for_db
        
        for key, value in self.column_meanings.items():
            db_id, otn, ocn = key.split('|')
            value = value.replace('#', '')
            value = value.replace('\n', ',  ')
            schema_item_dic[db_id][otn][ocn] = value
        return schema_item_dic
    
    def _generate_database_schema(self, schema_for_db):
        schema_prompt = '{\n '
        for table_name, cn_prompt in schema_for_db.items():
            schema_prompt += f'{table_name}:\n  ' + '{\n\t'
            for cn, prompt in cn_prompt.items():
                schema_prompt += f"{cn}: {prompt}" + '\n\t'
                schema_prompt += '\n\t'
            schema_prompt += '}\n '
        schema_prompt += '}'
        return schema_prompt
    
    def generate_dummy_sql(self, question_id):
        question = self.question_json[question_id]
        db_id = question['db_id']
        q = question['question']
        # evidence = question['evidence']
        evidence = None
        pk_dict, fk_dict = self.generate_pk_fk(question_id)
        db_prompt_dic = self._reconstruct_schema()
        db_prompt = db_prompt_dic[db_id]
        database_schema = self._generate_database_schema(db_prompt)
        prompt = dummy_sql_prompt.format(database_schema = database_schema, primary_key_dic = pk_dict, foreign_key_dic = fk_dict, question_prompt = q, evidence = evidence)
        # 返回false, 说明token数超过7168, 得删除
        while check_token(prompt):
            # 随机删除
            prompt = sample_prompt(prompt)
        dummy_sql = collect_response2(prompt=prompt, stop = 'return SQL', temperature=0.9)
        return prompt, dummy_sql
        
    def get_schema(self, question_id):
        question_info = self.question_json[question_id]
        db_id = question_info['db_id']
        _, dummy_sql = self.generate_dummy_sql(question_id)
    
        #extract schema from dummy_sql
        table_info = [content for content in self.table_json if content['db_id'] == db_id][0]
        table_names_list = table_info["table_names_original"]
        column_names_list = [[table_names_list[int(content[0])], content[1]] for content in table_info['column_names_original'][1:]]
        pure_column_name_list = [i[1] for i in column_names_list]
        filtered_tables, filtered_columns, schemas = [], [], []
        for table in table_names_list:
            if table in dummy_sql:
                filtered_tables.append(table)
        for column in pure_column_name_list:
            if column in dummy_sql:
                filtered_columns.append(column)
        filtered_tables = list(set(filtered_tables))
        filtered_columns = list(set(filtered_columns))
        for columns in filtered_columns:
            for table_column in column_names_list:
                if table_column[1] == columns and table_column[0] in filtered_tables:
                    schemas.append(table_column)
        return schemas
    
    # 第二次检查的
    def get_schema_check(self, question_id, prompts):
        question_info = self.question_json[question_id]
        db_id = question_info['db_id']
        dummy_sql = collect_response2(prompt=prompts, temperature=0.2, stop = 'return SQL')
        dummy_sql = extract_sql(dummy_sql)
        #extract schema from dummy_sql
        table_info = [content for content in self.table_json if content['db_id'] == db_id][0]
        table_names_list = table_info["table_names_original"]
        column_names_list = [[table_names_list[int(content[0])], content[1]] for content in table_info['column_names_original'][1:]]
        pure_column_name_list = [i[1] for i in column_names_list]
        filtered_tables, filtered_columns, schemas = [], [], []
        for table in table_names_list:
            if table in dummy_sql:
                filtered_tables.append(table)
        for column in pure_column_name_list:
            if column in dummy_sql:
                filtered_columns.append(column)
        filtered_tables = list(set(filtered_tables))
        filtered_columns = list(set(filtered_columns))
        for columns in filtered_columns:
            for table_column in column_names_list:
                if table_column[1] == columns and table_column[0] in filtered_tables:
                    schemas.append(table_column)
        return schemas
        
class TALOG(BaseModule):
    def __init__(self, db_root_path, mode):
        super().__init__(db_root_path, mode)
        self.csv_info, self.value_prompts = self._get_info_from_csv()
    
    def generate_schema_prompt(self, question_id, sl_schemas):
        question_info = self.question_json[question_id]
        db_id = question_info['db_id']
        schema_item_dic = {}
        
        for otn, ocn in sl_schemas:
            column_name, column_description, column_type, value_description = self.csv_info[f"{db_id}|{otn}"][ocn]
            value_prompt = self.value_prompts.get(f"{db_id}|{otn}|{ocn}")
            tmp_prompt = f"{column_type}, the full column name is {column_name}"
            if column_description not in ['', ' ', None]:
                column_description = column_description.replace('\n',' ')
                tmp_prompt += f', column description is {column_description}'
            if value_description not in ['', ' ', None]:
                value_description = value_description.replace('\n', ' ')
                tmp_prompt += f", value description is {value_description}"
            if value_prompt:
                tmp_prompt += f", {value_prompt}"
            if ' ' in otn: otn = f"`{otn}`"
            if ' ' in ocn: ocn = f"`{ocn}`"
            schema_item_dic[f"{otn}.{ocn}"] = tmp_prompt
        
        schema_prompt = '{\n\t'
        for otn_ocn, cn_prompt in schema_item_dic.items():
            schema_prompt += f'{otn_ocn}: {cn_prompt}\n'
            schema_prompt += '\n\t'
        schema_prompt += '}'
        return schema_prompt
    
    def generate_sr(self, question_id, sl_schemas):
        question = self.question_json[question_id]
        q = question['question']
        # e = question['evidence']
        e = None
        processed_schema = []
        for table, column in sl_schemas:
            if ' ' in table: table = f"`{table}`"
            if ' ' in column: column = f"`{column}`"
            processed_schema.append(f"{table}.{column}")
        processed_schema = f"[{', '.join(processed_schema)}]"
        processed_schema = processed_schema.replace("'",'')
        
        database_schema = self.generate_schema_prompt(question_id, sl_schemas)
        sr_prompt = generate_sr.format(sr_example = sr_examples, question = q, schema = processed_schema, column_description = database_schema,
                                       evidence = e)
        sr_prompt = sr_prompt.strip('\n')
        sr = collect_response2(prompt = sr_prompt, max_tokens=800, temperature=0.2)
        # print(sr)
        return sr_prompt, sr
    
    def sr2sql(self, question_id, sl_schemas):
        question = self.question_json[question_id]
        q = question['question']
        # e = question['evidence']
        e = None
        schema = ['.'.join(t) for t in sl_schemas] if sl_schemas else []
        _, sr = self.generate_sr(question_id, sl_schemas)
        sr = sr.replace('\"', '')
        database_schema = self.generate_schema_prompt(question_id, sl_schemas)
        _, fk = self.generate_pk_fk(question_id)
        sr2sql_prompt = sr2sql.format(question = q, schema = schema, evidence = e, column_description = database_schema, SR = sr, foreign_key_dic = fk)
        sr2sql_prompt = sr2sql_prompt.strip('\n')
        tmp_sql = collect_response2(prompt = sr2sql_prompt, temperature=1)
        #postprocess the tmp_sql to valid sql
        sql = 'SELECT ' + tmp_sql.replace('\"','')
        return sr, sql

def parse_single_condition(condition_str, tables):
    """解析单一过滤条件"""
    # 尝试匹配常见的条件模式：列名 操作符 值
    condition_pattern = re.search(r'([^\s<>=!]+)\s*([<>=!]+|LIKE|IN|BETWEEN)\s*(.*)', 
                                  condition_str, re.IGNORECASE)
    if not condition_pattern:
        return None
    
    column, operator, value = [p.strip() for p in condition_pattern.groups()]
    
    # 处理列名（可能带有表前缀）
    if '.' in column:
        table, col = column.split('.')
    else:
        if len(tables) == 1:
            table = list(tables)[0]
            col = column
        else:
            table = None
            col = column
    
    # 处理特殊操作符
    if operator.upper() == 'IN':
        # 提取IN列表中的值
        values = re.findall(r'\'([^\']+)\'|"([^"]+)"|\b(\d+)\b', value)
        values = [v[0] or v[1] or v[2] for v in values if any(v)]
        return (table, col, 'IN', tuple(values))
    
    elif operator.upper() == 'BETWEEN':
        # 提取BETWEEN的两个值
        between_vals = re.findall(r'\'([^\']+)\'|"([^"]+)"|\b(\d+)\b', value)
        between_vals = [v[0] or v[1] or v[2] for v in between_vals if any(v)]
        if len(between_vals) >= 2:
            return (table, col, 'BETWEEN', (between_vals[0], between_vals[1]))
    
    else:
        # 处理常规比较操作符
        # 提取值（可能是字符串、数字或布尔值）
        value = value.strip('\'"')
        return (table, col, operator, value)
    
    return None

def replace_aliases_in_sql_structure(formal_sql):
    """将SQL结构中的所有别名替换为真实表名"""
    if 'alias' not in formal_sql or not formal_sql['alias']:
        # print("No aliases found in the SQL structure.")
        return formal_sql
    
    # 创建别名到真实表名的映射
    alias_map = {alias: real for real, alias in formal_sql['alias']}
    
    # 替换列集合中的别名
    if 'C' in formal_sql and isinstance(formal_sql['C'], set):
        new_columns = set()
        for table, column in formal_sql['C']:
            # 如果表名是别名, 替换为真实表名
            real_table = alias_map.get(table, table)
            new_columns.add((real_table, column))
        formal_sql['C'] = new_columns
    
    # 替换过滤条件中的别名
    if 'F' in formal_sql and isinstance(formal_sql['F'], dict):
        for logical_op, conditions in formal_sql['F'].items():
            if isinstance(conditions, list):
                for condition in conditions:
                    if isinstance(condition, dict) and 'table' in condition:
                        table = condition['table']
                        # 如果表名是别名, 替换为真实表名
                        condition['table'] = alias_map.get(table, table)
    
    # 替换连接条件中的别名 (假设J可能是集合或字典)
    if 'J' in formal_sql:
        if isinstance(formal_sql['J'], set):
            # 如果J是集合形式: {('INNER', ('table1', 'col1'), '=', ('table2', 'col2')), ...}
            new_joins = set()
            for join_item in formal_sql['J']:
                if isinstance(join_item, tuple) and len(join_item) >= 4:
                    join_type = join_item[0]
                    left_side = join_item[1]
                    operator = join_item[2]
                    right_side = join_item[3]
                    
                    # 处理左侧 (table, column)
                    if isinstance(left_side, tuple) and len(left_side) == 2:
                        left_table, left_col = left_side
                        left_table = alias_map.get(left_table, left_table)
                        left_side = (left_table, left_col)
                    
                    # 处理右侧 (table, column)
                    if isinstance(right_side, tuple) and len(right_side) == 2:
                        right_table, right_col = right_side
                        right_table = alias_map.get(right_table, right_table)
                        right_side = (right_table, right_col)
                    
                    new_joins.add((join_type, left_side, operator, right_side))
                else:
                    new_joins.add(join_item)
            formal_sql['J'] = new_joins
    
    # 替换分组条件中的别名
    if 'G' in formal_sql and isinstance(formal_sql['G'], set):
        new_groups = set()
        for group_item in formal_sql['G']:
            if isinstance(group_item, tuple) and len(group_item) == 2:
                table, column = group_item
                # 如果表名是别名, 替换为真实表名
                table = alias_map.get(table, table)
                new_groups.add((table, column))
            else:
                new_groups.add(group_item)
        formal_sql['G'] = new_groups
    
    # 替换排序条件中的别名
    if 'O' in formal_sql and isinstance(formal_sql['O'], set):
        new_orders = set()
        for order_item in formal_sql['O']:
            if isinstance(order_item, tuple) and len(order_item) >= 2:
                table = order_item[0]
                column = order_item[1]
                # 如果表名是别名, 替换为真实表名
                table = alias_map.get(table, table)
                
                # 处理可能附加的ASC/DESC
                if len(order_item) == 3:
                    direction = order_item[2]
                    new_orders.add((table, column, direction))
                else:
                    new_orders.add((table, column))
            else:
                new_orders.add(order_item)
        formal_sql['O'] = new_orders
    
    return formal_sql

class FormalSQLValidator(BaseModule):
    def __init__(self, db_root_path, mode):
        super().__init__(db_root_path, mode)
        # 从CSV加载列信息
        self.csv_info, self.value_prompts = self._get_info_from_csv()
        table_json_path = os.path.join(db_root_path, f'{mode}_tables.json')
        self.table_json = json.load(open(table_json_path, 'r'))
    
    def formalize_schema(self, db_id):
        """将数据库模式转换为形式化表示: D = (Tabs, Cols, PKs, FKs, Types)"""
        # 获取特定数据库的表信息
        table_info = [content for content in self.table_json if content['db_id'] == db_id]
        if len(table_info) == 0:
            raise ValueError(f"Database ID '{db_id}' not found in table_json.")
        table_info = table_info[0]
        
        # 形式化表示
        formal_schema = {
            'Tabs': set(table_info['table_names_original']),
            'Cols': {},  # 表到列的映射
            'PKs': {},   # 表到主键的映射
            'FKs': [],   # 外键关系
            'Types': {}  # 列到类型的映射
        }
        
        # 构建列映射
        for i, col in enumerate(table_info['column_names_original']):
            if i == 0:  # 跳过第一个元素（通常是索引）
                continue
                
            table_idx, col_name = col
            table_name = table_info['table_names_original'][table_idx]
            
            if table_name not in formal_schema['Cols']:
                formal_schema['Cols'][table_name] = set()
                
            formal_schema['Cols'][table_name].add(col_name)
            
            # 添加列类型信息（从CSV信息中获取）
            key = f"{db_id}|{table_name}|{col_name}"
            if key in self.csv_info:
                _, _, col_type, _ = self.csv_info[key]
                formal_schema['Types'][(table_name, col_name)] = col_type
        if table_info['primary_keys'] != []:
            # 添加主键信息
            for pk in table_info['primary_keys']:
                pk_index = pk[0] if isinstance(pk, list) else pk
                pk_col = table_info['column_names_original'][pk_index]
                pk_table = table_info['table_names_original'][pk_col[0]]

                if pk_table not in formal_schema['PKs']:
                    formal_schema['PKs'][pk_table] = set()

                formal_schema['PKs'][pk_table].add(pk_col[1])
        else:
            formal_schema['PKs'] = {}
        if table_info['foreign_keys'] != []:
            # 添加外键关系
            for src, tgt in table_info['foreign_keys']:
                src_col = table_info['column_names_original'][src]
                tgt_col = table_info['column_names_original'][tgt]

                src_table = table_info['table_names_original'][src_col[0]]
                tgt_table = table_info['table_names_original'][tgt_col[0]]

                formal_schema['FKs'].append((
                    (src_table, src_col[1]),
                    (tgt_table, tgt_col[1])
                ))
        else:
            formal_schema['FKs'] = []
            
        return formal_schema
    
    def build_fk_graph(self, formal_schema):
        """从外键关系构建无向图"""
        from collections import defaultdict

        graph = defaultdict(set)
        edge_details = {}  # 存储边的详细信息 (table1, table2) -> (col1, col2)

        for (src_table, src_col), (tgt_table, tgt_col) in formal_schema['FKs']:
            # 无向图，双向连接
            graph[src_table].add(tgt_table)
            graph[tgt_table].add(src_table)

            # 存储连接的列信息
            edge_details[(src_table, tgt_table)] = (src_col, tgt_col)
            edge_details[(tgt_table, src_table)] = (tgt_col, src_col)

        return graph, edge_details
    
    def _find_join_condition(self, table1, table2, fk_relations):
        """
        在外键关系中查找两个表之间的连接条件
        Args:
            table1, table2: 要连接的两个表
            fk_relations: 外键关系列表 [((src_table, src_col), (tgt_table, tgt_col)), ...]
        Returns:
            JOIN语句字符串
        """
        for (src_table, src_col), (tgt_table, tgt_col) in fk_relations:

            # 情况1: table1 -> table2 (table1的外键指向table2)
            # 例如: disp.client_id -> client.client_id
            if src_table == table2 and tgt_table == table1:
                return f"JOIN {table2} ON {table1}.{tgt_col} = {table2}.{src_col}"

            # 情况2: table2 -> table1 (table2的外键指向table1)  
            # 例如: disp.account_id -> account.account_id
            elif src_table == table1 and tgt_table == table2:
                return f"JOIN {table2} ON {table1}.{src_col} = {table2}.{tgt_col}"

        return None

    def _find_all_paths(self, start_table, end_table, formal_schema, max_depth=4):
        fk_relations = formal_schema.get('FKs', [])
        all_paths = []

        def dfs(current_table, target_table, current_path, visited):
            if len(current_path) > max_depth:
                return

            if current_table == target_table:
                all_paths.append(current_path[:])  
                return

            for (src_table, src_col), (tgt_table, tgt_col) in fk_relations:
                next_table = None

                if src_table == current_table and tgt_table not in visited:
                    next_table = tgt_table
                elif tgt_table == current_table and src_table not in visited:
                    next_table = src_table

                if next_table:
                    visited.add(next_table)
                    current_path.append(next_table)
                    dfs(next_table, target_table, current_path, visited)
                    current_path.pop() 
                    visited.remove(next_table)

        # DFS
        visited = {start_table}
        dfs(start_table, end_table, [start_table], visited)

        return all_paths
    
    def find_connection_paths(self, table1: str, table2: str, 
                            max_paths: int = 3, max_depth: int = 4) -> Dict:

        formal_schema = self.formalize_schema(self.current_db_id)

        all_paths = self._find_all_paths(table1, table2, formal_schema, max_depth)

        if not all_paths:
            return {
                'connected': False,
                'total_paths': 0,
                'paths': []
            }

        return {
            'connected': True,
            'total_paths': len(all_paths),
            'paths': all_paths
        }
    
    
    def find_multiple_join_paths(self, formal_schema: Dict, table1: str, table2: str) -> List[Dict]:
        result = self.find_connection_paths(table1, table2)
        return result['paths'] if result['connected'] else []
    
    def check_tables_connectivity(self, tables: List[str]) -> Dict:
        formal_schema = self.formalize_schema(self.current_db_id)
        
        current_connections = self._get_current_connections({}, formal_schema)
        disconnected_components = self._find_disconnected_components(set(tables), current_connections)
        
        if len(disconnected_components) <= 1:
            return {
                'fully_connected': True,
                'disconnected_components': [],
                'missing_connections': []
            }
        
        missing_connections = []
        for i in range(len(disconnected_components) - 1):
            comp1 = disconnected_components[i]
            comp2 = disconnected_components[i + 1]
            
            best_table1 = self._find_best_connection_point(comp1, formal_schema)
            best_table2 = self._find_best_connection_point(comp2, formal_schema)
            
            connection_paths = self.find_connection_paths(best_table1, best_table2)
            
            missing_connections.append({
                'from_component': comp1,
                'to_component': comp2,
                'suggested_paths': connection_paths['paths']
            })
        
        return {
            'fully_connected': False,
            'disconnected_components': disconnected_components,
            'missing_connections': missing_connections
        }
    
    def _get_current_connections(self, join_conditions, formal_schema=None):
        from collections import defaultdict

        connections = defaultdict(set)
        print(f"[DEBUG] Getting current connections from join conditions: {join_conditions}")

        for join_condition in join_conditions:
            try:
                if len(join_condition) >= 4:
                    join_type, left_condition, operator, right_condition = join_condition[:4]

                    left_table = left_condition[0] if isinstance(left_condition, (list, tuple)) and len(left_condition) > 0 else None
                    left_column = left_condition[1] if isinstance(left_condition, (list, tuple)) and len(left_condition) > 1 else None
                    right_table = right_condition[0] if isinstance(right_condition, (list, tuple)) and len(right_condition) > 0 else None
                    right_column = right_condition[1] if isinstance(right_condition, (list, tuple)) and len(right_condition) > 1 else None

                    valid_join = True
                    if formal_schema:
                        left_valid = left_column in formal_schema.get('Cols', {}).get(left_table, set())
                        right_valid = right_column in formal_schema.get('Cols', {}).get(right_table, set())
                        valid_join = left_valid and right_valid

                        print(f"[DEBUG] Validating JOIN: {left_table}.{left_column} = {right_table}.{right_column}")
                        print(f"[DEBUG] Left valid: {left_valid}, Right valid: {right_valid}, Overall valid: {valid_join}")

                    if valid_join and left_table and right_table:
                        connections[left_table].add(right_table)
                        connections[right_table].add(left_table)
                        print(f"[DEBUG] Added valid connection: {left_table} <-> {right_table}")
                    else:
                        print(f"[DEBUG] Skipped invalid JOIN: {left_table}.{left_column} = {right_table}.{right_column}")

            except Exception as e:
                print(f"[DEBUG] Error processing JOIN condition {join_condition}: {str(e)}")
                continue
            
        print(f"[DEBUG] Final valid connections: {connections}")
        return connections

    def _find_disconnected_components(self, all_tables, connections):
        visited = set()
        components = []
        print(f"[DEBUG] Now Im in _find_disconnected_components function Finding disconnected components in tables: {all_tables}")
        def dfs(table, current_component):
            if table in visited:
                return

            visited.add(table)
            current_component.add(table)

            # 访问所有相邻的表
            for connected_table in connections.get(table, set()):
                if connected_table in all_tables: 
                    dfs(connected_table, current_component)

        for table in all_tables:
            if table not in visited:
                component = set()
                dfs(table, component)
                if component:  
                    components.append(component)
        print(f"[DEBUG] Disconnected components found: {components}")
        return components
    
    def _find_best_connection_point(self, component, formal_schema):
        fk_count = {}
        print(f"[DEBUG] Now Im in _find_best_connection_point function")
        for table in component:
            count = 0
            for (src_table, src_col), (tgt_table, tgt_col) in formal_schema['FKs']:
                if src_table == table or tgt_table == table:
                    count += 1
            fk_count[table] = count
        
        print("[DEBUG] Foreign key counts in component:", fk_count)
        return max(fk_count, key=fk_count.get)
    
    def detect_missing_joins(self, formal_sql, formal_schema):
        """检测并修复JOIN连通性问题"""
        tables_in_query = formal_sql['T']
        if len(tables_in_query) <= 1:
            return None
        print(f"[DEBUG] Detecting query tables are: {tables_in_query}")
        

        current_connections = self._get_current_connections(formal_sql['J'], formal_schema)
        disconnected_components = self._find_disconnected_components(tables_in_query, current_connections)

        print(f"[DEBUG] Disconnected components after validation: {disconnected_components}")

        if len(disconnected_components) > 1:
            print(f"[DEBUG] Found {len(disconnected_components)} disconnected components, generating missing JOINs...")
    
            all_missing_paths = [] 
    
            for i in range(len(disconnected_components) - 1):
                comp1 = disconnected_components[i]      
                comp2 = disconnected_components[i + 1]  

                print(f"[DEBUG] Connecting component {comp1} to component {comp2}")
        
                component_paths = []
                for table1 in comp1:
                    for table2 in comp2:
                        print(f"[DEBUG] Finding paths from {table1} to {table2}")
                        paths = self._find_all_paths(table1, table2, formal_schema)
                        if paths:
                            print(f"[DEBUG] Found {len(paths)} paths from {table1} to {table2}: {paths}")
                            component_paths.extend(paths)
                        else:
                            print(f"[DEBUG] No path found between {table1} and {table2}")

                if component_paths:
                    print(f"[DEBUG] All possible paths for this component pair: {component_paths}")
                    all_missing_paths.extend(component_paths)
    
            if all_missing_paths:
                print(f"[DEBUG] Found join paths: {all_missing_paths}")
                return all_missing_paths
            else:
                print(f"[DEBUG] No paths found to connect components")
                return None

        print(f"[DEBUG] No missing joins needed - all tables connected")
        return None
    
    
    def parse_sql(self, sql_string):
        """将SQL字符串解析为形式化表示: S = (T, C, J, F, G, O, L)"""

        def strip_ansi_codes(s):
            return re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', s)
        
        sql_string = strip_ansi_codes(sql_string)

        syntax_errors = self._validate_basic_syntax(sql_string)
        if syntax_errors:
            return None, f"Syntax errors: {'; '.join(syntax_errors)}"

        try:
            parser = Parser(sql_string)
            
            parsed = sqlparse.parse(sql_string)[0]
        except Exception as e:
            return None, f"SQL parse error: {str(e)}"
        
        # try:
        if 1:
            # 提取查询组件
            formal_sql = {
                'T': set(),  
                'C': set(),  
                'J': set(),  
                'F': {},     
                'G': set(),  
                'O': set(),  
                'L': None,   
                'alias': set(),  
                'subqueries': [] 
            }
            try:
                resolved_tables = parser.tables
            except Exception as e:
                return None, f"SQL parse error: {str(e)}"
            formal_sql['T'] = resolved_tables
            formal_sql['alias'] = extract_aliases(parsed)

            alias_map = {alias: real for real, alias in formal_sql['alias']}

            # --- 2. Extract Tables (T) ---
            known_tables_and_aliases = set(resolved_tables)
            for real, alias in formal_sql['alias']:
                known_tables_and_aliases.add(alias) # Add alias names
            
            # 提取列
            for _, alias in formal_sql['alias']:
                known_tables_and_aliases.add(alias) # Add alias names

            sorted_aliases = sorted(alias_map.items(), key=lambda x: len(x[0]), reverse=True)

            normalized_sql = sql_string
            for alias, real_table in sorted_aliases:
                if real_table is not None and alias is not None:
                    pattern = r'\b' + re.escape(alias) + r'\b'
                    normalized_sql = re.sub(pattern, real_table, normalized_sql)
                else:
                    print(f"[DEBUG] Skipping alias replacement: alias='{alias}', real_table='{real_table}'")

            # 调试信息：帮助排查别名替换问题
            sql = sql_string.replace('\n', ' ').strip()
            print(f"[DEBUG ALIAS] Original SQL: {sql}")
            print(f"[DEBUG ALIAS] Alias mapping: {alias_map}")  
            print(f"[DEBUG ALIAS] Normalized SQL: {normalized_sql}")

            parsed = sqlparse.parse(normalized_sql)[0]

            # formal_sql['C'] = extract_columns(parsed, alias_map, resolved_tables, known_tables_and_aliases)
            formal_sql['C'] = get_all_columns(sql)
            
            # formal_sql['G'] = extract_groupby_conditions(parsed, alias_map, resolved_tables)
            formal_sql['G'] = get_group_by_columns(sql)
            
            formal_sql['O'] = get_order_by_columns(sql)
            
            # formal_sql['F'] = extract_where_conditions(sql_string)
            formal_sql['F'] = get_where_structure(sql)
            
            formal_sql = replace_aliases_in_sql_structure(formal_sql)
            
            formal_sql['Having'] = get_having_structure(sql)
            formal_sql['subqueries'] = get_subqueries(sql)
            
            # 使用正则表达式提取LIMIT子句
            limit_match = re.search(r'LIMIT\s+(\d+)(?:\s*,\s*(\d+))?', sql_string, re.IGNORECASE)
            if limit_match:
                if limit_match.group(2):  # LIMIT x, y 格式
                    formal_sql['L'] = (int(limit_match.group(1)), int(limit_match.group(2)))
                else:  # LIMIT x 格式
                    formal_sql['L'] = int(limit_match.group(1))
            
            # 提取join条件
            formal_sql['J'] = extract_join_conditions(parsed, formal_sql['alias'])
            
            return formal_sql, None

        # except Exception as e:
        #     return None, f"SQL extract: {str(e)}"

    def build_formal_schema_from_json(self):
        """
        Converts the table_json structure into the formal_schema format with 'Tabs' and 'Cols'
        """
        formal_schema = {
            'Tabs': set(),
            'Cols': {}
        }

        # Extract database information
        for db_info in self.table_json:
            # Get table names
            table_names_original = db_info.get('table_names_original', [])

            # Add tables to formal_schema['Tabs']
            for table in table_names_original:
                formal_schema['Tabs'].add(table)
                formal_schema['Cols'][table] = set()

            # Get column information
            column_names_original = db_info.get('column_names_original', [])

            # Add columns to their respective tables
            for col_info in column_names_original:
                if len(col_info) >= 2:
                    table_index, column_name = col_info

                    # Skip the special -1 index which indicates the '*' wildcard
                    if table_index == -1:
                        continue
                    
                    # Ensure table_index is within range
                    if 0 <= table_index < len(table_names_original):
                        table_name = table_names_original[table_index]
                        formal_schema['Cols'][table_name].add(column_name)

        return formal_schema
    
    def _format_error(self, error_type, component, message, suggestion=None):
        """
        改进的错误格式化, 为LLM提供更直接的修复指导
        """
        error_msg = f"{error_type} in {component}: {message}"
        if suggestion:
            if "In table" in suggestion and ":" in suggestion:
                import re
                match = re.search(r"In table '([^']+)': (.+)", suggestion)
                if match:
                    table_name = match.group(1)
                    column_name = match.group(2).strip()

                    if ' ' in column_name:
                        error_msg += f" SOLUTION: Use {table_name}.'{column_name}' (column names with spaces need quotes)"
                    else:
                        error_msg += f" SOLUTION: Use {table_name}.{column_name}"
                else:
                    error_msg += f" Hint: {suggestion}"
            else:
                error_msg += f" Hint: {suggestion}"
        return error_msg

    def _find_join_path(self, formal_schema, table1, table2):
        fk_relations = formal_schema.get('FKs', [])
        
        for (src_table, src_col), (tgt_table, tgt_col) in fk_relations:
            if (src_table == table1 and tgt_table == table2):
                return f"{table1}.{src_col} = {table2}.{tgt_col}"
            elif (src_table == table2 and tgt_table == table1):
                return f"{table2}.{src_col} = {table1}.{tgt_col}"
        from collections import defaultdict, deque
        
        graph = defaultdict(list)
        edge_info = {}
        
        for (src_table, src_col), (tgt_table, tgt_col) in fk_relations:
            graph[src_table].append(tgt_table)
            graph[tgt_table].append(src_table)
            edge_info[(src_table, tgt_table)] = f"{src_table}.{src_col} = {tgt_table}.{tgt_col}"
            edge_info[(tgt_table, src_table)] = f"{tgt_table}.{tgt_col} = {src_table}.{src_col}"
        
        queue = deque([(table1, [table1])])
        visited = {table1}
        
        while queue:
            current, path = queue.popleft()
            
            if current == table2:
                join_conditions = []
                for i in range(len(path) - 1):
                    t1, t2 = path[i], path[i + 1]
                    if (t1, t2) in edge_info:
                        join_conditions.append(edge_info[(t1, t2)])
                    elif (t2, t1) in edge_info:
                        join_conditions.append(edge_info[(t2, t1)])
                
                if join_conditions:
                    if len(path) == 2:
                        return join_conditions[0] 
                    else:
                        return f"via {' → '.join(path)}: {' AND '.join(join_conditions)}"
                else:
                    return None 
            
            for neighbor in graph[current]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))
        
        return None  
        
    def _detect_as_aliases(self, sql_string):

        import re
        aliases = set()
        

        as_pattern = r'\b(?:SELECT\s+.*?|,\s*)((?:[^,\s()]+\s*[+\-*/]\s*[^,\s()]+|\w+\([^)]*\)|[^,\s()]+))\s+AS\s+(\w+)\b'
        matches = re.finditer(as_pattern, sql_string, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            alias_name = match.group(2)
            aliases.add(alias_name)
        
        func_alias_pattern = r'\b\w+\s*\([^)]*\)\s+AS\s+(\w+)\b'
        func_matches = re.finditer(func_alias_pattern, sql_string, re.IGNORECASE)
        
        for match in func_matches:
            alias_name = match.group(1)
            aliases.add(alias_name)
        
        expr_alias_pattern = r'\([^)]+\)\s+AS\s+(\w+)\b'
        expr_matches = re.finditer(expr_alias_pattern, sql_string, re.IGNORECASE)
        
        for match in expr_matches:
            alias_name = match.group(1)
            aliases.add(alias_name)
        
        return aliases
    
    def _validate_column(self, formal_schema, table_column_pairs, component_name, from_tables=None, sql_string=None):
        errors = []

        for table, column in table_column_pairs:
            if column == '*' or self._is_aggregate_function(column) or self._is_expression_or_function(column):
                continue
            print("[DEBUG] column:", column, "table:", table)
            if sql_string and table is None:
                import re
                alias_pattern = rf'\bAS\s+["\']?{re.escape(column)}["\']?\b'
                if re.search(alias_pattern, sql_string, re.IGNORECASE):
                    continue
                
            if table is not None:  
                if table not in formal_schema['Tabs']:
                    suggestion = f"Available tables: {', '.join(formal_schema['Tabs'])}"
                    errors.append(self._format_error(
                        "TABLE_NOT_FOUND",
                        component_name,
                        f"Table '{table}' does not exist in database",
                        suggestion
                    ))
                    continue

                table_columns = formal_schema['Cols'].get(table, set())

                if column not in table_columns:
                    replacement_hint = self._generate_sql_replacement_hint(formal_schema, table, column)

                    if replacement_hint:
                        suggestion = replacement_hint
                    else:
                        similar_columns = [
                            col for col in table_columns 
                            if self._similarity(column, col) > 0.7
                        ]

                        if similar_columns:
                            if ' ' in similar_columns[0]:
                                suggestion = f"Did you mean '{table}.\"{similar_columns[0]}\"'? (use quotes for column names with spaces)"
                            else:
                                suggestion = f"Did you mean '{table}.{similar_columns[0]}'?"
                        else:
                            available_cols = list(table_columns)[:5]
                            suggestion = f"Available columns in '{table}': {', '.join(available_cols)}"

                    errors.append(self._format_error(
                        "COLUMN_NOT_FOUND", 
                        component_name,
                        f"Column '{column}' does not exist in table '{table}'",
                        suggestion
                    ))

            else:  
                search_tables = from_tables if from_tables else formal_schema['Tabs']
                exact_tables = [
                    t for t in search_tables
                    if column in formal_schema['Cols'].get(t, set())
                ]

                if exact_tables:
                    if len(exact_tables) > 1:
                        suggestion = f"Specify table: {', '.join(f'{t}.{column}' for t in exact_tables)}"
                        errors.append(self._format_error(
                            "COLUMN_AMBIGUOUS",
                            component_name,
                            f"Column '{column}' exists in multiple tables",
                            suggestion
                        ))
                else:
                    best_match = None
                    best_similarity = 0

                    for t in search_tables:
                        t_columns = formal_schema['Cols'].get(t, set())
                        for col in t_columns:
                            similarity = self._similarity(column, col)
                            if similarity > best_similarity:
                                best_similarity = similarity
                                best_match = (t, col)

                    if best_match and best_similarity > 0.7:
                        t, col = best_match
                        if ' ' in col:
                            suggestion = f"Did you mean '{t}.\"{col}\"'? (use quotes for column names with spaces)"
                        else:
                            suggestion = f"Did you mean '{t}.{col}'?"
                    else:
                        suggestion = "Check column name spelling"

                    errors.append(self._format_error(
                        "COLUMN_NOT_FOUND",
                        component_name, 
                        f"Column '{column}' does not exist in any table",
                        suggestion
                    ))
        return errors
    
    def _generate_sql_replacement_hint(self, formal_schema, table, wrong_column):

        if table not in formal_schema['Cols']:
            return None

        available_columns = formal_schema['Cols'][table]


        similar_columns = []
        for col in available_columns:
            similarity = self._similarity(wrong_column, col)
            if similarity > 0.7:  
                similar_columns.append((col, similarity))

        if similar_columns:

            best_match = max(similar_columns, key=lambda x: x[1])
            best_column = best_match[0]


            if ' ' in best_column:
                return f"REPLACE '{table}.{wrong_column}' WITH '{table}.\"{best_column}\"' or '{table}.`{best_column}`'"
            else:
                return f"REPLACE '{table}.{wrong_column}' WITH '{table}.`{best_column}`' "

        return None

    def _extract_alias_columns(self, formal_sql_columns):

        real_columns = []
        alias_columns = []
        
        for table, column in formal_sql_columns:
            if table is None and column:
                is_alias = True

                alias_columns.append((table, column))
            else:
                real_columns.append((table, column))
        
        return real_columns, alias_columns
    
    def _process_where_conditions(self, where_data):

        conditions = []
        
        if not where_data:
            return conditions
            
        if 'conditions' in where_data:
            conditions.extend(where_data['conditions'])
        
        if 'AND' in where_data:
            conditions.extend(where_data['AND'])
            
        if 'OR' in where_data:
            conditions.extend(where_data['OR'])
            
        return conditions
    
    def _is_aggregate_function(self, expression):

        print("[DEBUG] Checking for aggregate function in expression:", expression)
        if not isinstance(expression, str):
            return False
        
        aggreagete_functions = ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(', 'ABS('
                                'SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'ABS']
        return expression.upper().startswith(tuple(aggreagete_functions)) or \
               any(func in expression for func in aggreagete_functions) 
    
    def _is_expression_or_function(self, column_text):

        print("[DEBUG] Checking for expression or function in column text:", column_text)
        if not isinstance(column_text, str):
            return False
            
        return any(
            column_text.startswith(func) or 
            column_text.endswith(')') and '(' in column_text 
            for func in ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(', 'ABS(', 'ROUND(', 'DATEADD(', 'DATEDIFF('
                         'SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'ABS', 'ROUND', 'DATEADD', 'DATEDIFF']
        )
    
    def validate(self, formal_sql, formal_schema=None, sql_string=None):
        errors = set()
        warnings = []

        if formal_schema is None:
            formal_schema = self.build_formal_schema_from_json()

        if sql_string:
            print(f"[DEBUG] Original SQL: {sql_string.strip()}")
            print(f"[DEBUG] Parsed SELECT columns (formal_sql['C']): {formal_sql['C']}")
            print(f"[DEBUG] Parsed JOIN conditions (formal_sql['J']): {formal_sql['J']}")

        table_not_exits = False
        for table in formal_sql['T']:
            if table not in formal_schema['Tabs']:
                suggestion = f"Available tables: {', '.join(formal_schema['Tabs'])}"
                errors.add(self._format_error(
                    "TABLE_NOT_FOUND",
                    "FROM",
                    f"Table '{table}' does not exist",
                    suggestion
                ))
                table_not_exits = True
        
        if table_not_exits:
            return errors, warnings
        
        filtered_select_columns = []
        join_columns = set()
        
        if formal_sql.get('J'):
            for join_condition in formal_sql['J']:
                try:
                    if len(join_condition) >= 4:
                        _, left_condition, _, right_condition = join_condition[:4]
                        if isinstance(left_condition, (list, tuple)) and len(left_condition) >= 2:
                            join_columns.add((left_condition[0], left_condition[1]))
                        if isinstance(right_condition, (list, tuple)) and len(right_condition) >= 2:
                            join_columns.add((right_condition[0], right_condition[1]))
                except Exception:
                    continue
        
        print(f"[DEBUG] Extracted JOIN columns: {join_columns}")
        
        for table, column in formal_sql['C']:
            if (table, column) not in join_columns:
                filtered_select_columns.append((table, column))
        
        print(f"[DEBUG] Filtered SELECT columns: {filtered_select_columns}")
        
        select_errors = self._validate_column(
            formal_schema, 
            filtered_select_columns, 
            "SELECT",
            from_tables=formal_sql['T'],
            sql_string=sql_string
        )
        if select_errors:
            print(f"[DEBUG] SELECT validation errors: {select_errors}")
            errors.update(select_errors)
        

        where_conditions = self._process_where_conditions(formal_sql['F'])
        
        for condition in where_conditions:
            if not isinstance(condition, dict):
                continue
                
            table = condition.get('table')
            column = condition.get('column')
            
            if not table or not column:
                continue
            print("[DEBUG] Validating WHERE condition:", table, column)
            if self._is_aggregate_function(column) or self._is_expression_or_function(column):
                continue 
                
            where_errors = self._validate_column(
                formal_schema,
                [(table, column)],
                "WHERE",
                from_tables=formal_sql['T'],
                sql_string=sql_string
            )
            errors.update(where_errors)

        select_columns = {column for _, column in filtered_select_columns}  
        group_by_columns = get_group_by_columns(sql_string)  
        
        if 'O' in formal_sql and formal_sql['O']:
            for order_item in formal_sql['O']:
                if not isinstance(order_item, tuple) or len(order_item) < 2:
                    warnings.append("Invalid ORDER BY format")
                    continue

                try:
                    first_part = order_item[0]
                    second_part = order_item[1]
                    
                    table = None
                    column = None

                    if first_part is not None:
                        if '(' in first_part:  
                            parts = first_part.split('(')
                            if len(parts) > 1:
                                table = parts[1].strip()
                        else:
                            table = first_part.strip()

                    if ')' in second_part:
                        column = second_part.split(')')[0]
                    else:
                        column = second_part

                    if table and column:
                        orderby_errors = self._validate_column(
                            formal_schema,
                            [(table, column)],
                            "ORDER_BY",
                            from_tables=formal_sql['T'],
                            sql_string=sql_string
                        )
                        errors.update(orderby_errors)
                        
                        is_aggregate = '(' in first_part
                        if not is_aggregate and column not in select_columns and column not in group_by_columns:
                            warnings.append(f"ORDER BY column '{column}' should appear in SELECT or GROUP BY")
                            
                except Exception as e:
                    warnings.append(f"Error processing ORDER BY item: {str(e)}")

        if formal_sql.get('J'):
            try:
                join_errors = []
                join_conditions = formal_sql.get('J', set())
                schema_tabs = formal_schema.get('Tabs', set())
                schema_cols = formal_schema.get('Cols', {})
                
                print(f"[DEBUG] Starting JOIN validation with {len(join_conditions)} conditions")
                
                for join_condition in join_conditions:
                    try:
                        if len(join_condition) >= 4:
                            join_type, left_condition, operator, right_condition = join_condition[:4]
                            
                            left_table = left_condition[0] if isinstance(left_condition, (list, tuple)) and len(left_condition) > 0 else None
                            left_column = left_condition[1] if isinstance(left_condition, (list, tuple)) and len(left_condition) > 1 else None
                            
                            right_table = right_condition[0] if isinstance(right_condition, (list, tuple)) and len(right_condition) > 0 else None
                            right_column = right_condition[1] if isinstance(right_condition, (list, tuple)) and len(right_condition) > 1 else None
                            
                            print(f"[DEBUG] Checking JOIN: {left_table}.{left_column} = {right_table}.{right_column}")
                            
                            if right_table and right_column:
                                if right_table not in schema_tabs:
                                    join_errors.append(f"TABLE_NOT_FOUND in JOIN: Table '{right_table}' does not exist")
                                elif right_column not in schema_cols.get(right_table, set()):
                                    available_cols = list(schema_cols.get(right_table, set()))
                                    suggestion = f"Table '{right_table}' has columns: {', '.join(available_cols[:5])}. Need to connect via intermediate table."
                                    join_errors.append(f"COLUMN_NOT_FOUND in JOIN: Column '{right_column}' does not exist in table '{right_table}'. Hint: {suggestion}")
                            
                            if left_table and left_column:
                                if left_table not in schema_tabs:
                                    join_errors.append(f"TABLE_NOT_FOUND in JOIN: Table '{left_table}' does not exist")
                                elif left_column not in schema_cols.get(left_table, set()):
                                    available_cols = list(schema_cols.get(left_table, set()))
                                    suggestion = f"Table '{left_table}' has columns: {', '.join(available_cols[:5])}"
                                    join_errors.append(f"COLUMN_NOT_FOUND in JOIN: Column '{left_column}' does not exist in table '{left_table}'. Hint: {suggestion}")
                                    
                    except Exception as e:
                        print(f"[DEBUG] Error processing JOIN condition {join_condition}: {str(e)}")
                        continue
                
                print(f"[DEBUG] JOIN validation found {len(join_errors)} errors")
                errors.update(join_errors)
                

            except Exception as e:
                warnings.append(f"JOIN validation skipped due to complexity: {str(e)}")
                print(f"[DEBUG] JOIN validation exception: {str(e)}")

        if errors and sql_string:
            llm_fix = self._generate_sql_fix_for_llm(sql_string, errors)
            if llm_fix:
                errors.add(llm_fix)  
            
        return errors, warnings

    def _validate_join_simple(self, formal_sql: dict, formal_schema: dict):
        """
        简化的JOIN验证, 重点检测JOIN条件中的列错误
        """
        errors = []
        warnings = []
        
        join_conditions = formal_sql.get('J', set())
        if not join_conditions:
            return errors, warnings
            
        schema_tabs = formal_schema.get('Tabs', set())
        schema_cols = formal_schema.get('Cols', {})
        
        for join_condition in join_conditions:
            try:
                if len(join_condition) >= 4:
                    join_type, left_condition, operator, right_condition = join_condition[:4]
                    
                    left_table = left_condition[0] if isinstance(left_condition, (list, tuple)) and len(left_condition) > 0 else None
                    left_column = left_condition[1] if isinstance(left_condition, (list, tuple)) and len(left_condition) > 1 else None
                    
                    right_table = right_condition[0] if isinstance(right_condition, (list, tuple)) and len(right_condition) > 0 else None
                    right_column = right_condition[1] if isinstance(right_condition, (list, tuple)) and len(right_condition) > 1 else None
                    
                    if left_table and left_column:
                        if left_table not in schema_tabs:
                            errors.append(self._format_error(
                                "TABLE_NOT_FOUND",
                                "JOIN",
                                f"Table '{left_table}' in JOIN does not exist",
                                f"Available tables: {', '.join(schema_tabs)}"
                            ))
                        elif left_column not in schema_cols.get(left_table, set()):
                            suggestion = self._generate_join_fix_suggestion(
                                formal_schema, left_table, right_table, left_column, left_table
                            )
                            errors.append(self._format_error(
                                "COLUMN_NOT_FOUND",
                                "JOIN", 
                                f"Column '{left_column}' does not exist in table '{left_table}' in JOIN condition",
                                suggestion
                            ))
                    
                    if right_table and right_column:
                        if right_table not in schema_tabs:
                            errors.append(self._format_error(
                                "TABLE_NOT_FOUND",
                                "JOIN",
                                f"Table '{right_table}' in JOIN does not exist",
                                f"Available tables: {', '.join(schema_tabs)}"
                            ))
                        elif right_column not in schema_cols.get(right_table, set()):
                            suggestion = self._generate_llm_friendly_suggestion(
                                formal_schema, sql, left_table, right_table, right_column, right_table
                            )
                            
                            errors.append(self._format_error(
                                "COLUMN_NOT_FOUND",
                                "JOIN",
                                f"Column '{right_column}' does not exist in table '{right_table}' in JOIN condition",
                                suggestion
                            ))
                        
            except Exception:
                continue
        
        return errors, warnings
    
    def _need_intermediate_table(self, formal_schema, table1, table2):
        """检查两个表是否需要通过中间表连接"""
        fk_relations = formal_schema.get('FKs', [])
        
        for (src_table, src_col), (tgt_table, tgt_col) in fk_relations:
            if (src_table == table1 and tgt_table == table2) or (src_table == table2 and tgt_table == table1):
                return False  
        
        return True  
    
    def _suggest_intermediate_path(self, formal_schema, table1, table2):
        """建议通过中间表的连接路径"""
        fk_relations = formal_schema.get('FKs', [])
        
        table1_relations = set()
        table2_relations = set()
        
        for (src_table, src_col), (tgt_table, tgt_col) in fk_relations:
            if src_table == table1:
                table1_relations.add(tgt_table)
            elif tgt_table == table1:
                table1_relations.add(src_table)
                
            if src_table == table2:
                table2_relations.add(tgt_table)
            elif tgt_table == table2:
                table2_relations.add(src_table)
        
        common_tables = table1_relations & table2_relations
        
        if common_tables:
            intermediate = list(common_tables)[0]  
            
            path1 = self._find_join_path(formal_schema, table1, intermediate)
            path2 = self._find_join_path(formal_schema, intermediate, table2)
            
            if path1 and path2:
                return f"USE: {table1} JOIN {intermediate} ON {path1} JOIN {table2} ON {path2}"
        
        return None
    
    def _generate_llm_friendly_suggestion(self, formal_schema, sql_string, left_table, right_table, missing_column, missing_table):

        suggestions = []
        
        import re
        error_join_pattern = rf'JOIN\s+{re.escape(missing_table)}\s+ON\s+[^J]+{re.escape(missing_column)}'
        error_join_match = re.search(error_join_pattern, sql_string, re.IGNORECASE)
        
        if error_join_match:
            error_join = error_join_match.group(0)
            suggestions.append(f"PROBLEM: '{error_join}' is invalid because table '{missing_table}' has no column '{missing_column}'")
        
        intermediate_path = self._suggest_intermediate_path(formal_schema, left_table, right_table)
        if intermediate_path and "USE:" in intermediate_path:
            replacement = intermediate_path.replace("USE: ", "")
            suggestions.append(f"SOLUTION: Replace the invalid JOIN with: {replacement}")
        
        available_cols = list(formal_schema['Cols'].get(missing_table, set()))[:3]
        if available_cols:
            suggestions.append(f"Available columns in '{missing_table}': {', '.join(available_cols)}")
        
        return ". ".join(suggestions)
    
    def _generate_sql_fix_for_llm(self, sql_string, errors):
        """
        基于错误信息生成完整的SQL修复建议
        """
        if not errors:
            return None
            
        join_errors = [error for error in errors if "JOIN" in error]
        if not join_errors:
            return None
            
        suggestions = []
        for error in join_errors:
            if "SOLUTION: Replace" in error:
                import re
                solution_match = re.search(r'SOLUTION: Replace the invalid JOIN with: (.+?)(?:\.|$)', error)
                if solution_match:
                    new_join = solution_match.group(1)
                    suggestions.append(f"Use: {new_join}")
        
        if suggestions:
            return f"FIX INSTRUCTION: {'; '.join(suggestions)}"
        
        return None
    def _generate_join_fix_suggestion(self, formal_schema, left_table, right_table, missing_column, missing_table):

        suggestions = []
        
        suggestions.append(f"ERROR: Table '{missing_table}' does not have column '{missing_column}'")
        
        available_cols = list(formal_schema['Cols'].get(missing_table, set()))
        if available_cols:
            suggestions.append(f"Available columns in '{missing_table}': {', '.join(available_cols[:5])}")
        
        if self._need_intermediate_table(formal_schema, left_table, right_table):
            intermediate_path = self._suggest_intermediate_path(formal_schema, left_table, right_table)
            if intermediate_path:
                suggestions.append(f"SOLUTION: {intermediate_path}")
            else:
                suggestions.append(f"No direct relationship found between '{left_table}' and '{right_table}'. Check foreign key relationships.")
        
        return "; ".join(suggestions)
    
    def _validate_join(self, formal_sql:dict, formal_schema:dict):

        from collections import deque  
        
        join_conditions = formal_sql.get('J', set())
        aliases = formal_sql.get('alias', set())
        schema_tabs = formal_schema.get('Tabs', set())
        schema_cols = formal_schema.get('Cols', {})
        schema_fks = formal_schema.get('FKs', [])
        schema_pks = formal_schema.get('PKs', {})

        errors = []
        warnings = []
        resolved_joins = set()
        basic_validation_passed_for_all = True

        alias_to_real = {alias: real for real, alias in aliases}

        fk_pairs = set()
        for (src_tab, src_col), (tgt_tab, tgt_col) in schema_fks:
            if src_tab in schema_tabs and tgt_tab in schema_tabs and \
               src_col in schema_cols.get(src_tab, set()) and \
               tgt_col in schema_cols.get(tgt_tab, set()):
                 fk_pairs.add(((src_tab, src_col), (tgt_tab, tgt_col)))
                 fk_pairs.add(((tgt_tab, tgt_col), (src_tab, src_col)))
                 
        for join_condition in join_conditions:
            try:
                join_type, left_condition, operator, right_condition = join_condition
                left_table_alias, left_column = left_condition
                right_table_alias, right_column = right_condition
            except (ValueError, TypeError):
                errors.append(self._format_error(
                    "JOIN_FORMAT_ERROR",
                    "JOIN",
                    f"Malformed join condition: {join_condition}"
                ))
                basic_validation_passed_for_all = False
                continue

            left_table = alias_to_real.get(left_table_alias, left_table_alias)
            right_table = alias_to_real.get(right_table_alias, right_table_alias)

            current_join_valid = True

            if left_table not in schema_tabs:
                errors.append(self._format_error(
                    "TABLE_NOT_FOUND",
                    "JOIN",
                    f"Table '{left_table}' does not exist"
                ))
                current_join_valid = False
            if right_table not in schema_tabs:
                errors.append(self._format_error(
                    "TABLE_NOT_FOUND", 
                    "JOIN",
                    f"Table '{right_table}' does not exist"
                ))
                current_join_valid = False

            if not current_join_valid:
                basic_validation_passed_for_all = False
                continue

            def is_simple_column(col_name):
                if not isinstance(col_name, str): 
                    return False
                col_name_norm = col_name.strip().upper()
                return not (col_name_norm.startswith(('(', 'SELECT', 'CASE', 'CAST'))) and \
                       '.' not in col_name

            left_is_simple = is_simple_column(left_column)
            right_is_simple = is_simple_column(right_column)

            if left_is_simple:
                if left_column not in schema_cols.get(left_table, set()):
                    errors.append(self._format_error(
                        "COLUMN_NOT_FOUND",
                        "JOIN", 
                        f"Column '{left_column}' does not exist in table '{left_table}'"
                    ))
                    current_join_valid = False

            if right_is_simple:
                if right_column not in schema_cols.get(right_table, set()):
                    errors.append(self._format_error(
                        "COLUMN_NOT_FOUND",
                        "JOIN",
                        f"Column '{right_column}' does not exist in table '{right_table}'"
                    ))
                    current_join_valid = False

            if not current_join_valid:
                basic_validation_passed_for_all = False
                continue

            if left_is_simple and right_is_simple:
                 resolved_join_tuple = (join_type, (left_table, left_column), operator, (right_table, right_column))
                 resolved_joins.add(resolved_join_tuple)
            else:
                 basic_validation_passed_for_all = False

            resolved_join_tuple = (join_type, (left_table, left_column), operator, (right_table, right_column))
            resolved_joins.add(resolved_join_tuple)

            if operator == '=':
                is_fk_match = False
                join_pair_forward = ((left_table, left_column), (right_table, right_column))
                join_pair_backward = ((right_table, right_column), (left_table, left_column))
                if join_pair_forward in fk_pairs or join_pair_backward in fk_pairs:
                    is_fk_match = True

                is_pk_pk_match = (left_column in schema_pks.get(left_table, set()) and
                                  right_column in schema_pks.get(right_table, set()))

                if not is_fk_match and not is_pk_pk_match:
                     warnings.append(f"JOIN condition '{left_table}.{left_column} = {right_table}.{right_column}' does not match FK/PK relationship")

        if basic_validation_passed_for_all and resolved_joins:
            joined_tables = set()
            adj_list = {}

            for _, (t1, _), _, (t2, _) in resolved_joins:
                joined_tables.add(t1)
                joined_tables.add(t2)
                adj_list.setdefault(t1, set()).add(t2)
                adj_list.setdefault(t2, set()).add(t1)

            if len(joined_tables) > 1:
                start_node = next(iter(joined_tables))
                visited = {start_node}
                queue = deque([start_node])

                while queue:
                    current_node = queue.popleft()
                    for neighbor in adj_list.get(current_node, set()):
                        if neighbor not in visited:
                            visited.add(neighbor)
                            queue.append(neighbor)

                if visited != joined_tables:
                    unvisited = joined_tables - visited
                    errors.append(self._format_error(
                        "JOIN_CONNECTIVITY_ERROR",
                        "JOIN",
                        f"Tables not connected: {unvisited}",
                        "Add missing join conditions"
                    ))

        return errors, warnings


    def _validate_basic_syntax(self, sql_string):

        errors = []
        
        import re
        
        cast_pattern = r'CAST\s*\([^)]+\)'
        cast_matches = re.finditer(cast_pattern, sql_string, re.IGNORECASE)
        
        for match in cast_matches:
            cast_expr = match.group(0)
            as_count = len(re.findall(r'\bAS\b', cast_expr, re.IGNORECASE))
            if as_count > 1:
                errors.append(self._format_error(
                    "SYNTAX_ERROR",
                    "CAST",
                    f"Invalid CAST syntax: {cast_expr}",
                    "CAST should be: CAST(expression AS type)"
                ))
        
        open_parens = sql_string.count('(')
        close_parens = sql_string.count(')')
        if open_parens != close_parens:
            errors.append(self._format_error(
                "SYNTAX_ERROR",
                "PARENTHESES",
                f"Mismatched parentheses: {open_parens} open, {close_parens} close",
                "Check parentheses matching"
            ))
        
        semicolon_positions = [i for i, char in enumerate(sql_string) if char == ';']
        if semicolon_positions:
            last_semicolon = semicolon_positions[-1]
            remaining_text = sql_string[last_semicolon + 1:].strip()
            if remaining_text and not remaining_text.isspace():
                errors.append(self._format_error(
                    "SYNTAX_ERROR",
                    "SEMICOLON",
                    f"Text after semicolon: '{remaining_text}'",
                    "Semicolon should be at the end"
                ))
        
        common_keywords = {
            'SLECT': 'SELECT',
            'FORM': 'FROM', 
            'WEHRE': 'WHERE',
            'GROPU': 'GROUP',
            'OREDER': 'ORDER',
            'HAIVNG': 'HAVING'
        }
        
        sql_upper = sql_string.upper()
        for wrong, correct in common_keywords.items():
            if f' {wrong} ' in f' {sql_upper} ':
                errors.append(self._format_error(
                    "SYNTAX_ERROR",
                    "SPELLING",
                    f"Keyword misspelled: '{wrong}'",
                    f"Did you mean '{correct}'?"
                ))
        
        return errors
    
    def _similarity(self,str1, str2):
        str1_orig, str2_orig = str1, str2
        str1, str2 = str1.lower(), str2.lower()

        if str1 == str2:
            return 1.0

        def normalize_string(s):
            return re.sub(r'[^a-z0-9]', '', s)
        
        str1_normalized = normalize_string(str1)
        str2_normalized = normalize_string(str2)

        if str1_normalized == str2_normalized:
            return 0.95

        word_mappings = {
            'lowest': 'low',
            'highest': 'high', 
            'lowgrade': 'lowgrade',
            'highgrade': 'highgrade',
            'lowestgrade': 'lowgrade',
            'highestgrade': 'highgrade',
        }

        for variant, standard in word_mappings.items():
            if str1_normalized == variant and str2_normalized == standard:
                return 0.9
            if str2_normalized == variant and str1_normalized == standard:
                return 0.9

        if str1_normalized in str2_normalized or str2_normalized in str1_normalized:
            return 0.8

        def tokenize(s):
            tokens = re.findall(r'[a-z]+|\d+', s)
            return set(tokens)
        
        tokens1 = tokenize(str1_normalized)
        tokens2 = tokenize(str2_normalized)
        
        if tokens1 and tokens2:
            intersection = len(tokens1 & tokens2)
            union = len(tokens1 | tokens2)
            jaccard_sim = intersection / union if union > 0 else 0
            if jaccard_sim > 0.7:
                return 0.85

        if abs(len(str1) - len(str2)) <= 2:
            common = sum(1 for a, b in zip(str1, str2) if a == b)
            max_len = max(len(str1), len(str2))
            if max_len > 0:
                similarity = common / max_len
                if similarity > 0.6:
                    return similarity
        if str1.strip == str2.strip():
            return 0.9
        return 0.0

    
    def integrate_with_tasl(self, sl_schemas, question_id):
        question_info = self.question_json[question_id]
        db_id = question_info['db_id']
        formal_schema = self.formalize_schema(db_id)
        
        schema_errors = []
        for table, column in sl_schemas:
            if table not in formal_schema['Tabs']:
                schema_errors.append(f"A nonexistent table is selected: '{table}'")
            elif column not in formal_schema['Cols'].get(table, set()):
                schema_errors.append(f"Table '{table}' does not exist: '{column}'")
        
        if schema_errors:
            feedback = {
                'status': 'error',
                'errors': schema_errors,
                'suggestions': self._generate_schema_suggestions(formal_schema, sl_schemas)
            }
        else:
            feedback = {
                'status': 'valid',
                'schema': sl_schemas
            }
            
        return feedback
    
    def integrate_with_talog(self, formal_schema, sql, miss=False):
        
        if 'SELECT' not in sql:
            sql = 'SELECT ' + sql
        assert sql is not None, "SQL cannot be None"
        
        formal_sql, parse_error = self.parse_sql(sql)
        print("[DEBUG] Parsed SQL:", formal_sql)
        if formal_sql is None:
            return None
        join_error, _ = self._validate_join_simple(formal_sql, formal_schema)
        if join_error is not None:
            formal_sql['J'] = set()
            print("[DEBUG] JOIN validation errors:", join_error)
        else:
            print("[DEBUG] JOIN validation passed")
            
        print("[DEBUG] miss:", miss)
        if miss:
            missing_joins = self.detect_missing_joins(formal_sql, formal_schema)
            if missing_joins:
                print("[DEBUG] Missing JOINs detected:", missing_joins)
                paths = []
                FKs = formal_schema.get('FKs', [])

                fk_lookup = {}
                for (child_table, child_col), (parent_table, parent_col) in FKs:
                    key1 = (child_table, parent_table)
                    key2 = (parent_table, child_table)
                    fk_lookup[key1] = f"{child_table}.{child_col} = {parent_table}.{parent_col}"
                    fk_lookup[key2] = f"{parent_table}.{parent_col} = {child_table}.{child_col}"

                current_connections = self._get_current_connections(formal_sql['J'], formal_schema)
                existing_pairs = set()
                for table, connected_tables in current_connections.items():
                    for connected_table in connected_tables:
                        pair = tuple(sorted([table, connected_table]))
                        existing_pairs.add(pair)

                print(f"[DEBUG] Existing connections: {existing_pairs}")

                for tables in missing_joins:
                    if len(tables) < 2:
                        continue

                    # Build path with JOIN conditions, skip existing connections
                    join_segments = []
                    for i in range(len(tables) - 1):
                        current_table = tables[i]
                        next_table = tables[i + 1]

                        pair = tuple(sorted([current_table, next_table]))
                        if pair in existing_pairs:
                            print(f"[DEBUG] Skipping existing connection: {current_table} <-> {next_table}")
                            continue
                        
                        # Find the FK relationship between current and next table
                        join_condition = fk_lookup.get((current_table, next_table))
                        if not join_condition:
                            join_condition = fk_lookup.get((next_table, current_table))

                        if join_condition:
                            next_table = escape_reserved_words(next_table)
                            join_condition = escape_reserved_words(join_condition)
                            join_segments.append(f"JOIN {next_table} ON {join_condition}")
                        else:
                            # Fallback if no direct FK relationship found
                            next_table = escape_reserved_words(next_table)
                            join_segments.append(f"JOIN {next_table} (no direct FK)")

                    if join_segments:  
                        path = " ".join(join_segments)
                        paths.append(path)
                        print(f"[DEBUG] Path with missing JOIN conditions: {path}")

                return {
                    'status': 'error',
                    'phase': 'connectivity',
                    'errors': 'missing_joins',
                    'missing_joins': paths,  # Only missing JOIN conditions
                    'sql': sql
                }
        else:
            # 处理复杂SQL跳过情况
            if formal_sql is None and parse_error and "COMPLEX_SQL_SKIPPED" in parse_error:
                return {
                    'status': 'skipped',
                    'phase': 'complexity_check',
                    'reason': parse_error.replace("COMPLEX_SQL_SKIPPED: ", ""),
                    'sql': sql,
                    'message': 'SQL too complex for static validation - assume valid if no syntax errors'
                }
            
            if formal_sql is None:
                errors = f"SQL parsing failed: {parse_error}"
                return {
                    'status': 'error',
                    'phase': 'parsing',
                    'error': errors,
                    'sql': sql
                }
                
            if parse_error:
                return {
                    'status': 'error',
                    'phase': 'parsing',
                    'error': parse_error,
                    'sql': sql
                }
                
            errors, warnings = self.validate(formal_sql, formal_schema, sql_string=sql)
            
            if errors:
                return {
                    'status': 'error',
                    'phase': 'validation',
                    'errors': errors,
                    'warnings': warnings,
                    'sql': sql,
                    'suggestions': self._generate_sql_suggestions(formal_schema, formal_sql, errors)
                }
            else:
                return {
                    'status': 'valid',
                    'warnings': warnings,
                    'sql': sql
                }
            
    def _create_relation_mapping(self, formal_schema):
        relations = {
            'table_to_columns': {},
            'column_to_tables': {},
            'fk_relations': []
        }

        relations['table_to_columns'] = {
            table: list(columns) for table, columns in formal_schema['Cols'].items()
        }

        for table, columns in formal_schema['Cols'].items():
            for column in columns:
                if column not in relations['column_to_tables']:
                    relations['column_to_tables'][column] = []
                relations['column_to_tables'][column].append(table)

        relations['fk_relations'] = [
            {
                'source': {'table': src_table, 'column': src_col},
                'target': {'table': tgt_table, 'column': tgt_col}
            }
            for (src_table, src_col), (tgt_table, tgt_col) in formal_schema['FKs']
        ]
    
        return relations
    
    def _generate_schema_suggestions(self, formal_schema, sl_schemas):
        suggestions = []
        
        for table, column in sl_schemas:
            if table not in formal_schema['Tabs']:
                similar_tables = self._find_similar_names(table, formal_schema['Tabs'])
                if similar_tables:
                    suggestions.append(f"Table '{table}' does not exist, possibly referring to: {similar_tables}")
                    
            elif column not in formal_schema['Cols'].get(table, set()):
                similar_columns = self._find_similar_names(column, formal_schema['Cols'].get(table, set()))
                if similar_columns:
                    suggestions.append(f"Column '{column}' does not exist in table '{table}', possibly referring to: {similar_columns}")
        
        suggestions.append(f"Please read the {formal_schema} carefully.")
        return suggestions
    
    def _generate_sql_suggestions(self, formal_schema, formal_sql, errors):
        suggestions = []
        relation = self._create_relation_mapping(formal_schema)
        
        for error in errors:
            if "TABLE_NOT_FOUND" in error:
                import re
                match = re.search(r"'([^']+)'", error)
                if match:
                    table_name = match.group(1)
                    similar_tables = self._find_similar_names(table_name, formal_schema['Tabs'])
                    if similar_tables:
                        suggestions.append(f"Table '{table_name}' possibly referring to: {similar_tables}")
                        
            elif "COLUMN_NOT_FOUND" in error:
                import re
                col_match = re.search(r"Column\s+'([^']+)'", error)
                table_match = re.search(r"table\s+'([^']+)'", error)
                
                if col_match and table_match:
                    col_name = col_match.group(1)
                    table_name = table_match.group(1)
                    
                    similar_columns = self._find_similar_names(
                        col_name, 
                        formal_schema['Cols'].get(table_name, set())
                    )
                    
                    if similar_columns:
                        suggestions.append(f"In Table '{table_name}', Column '{col_name}' possibly referring to: {similar_columns}")
        
        suggestions.append(f"Please read the {relation} carefully.")
        return suggestions
    
    def _find_similar_names(self, name, name_set, max_suggestions=3):
        if not name_set:
            return []
            
        similarities = [(other, self._similarity(name, other)) 
                        for other in name_set]
        
        similarities.sort(key=lambda x: x[1], reverse=True)
        
        high_similarity = [s[0] for s in similarities if s[1] > 0.8][:max_suggestions]
        if high_similarity:
            return high_similarity
        
        return [s[0] for s in similarities[:max_suggestions] if s[1] > 0.3]

import re
def extract_sql(text) -> str:
    sql = text.strip()  
    if "```sql" in text:
        parts = text.split("```sql")
        if len(parts) >= 2:
            sql_part = parts[1].split("```")[0]
            sql_part.replace('\n', ' ')
            sql = sql_part.strip()

    return sql

def extract_sqls(text) -> list:
    """
    Extract SQL statements from text using multiple methods.
    Returns a list of SQL statements if found, otherwise returns an empty list.
    """
    sqls = set()
    
    # Method 1: Look for code blocks with sql tag
    if "```sql" in text:
        parts = text.split("```sql")
        for i in range(1, len(parts)):
            if "```" in parts[i]:
                sql_part = parts[i].split("```")[0]
                sql_part = sql_part.replace('\n', ' ').strip()
                if sql_part:
                    return sql_part

def update_schema(schema, schema_2):
    # Convert the original schema to a set of tuples for faster lookup
    existing_pairs = set(tuple(pair) for pair in schema)
    
    # Add new pairs from schema_2 that don't already exist in schema
    for pair in schema_2:
        if tuple(pair) not in existing_pairs:
            schema.append(pair)
    return schema


import random

def sample_prompt(prompt, keep_ratio=0.7):
    sentences = prompt.split('. ')
    sampled = random.sample(sentences, int(len(sentences) * keep_ratio))
    return '. '.join(sampled)

def connect_sql(sql, db_id:str) ->str :
    """
    Connect to the database and execute the SQL query.
    Returns the result of the query execution.
    """
    global db_root_path
    import sqlite3
    path = os.path.join(db_root_path, db_id, db_id)
    # print(path)
    conn = sqlite3.connect(f'{path}.sqlite')
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        if result is None or result == [] or len(result) == 0:
            return 'Error executing SQL: No result returned.'
        elif all(row[0] is None for row in result):
            return 'Error executing SQL: No result returned.'
        
        print("SQL executed successfully.")
        return result
    except TimeoutError:
        return 'Query timeout'
    except Exception as e:
        error = f"Error executing SQL:{str(e)}"
        return error
    finally:
        conn.close()

def safe_re_try_check(tables_in_query):
    if tables_in_query is None:
        print("[ERROR] tables_in_query is None - SQL parsing failed")
        return False
    
    if not isinstance(tables_in_query, (list, set)):
        print(f"[ERROR] tables_in_query has wrong type: {type(tables_in_query)}")
        return False
    
    if len(tables_in_query) == 0:
        print("[WARNING] No tables found in query")
        return False
    
    return True

def replace_table_by_alias(sql: str, res_sql: str, tmp_parse, formal_schema) -> str:
    res = res_sql.split('.')
    alias_res = res[0].split('no such column:')[1]
    
    alias = tmp_parse[0]['alias']
    for table, alias_table in alias:
        print(f"[DEBUG] table: {table}, alias_table: {alias_table}")
        print(alias_res)
        if alias_res.strip() == alias_table.strip():
            alias_res = table
    
    column_name = res[1] if len(res) > 1 else ""
    found_tables = []
    
    for table_name, columns in formal_schema.get('Cols', {}).items():
        if column_name in columns:
            found_tables.append(table_name)
    
    if found_tables:
        if alias_res in found_tables:
            return f"COLUMN_VALIDATION: Column '{column_name}' exists in table '{alias_res}'. Status: VALID"
        else:
            return f"TABLE_MISMATCH: Column '{column_name}' not found in table '{alias_res}'. Available in: {found_tables}. Suggestion: Change table alias or use correct table name."
    else:
        return f"COLUMN_NOT_FOUND: Column '{column_name}' does not exist in schema. Available columns: {list(formal_schema.get('Cols', {}).keys())}. Action: Check column name spelling or schema definition."


fix_index = 0
def re_try(db_id, formal_schema, sql, check_errors, validator:FormalSQLValidator, schema=None, miss = False):
    global fix_index
    
    try_times = 1
    sql_feedback = validator.integrate_with_talog(formal_schema, sql=sql)
    print(formal_schema.get('FKs', []))
    from extract.struct import get_all_function
    from check_type.check_round import detect_round_type_issues
    from check_type.check_function import check_dialect_function
    while check_errors and try_times < 5:
        
        print(f"[DEBUG] We verify sql error: {check_errors}")
        # print(f"suggest: {sql_feedback.get('suggestions', [])}")

        assert sql !=' 'or sql != ''
        
        correction_prompt = f"""
# Databse: {schema}

# Code with Errors: 
```sql
{sql}
```
## Error:
- {check_errors}
# Task:
- First, clean all comments and understand user problem and suggestion. Second, write a SQL query that follow Databse knowledge. Finally, check the SQL is correct meet the knowledge schema and solve problem.
- Let's think step by step.
- Finally, Output the corrected SQL. 
            """

        corrected_sql_response = collect_response2(prompt=correction_prompt)
        if corrected_sql_response == 'Model\'s maximum context length exceeded.':
            return 'give up!'

        sql = extract_sql(corrected_sql_response)

        sql_feedback = validator.integrate_with_talog(formal_schema, sql = sql)
        if sql_feedback is None:
            print("[ERROR] SQL feedback is None, skipping this retry")
            check_errors = None
        elif 'error' in sql_feedback:
            check_errors = sql_feedback.get('errors')
            if miss == True:
                check_errors = sql_feedback.get('missing_joins')
                try_times += 1
        else:
            check_errors = None
        if check_errors is None:
            check_errors = set()
        print(sql)

        tmp = execute_mysql_query_safe(sql)
        
        if 'Error executing SQL:' in tmp:
            fix_index += 1
            
            # dialect_function
            errors = check_dialect_function(sql)
            check_errors.add(errors)
            
            error = detect_round_type_issues(sql, source_dialect='postgres')
            if error is not None and len(error) > 0:
                for e in error:
                    issue_with_suggestion = f"{e['issue']}. Suggestion: {e['suggestion']}"
                    check_errors.add(issue_with_suggestion)
            
        else:
            print("SQL fix success!")
            fix_index += 1
            return sql
        
        if try_times >= 4:

            return 'give up!'
        try_times += 1
    
    return 'give up!'
        
def escape_reserved_words(expression: str) -> str:

    reserved_words = ['order', 'group', 'select', 'from', 'where', 'join', 'on', 'as', 'and', 'or', 'not', 'in', 'is', 'null']
    for word in reserved_words:
        if word in expression.lower():
            expression = re.sub(r'\b' + re.escape(word) + r'\b', f'"{word}"', expression, flags=re.IGNORECASE)
    return expression

if __name__ == '__main__':
    column_meaning_path = '/TA-SQL/outputs/column_meaning.json'
    # column_meaning_path = '/TA-SQL/data/spider/database/conclude_meaning-405b.json'
    mode = 'dev'
    
    print("初始化模块...")
    tasl = TASL(db_root_path, mode=mode, column_meaning_path=column_meaning_path)
    talog = TALOG(db_root_path, mode=mode)
    validator = FormalSQLValidator(db_root_path, mode)
    result = {}
    
    import time 
    start_time = time.time()
    
    try:
        i = 0
        while i < 1228:
            
            try:
                question_id = i
                question_info = talog.question_json[question_id]
                db_path = os.path.join(db_root_path, question_info['db_id'], f"{question_info['db_id']}.sqlite")
                conn = sqlite3.connect(db_path)
               
                db_id = question_info['db_id']
                formal_schema = validator.formalize_schema(db_id)

                while 1:
                    sl_schemas = tasl.get_schema(question_id)
                    schema_feedback = validator.integrate_with_tasl(sl_schemas, question_id=question_id)
                    if schema_feedback['status'] == 'valid':
                        break

                sl_schemas_prompt = ""
                if sl_schemas == []:
                    sl_schemas_prompt = f"The schemas we do not know how to extract main schemas."
                else:
                    sl_schemas_prompt = f"Incomplete schemas: {sl_schemas}"
                    
                check_schema_prompt = f"""
                # Schema: {formal_schema}
                # Question Analysis
                Question: {question_info['question']}

                # {sl_schemas_prompt}

                # Task
                    1. Analyze the question.Identify any tables or columns that were necessarily, you should ADD or maintain them. Do not remove any tables or columns.
                    2. Generate a SQL query that uses ALL and ONLY the necessary tables and columns to answer the question
                Output a complete SQL query that properly addresses the question.
                Note: Focus on identifying missing or unnecessary schema elements rather than optimizing the query logic. Let's think step by step.
                """
                
                while 1:
                    sl_schemas_2 = tasl.get_schema_check(question_id, prompts=check_schema_prompt)
                    # 验证schema
                    schema_feedback = validator.integrate_with_tasl(sl_schemas, question_id=question_id)
                    if schema_feedback['status'] == 'valid':
                        break
                sl_schemas = update_schema(sl_schemas, sl_schemas_2)

                sr, origin_sql = talog.sr2sql(question_id, sl_schemas)
                sql = extract_sqls(origin_sql)
                
                if sql is None:
                    result[i] = "give up!" + "\t----- bird -----\t" + db_id
                    continue

                with open(f'/TA-SQL/rq1/tasql-{start_time}-7b-our-{T}.txt', 'a') as f:
                    f.write(sql)
                    f.write('\n')

                
                if "SELECT" not in sql:
                    sql = "SELECT " + sql
                    
                sql_for_path = sql
                
                res_sql = connect_sql(sql, db_id)

                if 'Error executing SQL:' in res_sql:
                    
                    sql_feedback = validator.integrate_with_talog(formal_schema, sql=sql)
                    tmp_parse = validator.parse_sql(sql)
                    check_errors = None
                    if sql_feedback is not None:
                        check_errors = sql_feedback.get('errors', [])
                    
                    if check_errors is None:
                        check_errors = [res_sql]
                    elif isinstance(check_errors, list):
                        check_errors.append(res_sql)
                    else:
                        check_errors = [str(check_errors), res_sql]
                    
                    print(f"error: {check_errors}")
                    
                    if res_sql != 'Error executing SQL: No result returned.':
                        if 'no such column:' in res_sql:
                            if '.' in res_sql:
                                prompts = replace_table_by_alias(sql, res_sql, tmp_parse, formal_schema)
                                if isinstance(check_errors, str):
                                    check_errors += prompts
                                else:
                                    check_errors:list
                                    check_errors.append(prompts)
                        corrected_sql = re_try(db_id, formal_schema, sql, check_errors, validator, formal_schema)
                        
                        if corrected_sql is None or 'give up!' in corrected_sql:
                            sql = corrected_sql
                            sql_feedback = validator.integrate_with_talog(formal_schema, sql=sql_for_path, miss = True)
                            if sql_feedback is None or  'errors' not in sql_feedback:
                                result[i] = "give up!" + "\t----- bird -----\t" + db_id
                                continue
                            
                            errors = sql_feedback.get('errors') 
                            paths = None
                            if errors == 'missing_joins':
                                paths = sql_feedback.get('missing_joins')
                                
                            if paths is None or len(paths) == 0:
                                result[i] = "give up!" + "\t----- bird -----\t" + db_id
                                continue
                
                            original_sql = sql_for_path
                
                            if paths is not None and len(paths) > 0:
                                paths = list(set(paths))
                                if len(paths) > 3:
                                    paths = paths[:2]
                                from  compare_with_debug.schema import get_schema
                                schema = get_schema(db_path)
                                for path in paths:
                                    # modified_sql = edit_join_sql(original_sql, path)  
                                    prompts = f"""The SQL is missing join conditions. Please add the missing join conditions to the SQL query.We found the soultion {path}, and read the schema, note about the alias problem.
                                    """
                                    modified_sql = re_try(db_id, formal_schema, original_sql, prompts, validator=validator,schema=schema, miss=True)
                                    from connect import connect_sql_with_timeout
                                    res_sql = connect_sql_with_timeout(modified_sql, db_id)
                                    if res_sql is None:
                                        sql = modified_sql
                                        break
                                if sql is None or 'give up!' in sql:
                                    result[i] = "give up!" + "\t----- bird -----\t" + db_id
                                    continue
                        # else:
                        #     sql = corrected_sql
                    
                    final_sql = correct_sql_using_schema(sql, formal_schema, conn, validator)
                    if final_sql is None:
                        result[i] = "give up!" + "\t----- bird -----\t" + db_id
                        continue
                    else:
                        sql = final_sql
                else:

                result[i] = sql + "\t----- bird -----\t" + db_id
                
            except Exception as e:
                result[i] = "give up!" + "\t----- bird -----\t" + db_id
                
            finally:
                i += 1
                
                if i % 50 == 0:
                    with open(f'/TA-SQL/rq1/tasql-{start_time}-7b-our-{T}.json', 'w') as f:
                        json.dump(result, f, indent=4)

    except KeyboardInterrupt:
        with open(f'/TA-SQL/rq1/tasql-{start_time}-7b-our-{T}.json', 'w') as f:
            json.dump(result, f, indent=4)
        raise
    
    finally:
        with open(f'/TA-SQL/rq1/tasql-{start_time}-7b-our-{T}.json', 'w') as f:
            json.dump(result, f, indent=4)
        
        fix_info = f"fix {fix_index} "
        with open('/TA-SQL/fix.txt','w') as f:
            f.write(fix_info)
        


# if __name__ == '__main__':
#     validator = FormalSQLValidator(db_root_path, mode='dev')
#     sql_with_subqueries = """
#     SELECT
#         c.customer_name,
#         c.country,
#         (SELECT MAX(o.order_date) FROM orders AS o WHERE o.customer_id = c.customer_id) AS last_order_date,
#         recent_orders.total_amount
#     FROM
#         customers AS c
#     JOIN
#         (
#             SELECT
#                 customer_id,
#                 SUM(amount) AS total_amount
#             FROM orders
#             WHERE order_date > '2023-01-01'
#             GROUP BY customer_id
#         ) AS recent_orders
#     ON
#         c.customer_id = recent_orders.customer_id
#     WHERE
#         c.country IN (SELECT country_name FROM active_countries)
#     ORDER BY
#         last_order_date DESC;
#     """
#     db_id = 'california_schools'
#     formal_schema = validator.formalize_schema(db_id)
#     sql_feedback = validator.integrate_with_talog(formal_schema, sql=sql_with_subqueries)
