import os
import json
import tqdm
import sqlite3
import csv
from src.prompt_bank import dummy_sql_prompt, sr_examples, generate_sr, sr2sql
from src.llm2 import collect_response2


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
                    value_description = row['value_description'].strip() if row['value_description'] is not None else ""
                    column_info[ocn] = [column_name, column_description, column_type, value_description]

                if column_type in ['text', 'date', 'datetime']:
                    sql = f'''SELECT DISTINCT "{ocn}" FROM `{otn}` where "{ocn}" IS NOT NULL ORDER BY RANDOM()'''
                    cursor.execute(sql)
                    values = cursor.fetchall()
                    # Check if values exist and convert to string to safely get length
                    if len(values) > 0:
                        # Convert to string to safely check length
                        first_value_str = str(values[0][0]) if values[0][0] is not None else ""
                        if len(first_value_str) < 50:
                            if len(values) <= 10:
                                example_values = [str(v[0]) if v[0] is not None else "" for v in values]
                                value_prompt[f"{db_id}|{otn}|{ocn}"] = f"all possible values are {example_values}"
                            else:
                                example_values = [str(v[0]) if v[0] is not None else "" for v in values[:3]]
                                value_prompt[f"{db_id}|{otn}|{ocn}"] = f"example values are {example_values}"
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
        evidence = question['evidence']
        pk_dict, fk_dict = self.generate_pk_fk(question_id)
        db_prompt_dic = self._reconstruct_schema()
        db_prompt = db_prompt_dic[db_id]
        database_schema = self._generate_database_schema(db_prompt)
        prompt = dummy_sql_prompt.format(database_schema = database_schema, primary_key_dic = pk_dict, foreign_key_dic = fk_dict, question_prompt = q, evidence = evidence)
        dummy_sql = collect_response2(prompt=prompt, stop = 'return SQL')
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
    
    def get_schema_check(self, question_id, prompts):
        question_info = self.question_json[question_id]
        db_id = question_info['db_id']
        dummy_sql = collect_response2(prompt=prompts)
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
        # print(self.csv_info)
        for otn, ocn in sl_schemas:
            key = f"{db_id}|{otn}"
            if key not in self.csv_info or ocn not in self.csv_info[key]:
                continue
            column_name, column_description, column_type, value_description = self.csv_info[key][ocn]
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
        e = question['evidence']
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
        sr = collect_response2(prompt = sr_prompt, max_tokens=800)
        # print(sr)
        return sr_prompt, sr
    
    def sr2sql(self, question_id, sl_schemas):
        question = self.question_json[question_id]
        q = question['question']
        e = question['evidence']
        schema = ['.'.join(t) for t in sl_schemas] if sl_schemas else []
        _, sr = self.generate_sr(question_id, sl_schemas)
        sr = sr.replace('\"', '')
        database_schema = self.generate_schema_prompt(question_id, sl_schemas)
        _, fk = self.generate_pk_fk(question_id)
        sr2sql_prompt = sr2sql.format(question = q, schema = schema, evidence = e, column_description = database_schema, SR = sr, foreign_key_dic = fk)
        sr2sql_prompt = sr2sql_prompt.strip('\n')
        # print(sr2sql_prompt)
        tmp_sql = collect_response2(prompt = sr2sql_prompt)
        #postprocess the tmp_sql to valid sql
        sql = 'SELECT ' + tmp_sql.replace('\"','')
        return sr, sql
        
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
        table_info = [content for content in self.table_json if content['db_id'] == db_id][0]
        
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
    
    def parse_sql(self, sql_string):
        from sql_metadata import Parser
        """将SQL字符串解析为形式化表示: S = (T, C, J, F, G, O, L)"""
        import re
        def strip_ansi_codes(s):
            return re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', s)
        sql_string = strip_ansi_codes(sql_string)
        sql_string = normalize_sql_for_sqlite(sql_string)

        try:
            parser = Parser(sql_string)
        except Exception as e:
            return None, f"SQL parse error: {str(e)}"
        try:
            # 提取查询组件
            formal_sql = {
                'T': set(parser.tables),  # 表集合
                'C': set(),  # 列集合
                'J': set(),  # 连接条件
                'F': set(),  # 过滤条件
                'G': set(),  # 分组条件
                'O': set(),  # 排序条件
                'L': None    # 限制条件
            }

            # 提取列
            for column in parser.columns:
                # 如果列名中包含表名（如 table.column 格式）
                if '.' in column:
                    table, col = column.split('.')
                    formal_sql['C'].add((table, col))
                else:
                    # 对于没有表前缀的列，如果只有一个表，则假定它们属于该表
                    if len(parser.tables) == 1:
                        table = list(parser.tables)[0]
                        formal_sql['C'].add((table, column))
                    else:
                        # 如果有多个表，我们无法确定，保留为None
                        formal_sql['C'].add((None, column))

            return formal_sql, None

        except Exception as e:
            return None, f"SQL extract: {str(e)}"
    
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
    
    def validate(self, formal_sql):
        """验证形式化SQL是否符合形式化模式"""
        # formal_sql 是sql语句的形式化转换
        errors = []
        warnings = []
        
        formal_schema = self.build_formal_schema_from_json()
        # print(f"formal_schema: {formal_schema}")

        # 规则1: 表存在性验证
        for table in formal_sql['T']:
            if table not in formal_schema['Tabs']:
                # 建议使用可用的表
                suggestion = f"Available tables are: {', '.join(formal_schema['Tabs'])}."
                errors.append(f"Table '{table}' does not exist in schema. {suggestion}")

        # 规则2: 列存在性验证
        for table, column in formal_sql['C']:
            if table is not None:  # 明确指定了表
                if table not in formal_schema['Tabs']:
                    # 已在规则1中捕获
                    continue

                if column not in formal_schema['Cols'].get(table, set()):
                    # 查找该列存在于哪些其他表中
                    tables_with_column = [
                        t for t in formal_schema['Tabs']
                        if column in formal_schema['Cols'].get(t, set())
                    ]

                    if tables_with_column:
                        # 提供具体的建议信息
                        if len(tables_with_column) == 1:
                            suggestion = f"Column '{column}' exists in table '{tables_with_column[0]}'. Consider using this table instead or join it with '{table}'."
                            errors.append(f"Column '{column}' does not exist in table '{table}'. {suggestion}")
                            break
                        else:
                            suggestion = f"Column '{column}' exists in tables: {', '.join(tables_with_column)}. Consider using one of these tables or join them with '{table}'."
                            errors.append(f"Column '{column}' does not exist in table '{table}'. {suggestion}")
                            
                        if 'FKs' in formal_schema:
                            # # 检查是否存在外键关系，如果有，提供更具体的连接建议
                            for (src_table, src_col), (tgt_table, tgt_col) in formal_schema['FKs']:
                                if table == src_table and tgt_table in tables_with_column:
                                    suggestion += f" You can join '{table}' with '{tgt_table}' using '{src_col}' = '{tgt_col}'."
                                elif table == tgt_table and src_table in tables_with_column:
                                    suggestion += f" You can join '{table}' with '{src_table}' using '{tgt_col}' = '{src_col}'."

                            errors.append(f"Column '{column}' does not exist in table '{table}'. {suggestion}")
                    else:
                        # 如果列不存在于任何表，建议查看所有可用的列
                        available_columns = {col for cols in formal_schema['Cols'].values() for col in cols}
                        similar_columns = [col for col in available_columns if self._similarity(column, col) > 0.7]

                        if similar_columns:
                            suggestion = f"Did you mean one of these columns: {', '.join(similar_columns)}?"
                            # errors.append(f"Column '{column}' does not exist in table '{table}' or any other tables. {suggestion}")
                        else:
                            errors.append(f"Column '{column}' does not exist in table '{table}' or any other tables.")

            else:  # 未指定表，需检查列在所有表中的唯一性
                tables_with_column = [
                    t for t in formal_schema['Tabs']
                    if column in formal_schema['Cols'].get(t, set())
                ]

                if not tables_with_column:
                    # 同样提供相似列的建议
                    available_columns = {col for cols in formal_schema['Cols'].values() for col in cols}
                    similar_columns = [col for col in available_columns if self._similarity(column, col) > 0.7]

                    if similar_columns:
                        suggestion = f"Did you mean one of these columns: {', '.join(similar_columns)}?"
                        errors.append(f"Column '{column}' does not exist in any tables. please check again.")
                    else:
                        errors.append(f"Column '{column}' does not exist in any tables.")
                elif len(tables_with_column) > 1:
                    suggestion = f"Column '{column}' exists in multiple tables: {', '.join(tables_with_column)}. Please specify which table you want to use."
                    warnings.append(f"Column '{column}' is ambiguous. {suggestion}")

        return errors, warnings

    def _similarity(self, str1, str2):
        """计算两个字符串的相似度，可以用来找出可能的拼写错误"""
        # 这里可以实现一个简单的相似度算法，如Levenshtein距离或其他
        # 简化实现：
        str1, str2 = str1.lower(), str2.lower()
        if str1 == str2:
            return 1.0
        if str1 in str2 or str2 in str1:
            return 0.8
        return 0.0  # 更完善的实现应该返回一个0到1之间的相似度值
    
    def integrate_with_tasl(self, sl_schemas):
        """与TASL模块集成，验证并修正schema选择"""
        # 获取原始schema
        question_info = self.question_json[question_id]
        db_id = question_info['db_id']
        formal_schema = self.formalize_schema(db_id)
        
        # 验证schema选择
        schema_errors = []
        for table, column in sl_schemas:
            if table not in formal_schema['Tabs']:
                schema_errors.append(f"A nonexistent table is selected: '{table}'")
            elif column not in formal_schema['Cols'].get(table, set()):
                schema_errors.append(f"Table '{table}' does not exist: '{column}'")
        
        # 生成修正反馈
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
    
    def integrate_with_talog(self, talog_instance:TALOG, question_id, sl_schemas, formal_schema, sql):
        """与TALOG模块集成, 验证生成的SQL"""
        
        # print(f"Generated SQL:\n {sql}")
        if 'SELECT' not in sql:
            sql = 'SELECT ' + sql

        formal_sql, parse_error = self.parse_sql(sql)
        
        if parse_error:
            return {
                'status': 'error',
                'phase': 'parsing',
                'error': parse_error,
                'sql': sql
            }
            
        # 验证SQL
        errors, warnings = self.validate(formal_sql)
        
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
        """创建数据库中所有表和列的关系映射"""

        # 创建关系映射字典
        relations = {
            'table_to_columns': {},  # 表到列的映射
            'column_to_tables': {},  # 列到表的映射
            'fk_relations': []       # 外键关系
        }

        # 构建表到列的映射
        relations['table_to_columns'] = {
            table: list(columns) for table, columns in formal_schema['Cols'].items()
        }

        # 构建列到表的映射
        for table, columns in formal_schema['Cols'].items():
            for column in columns:
                if column not in relations['column_to_tables']:
                    relations['column_to_tables'][column] = []
                relations['column_to_tables'][column].append(table)

        # 添加外键关系
        relations['fk_relations'] = [
            {
                'source': {'table': src_table, 'column': src_col},
                'target': {'table': tgt_table, 'column': tgt_col}
            }
            for (src_table, src_col), (tgt_table, tgt_col) in formal_schema['FKs']
        ]
    
        return relations
    
    def _generate_schema_suggestions(self, formal_schema, sl_schemas):
        """生成schema选择的修正建议"""
        suggestions = []
        
        # 为每个问题生成可能的修正
        for table, column in sl_schemas:
            if table not in formal_schema['Tabs']:
                # 建议最相似的表名
                similar_tables = self._find_similar_names(table, formal_schema['Tabs'])
                if similar_tables:
                    suggestions.append(f"Table '{table}' does not exist, possibly referring to: {similar_tables}")
                    
            elif column not in formal_schema['Cols'].get(table, set()):
                # 建议表中最相似的列名
                similar_columns = self._find_similar_names(column, formal_schema['Cols'].get(table, set()))
                if similar_columns:
                    suggestions.append(f"Column '{column}' does not exist in table '{table}', possibly referring to: {similar_columns}")
        
        suggestions.append(f" Please read the {formal_schema} carefully.")
        return suggestions
    
    def _generate_sql_suggestions(self, formal_schema, formal_sql, errors):
        """生成SQL修正建议"""
        suggestions = []
        relation = self._create_relation_mapping(formal_schema)
        for error in errors:
            if "does not exist in any tables." in error:
                # 将数据库中所有的表和列映射到关系
                suggestions.append(f"The column does not exist in any tables. ")
        
        for error in errors:
            if "Table" in error and "does not exist" in error:
                # 提取表名
                import re
                match = re.search(r"'([^']+)'", error)
                if match:
                    table_name = match.group(1)
                    similar_tables = self._find_similar_names(table_name, formal_schema['Tabs'])
                    if similar_tables:
                        suggestions.append(f"Table '{table_name}' possibly referring to: {similar_tables}")
                        
            elif "Column" in error and "does not exist" in error:
                # 提取列名和表名
                import re
                col_match = re.search(r"Column\s+'([^']+)'", error)
                table_match = re.search(r"Table\s+'([^']+)'", error)
                
                if col_match and table_match:
                    col_name = col_match.group(1)
                    table_name = table_match.group(1)
                    
                    similar_columns = self._find_similar_names(
                        col_name, 
                        formal_schema['Cols'].get(table_name, set())
                    )
                    
                    if similar_columns:
                        suggestions.append(f"In Table '{table_name}', Column '{col_name}' possibly referring to: {similar_columns}")
        suggestions.append(f" Please read the {relation} carefully.")
        return suggestions
    
    def _find_similar_names(self, name, name_set, max_suggestions=3):
        """查找与给定名称最相似的名称"""
        if not name_set:
            return []
            
        # 使用Levenshtein距离计算相似度
        import Levenshtein
        
        similarities = [(other, Levenshtein.distance(name.lower(), other.lower())) 
                        for other in name_set]
        
        # 按距离排序
        similarities.sort(key=lambda x: x[1])
        
        # 返回前N个最相似的名称
        return [s[0] for s in similarities[:max_suggestions]]

import re
def extract_sql(text) -> str:
    """
    Extract SQL statements from text using multiple methods.
    Returns the SQL if found, otherwise returns the original text.
    """
    # Method 1: Look for code blocks with sql tag
    if "```sql" in text:
        parts = text.split("```sql")
        if len(parts) >= 2:
            sql_part = parts[1].split("```")[0]
            sql_part.replace('\n', ' ')
            return sql_part.strip()
    
    # Method 2: Look for generic code blocks that might contain SQL
    if "```" in text:
        code_blocks = re.findall(r"```(?:\w*)\n(.*?)\n```", text, re.DOTALL)
        for block in code_blocks:
            # Check if block contains SQL keywords
            sql_keywords = ["SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", 
                          "CREATE", "ALTER", "DROP", "JOIN", "GROUP BY", "ORDER BY"]
            if any(keyword in block.upper() for keyword in sql_keywords):
                block.replace('\n', ' ')
                return block.strip()
    
    # Method 3: Try to find SQL statements directly in text
    # Look for patterns like "SELECT * FROM table WHERE condition"
    sql_pattern = re.search(r"(SELECT\s+.+?FROM\s+.+?(?:WHERE\s+.+?)?(?:;|$))", text, re.IGNORECASE | re.DOTALL)
    if sql_pattern:
        return sql_pattern.group(1).strip()
    
    # No SQL found, return original text
    return text

def update_schema(schema, schema_2):
    # Convert the original schema to a set of tuples for faster lookup
    existing_pairs = set(tuple(pair) for pair in schema)
    
    # Add new pairs from schema_2 that don't already exist in schema
    for pair in schema_2:
        if tuple(pair) not in existing_pairs:
            schema.append(pair)
    return schema

# 用llm消除问题歧义
def Disambiguation_question(question)->str:
    prompt = f"""
    # SQL Question Analysis
        Question: {question}

        Most of the time, this SQL query is ONLY ouput single column.
        # Task
        Analyze the given SQL-related question and provide:

        1. **Ambiguity Assessment**:
           - Identify any unclear aspects in the question
           - Note multiple possible interpretations
           - if needed, using math symbols to clarify the question

        Provide your analysis in a structured format, focusing on helping clarify the exact SQL query needed.
    """
    response = collect_response2(prompt=prompt)
    return response


def execute_sql(predicted_sql, db_path):
    try:
        conn = sqlite3.connect(db_path)
        # Connect to the database
        cursor = conn.cursor()
        cursor.execute(predicted_sql)
        predicted_res = cursor.fetchall()
    except sqlite3.Error as e:
        predicted_res= e
    return str(predicted_res)

import random

def sample_prompt(prompt, keep_ratio=0.7):
    # 按句子分割
    sentences = prompt.split('. ')
    # 随机选择一部分句子
    sampled = random.sample(sentences, int(len(sentences) * keep_ratio))
    # 重新连接
    return '. '.join(sampled)

# 将SQL查询转换为SQLite兼容格式
def normalize_sql_for_sqlite(sql_string):
    """将SQL查询转换为SQLite兼容格式，特别是处理标识符引用"""
    import re
    
    # 处理带有表名前缀的列名: `表名.列名` -> 表名."列名"
    def replace_with_table_prefix(match):
        full_match = match.group(0)
        column_reference = match.group(1)
        
        # 检查是否包含表名前缀（通过寻找点号）
        if '.' in column_reference:
            table_name, column_name = column_reference.split('.', 1)
            return f'{table_name}."{column_name}"'
        else:
            # 如果没有表名前缀，简单地用双引号替换反引号
            return f'"{column_reference}"'
    
    # 使用正则表达式进行替换
    normalized_sql = re.sub(r'`([^`]*)`', replace_with_table_prefix, sql_string)
    
    return normalized_sql

if __name__ == '__main__':
    db_root_path = '/TA-SQL/data/train/train_databases'
    column_meaning_path = '/TA-SQL/data/train_column_meaning.json'
    mode = 'train'
    
    print("初始化模块...")
    tasl = TASL(db_root_path, mode=mode, column_meaning_path=column_meaning_path)
    talog = TALOG(db_root_path, mode=mode)
    validator = FormalSQLValidator(db_root_path, mode)
    result={}
    try:
    # if 1:
        errors = []
        with open('/TA-SQL/spider/mismatches.txt', 'r') as f:
            errors = f.readlines()
        # for i in range(0, 1534):
        print(errors)
        exit(0)
        for i in errors:
            question_id = i
            question_info = talog.question_json[question_id]
            db_id = question_info['db_id']
            print(f"\n第{i}个问题: {question_info['question']}")
            question_info['question'] += "\n" + "This SQL query is ONLY ouput single column."
            question_info['question'] = str(question_info['question']) + "\n" + str(Disambiguation_question(question_info['question']))
            formal_schema = validator.formalize_schema(db_id)

            print("\n第一步: TASL生成schema...")
            sl_schemas = tasl.get_schema(question_id)
            sl_schemas_prompt = ""
            if sl_schemas == []:
                sl_schemas_prompt = f"The schemas we do not know how to extract main schemas."
            else:
                sl_schemas_prompt = f"Extracted schemas: {sl_schemas}"
                
            print(f"初始生成的schema: {sl_schemas}")
            # 让LLM检查schema对question的提取出表和列是否会缺少?
            check_schema_prompt = f"""
            # Database Information
            ## Complete Schema: {formal_schema}

            # Question Analysis
            Question: {question_info['question']}
            Evidence: {question_info['evidence']}

            # {sl_schemas_prompt}

            # Task
            The above schema extraction may be incomplete. Your task is to:
            1. Analyze the question and evidence carefully
            2. Determine if any tables or columns needed to answer the question are missing from the extracted schema
            3. Identify any tables or columns that were necessarily, you should ADD or maintain them. Do not remove any tables or columns.
            4. Generate a SQL query that uses ALL and ONLY the necessary tables and columns to answer the question

            Output a complete SQL query that properly addresses the question. I will extract the final schema based on your SQL query.

            Note: Focus on identifying missing or unnecessary schema elements rather than optimizing the query logic.
            """
            sl_schemas_2 = tasl.get_schema_check(question_id, prompts=check_schema_prompt)
            
            # 提取sl_schemas_2中新的给sl_schemas
            sl_schemas = update_schema(sl_schemas, sl_schemas_2)

            print(f"LLM再次提取的schema: {sl_schemas}")
            
            # 验证schema
            # schema_feedback = validator.integrate_with_tasl(tasl, question_id)

            print("\n第二步: TALOG生成SQL...")
            sr, origin_sql = talog.sr2sql(question_id, sl_schemas)

            sql = extract_sql(origin_sql)
            assert sql !=' 'or sql != ''
            if "SELECT" not in sql:
                sql = "SELECT " + sql
            print(f"生成的SQL: {sql}")

            print("\n第三步: 验证SQL...")
            sql_feedback = validator.integrate_with_talog(talog, question_id, sl_schemas, formal_schema, sql=sql)
            print('\n')
            check_errors = []
            check_errors = sql_feedback.get('errors')
            if check_errors is not None:
                check_errors[0].split('.')
            
            try_times = 1
            while check_errors is not None and len(check_errors) >= 2 and try_times < 5:
                print("错误过多, 重新生成SQL! ")
                sql = talog.sr2sql(question_id, sl_schemas)[1]
                sql = extract_sql(sql)
                assert sql !=' 'or sql != ''
                if "SELECT" not in sql:
                    sql = "SELECT " + sql
                    print(f"生成的SQL: {sql}")
                sql_feedback = validator.integrate_with_talog(talog, question_id, sl_schemas, formal_schema, sql=sql)
                try_times += 1
                
            if try_times == 5:
                print("give up!")
                result[i] = "give up!" + "\t----- bird -----\t" + db_id
                continue

            try_times = 1
            while sql_feedback['status'] == 'error' and try_times < 5:
                print(f"sql verify error: {sql_feedback.get('errors', [sql_feedback.get('error', '')])}")
                # print(f"suggest: {sql_feedback.get('suggestions', [])}")

                prompt = talog.generate_schema_prompt(question_id, sl_schemas)

                prompt ='\n' + str(origin_sql)
                # 将错误反馈给LLM修正
                assert sql !=' 'or sql != ''
                correction_prompt = f"""
                # Knowledge1 : {formal_schema}

                # Code with Errors: 
                ## Code:
                ```sql
                {sql}
                ```
                ## Error Details:
                - {sql_feedback.get('errors', [sql_feedback.get('error', '')])}

                # Task: Fix SQL compilation errors only. 
                    - Correct syntax and reference errors without changing the query logic 
                    - Use the provided schema to verify table and column names.
                    - If column/table references are incorrect, find the proper matching names in the schema.
                    - Preserve the original query intent and structure
                    - Think step by step to identify and address each error
                Output the corrected SQL query.
                """

                print("\n请求LLM修正SQL...")
                corrected_sql_response = collect_response2(prompt=correction_prompt)

                # 提取修正后的SQL
                sql = extract_sql(corrected_sql_response)
                print(f"LLM修正后的SQL: {corrected_sql_response}")

                # sql = extract_sql(origin_sql)
                print(f"生成的SQL: {sql}")
                sql_feedback = validator.integrate_with_talog(talog, question_id, sl_schemas, formal_schema, sql = sql)

                try_times += 1

            sql = sql.replace('\n"',' ')
            sql = sql.replace('\"','')
            print("修正后的SQL验证通过编译和表验证!")
            print(f"\n编译验证后的SQL:\n {sql}")
            
            # while 1:
            #     print("\n第四步: 反思SQL...")
            #     reflection = f"""
            #     # Resource
            #     ## knowledge-base : {formal_schema}
            #     ## Evidence: {question_info['evidence']}

            #     # Answer to Question: 
            #     To address the Question: {question_info['question']}, I used the following SQL query:
            #     ```sql
            #     {sql}
            #     ```
            #     Task: Check SQL logic errors only. 
            #     ## Key points:
            #     - Please check the SQL query to ensure it is valid and meets the question.
            #     - Evidence will help you to think and check the SQL query. You should understand the relation between the evidence and the SQL. Maybe     you leave out some evidences.
            #     - You can logic with the database relationships and the SQL query. I've provided some infomation in 'knowledge-base'.
            #     - You need to check the Question filter information, and the SQL filter information.
            #     - Finally, this SQL is not absolutely incorrect, you only need to check it. if you think it is correct, please also output it.
            #     - Think step by step to identify and address each error
            #     """
            #     ans = collect_response(prompt=reflection)
            #     finaly_sql = extract_sql(ans)
            #     sql_feedback = validator.integrate_with_talog(talog, question_id, sl_schemas, formal_schema, sql=finaly_sql)
                
            #     if sql_feedback['status'] == 'valid':
            #         print("最终生成的SQL验证通过!")
            #         break
            
            # 直接连进数据库验证
            # print("\n第五步: 执行SQL...")
            # db_path = os.path.join(db_root_path, db_id + '.sqlite')
            # ans = str(execute_sql(sql, db_path=db_path))
            # # 请求LLM根据答案修改
            # prompt = f"""
            # # SQL Query Evaluation Task

            #     ## Database Schema
            #     {formal_schema}

            #     ## Original Question
            #     {question_info['question']}

            #     ## Expected Information
            #     {question_info['evidence']}

            #     ## SQL Query Used
            #     ```sql
            #     {sql}
            # # Query Results
            #     {ans}
            #     ```
            # ## Task
            #     1.Analyze whether the SQL query results correctly answer the original question
            #     2.Identify any discrepancies between the expected information and the query results
            #     3.Determine if the query logic matches the intention of the question
            #     4.If the query is correct:
            #     - Confirm that it produces the expected results
            # """
            # print("\n请求LLM修正SQL...")
            
            # sql = collect_response2(prompt=prompt)
            # sql = extract_sql(sql)
            # print(f"LLM修正后的SQL: {sql}")
            sql = normalize_sql_for_sqlite(sql)
            result[i] = sql + "\t----- bird -----\t" + db_id
            question_id += 1
    except Exception as e:
        with open('/TA-SQL/spider/predict_dev_ours_2_0.json', 'w') as f:
            json.dump(result, f, indent=4)
        print(f"发生错误: {e}")
        pass
    except KeyboardInterrupt:
        with open('/TA-SQL/spider/predict_dev_ours_2_0.json', 'w') as f:
            json.dump(result, f, indent=4)
        print("程序被中断，保存结果。")
        raise
    with open('/TA-SQL/spider/predict_dev_ours_2_0.json', 'w') as f:
            json.dump(result, f, indent=4)
    
# if __name__ == '__main__':
#     db_root_path = './data/dev_databases'
#     column_meaning_path = './outputs/column_meaning.json'
#     mode = 'dev'
#     test_module = BaseModule(db_root_path, mode)
#     # pk_dict,_ = test_module.generate_pk_fk(0)
#     # print(pk_dict)
#     question_id = 0
#     tasl = TASL(db_root_path, mode=mode,column_meaning_path=column_meaning_path)
#     talog = TALOG(db_root_path, mode=mode)
#     validator = FormalSQLValidator(db_root_path, mode=mode)
    
#     sl_schemas = tasl.get_schema(question_id)
#     sql = talog.sr2sql(question_id, sl_schemas)
#     sql = extract_sql(sql)
    
#     sql_feedback = validator.integrate_with_talog(talog, question_id, sl_schemas)
#     print(sql_feedback)
#     sql = talog.sr2sql(question_id, sl_schemas)
#     sql = extract_sql(sql)
#     print(sql)

# if __name__ == '__main__':
#     import sqlglot
#     simple_sql = "SELECT MAX((CAST(`Free Meal Count (K-12)` AS FLOAT) / `Enrollment (K-12)`)) AS HighestEligibleFreeRate\nFROM schools\nWHERE County = 'Alameda';"
#     f =FormalSQLValidator(db_root_path='./data/dev_databases', mode='dev')
#     f.integrate_with_talog(talog=None, question_id=0, sl_schemas=None)
#     a= f.parse_sql(sql_string=simple_sql)
#     print(a)

# if __name__ == '__main__':
#     db_root_path = './data/dev_databases'
#     column_meaning_path = './outputs/column_meaning.json'
#     mode = 'dev'
    
#     print("初始化模块...")
#     validator = FormalSQLValidator(db_root_path, mode)
    
#     question_info = validator.question_json[0]
#     db_id = question_info['db_id']
#     print(f"\n第0个问题: {question_info['question']}")
    
#     sql = "SELECT MAX(frpm.`Free Meal Count (K-12)` / frpm.`Enrollment (K-12)`) FROM frpm INNER JOIN schools ON frpm.`CDSCode` = schools.`CDSCode` WHERE schools.`County Name` = 'Alameda';"
#     formal_sql, _ = validator.parse_sql(sql)
#     print(formal_sql)
#     parse_error, warnings = validator.validate(formal_sql)
    
#     print(parse_error)
    
# formal_schema 不是一定准确的, 还是得去数据库找