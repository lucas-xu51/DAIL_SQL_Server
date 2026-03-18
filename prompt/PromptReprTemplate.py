from utils.utils import get_filtered_schema, get_sql_for_database, get_filtered_schema_with_examples
import json
import sqlite3


class BasicPrompt(object):
    def __init__(self, *args, **kwargs):
        # used to avoid empty init function in 0-shot prompt
        pass

    def format_target(self, example: dict):
        return self.format_question(example) + "\nSELECT "

    def format_question(self, example: dict):
        raise NotImplementedError()

    def get_extra_info(self, db_id):
        return None


class SQLPrompt(BasicPrompt):
    template_info = "/* Given the following database schema: */\n{}"
    template_question = "/* Answer the following: {} */"

    def format_question(self, example: dict):
        sqls = get_sql_for_database(example["path_db"])

        prompt_info = self.template_info.format("\n\n".join(sqls))
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n\n".join(prompt_components)
        return prompt


class SQLFilteredPrompt(BasicPrompt):
    template_info = "/* Given the following database schema: */\n{}"
    template_question = "/* Answer the following: {} */"

    def format_question(self, example: dict):
        # print(example)
        sqls = get_filtered_schema(path_db=example["path_db"], example=example)
        # print(sqls)

        prompt_info = self.template_info.format("\n\n".join(sqls))
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n\n".join(prompt_components)
        return prompt

class SQLFilteredExamplePrompt(BasicPrompt):
    template_info = "/* Given the following database schema: */\n{}"
    template_question = "/* Answer the following: {} */"

    def format_question(self, example: dict):
        sqls = get_filtered_schema_with_examples(path_db=example["path_db"], example=example)
        # print(sqls)

        prompt_info = self.template_info.format("\n\n".join(sqls))
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n\n".join(prompt_components)
        return prompt
    
# def build_filtered_schema(example):
#     tables = example.get("tables", [])
#     if not tables:
#         return "/* No table information found */"
#     column_to_table = {str(k): str(v) for k, v in example.get("column_to_table", {}).items()}
#     table_to_columns = {}
#     for col_idx, tab_idx in column_to_table.items():
#         if tab_idx != "None":
#             table_to_columns.setdefault(tab_idx, []).append(col_idx)

#     primary_keys = set()
#     for table in tables:
#         if 'table_info' in table and 'primary_key' in table['table_info']:
#             for pk in table['table_info']['primary_key']:
#                 primary_keys.add(pk)

#     table_idx_set = set()
#     column_idx_set = set()
    
#     sc_link = example.get("sc_link", {})
#     for key in sc_link.get("q_tab_match", {}):
#         try:
#             _, tab_idx = key.split(",")
#             table_idx_set.add(tab_idx)
#         except:
#             continue

#     for key in sc_link.get("q_col_match", {}):
#         try:
#             _, col_idx = key.split(",")
#             column_idx_set.add(col_idx)
#         except:
#             continue

#     use_all_columns = False
#     if table_idx_set:
#         schema_mode = "matched_table"
#     elif column_idx_set:
#         schema_mode = "matched_column"
#     else:
#         schema_mode = "full_schema"
#         use_all_columns = True
#         table_idx_set = set(str(i) for i in range(len(tables)))

#     if schema_mode == "matched_column":
#         for col_idx in column_idx_set:
#             tab_idx = column_to_table.get(col_idx)
#             if tab_idx:
#                 table_idx_set.add(tab_idx)

#     schema_parts = []
#     for table_idx in sorted(table_idx_set, key=int):
#         if int(table_idx) >= len(tables):
#             continue

#         table = tables[int(table_idx)]
#         table_name = table.get("name", f"table_{table_idx}")
#         schema = table.get("schema", [])
#         if not schema:
#             continue

#         col_defs = []
#         pk_cols = []
        
#         for col_pos, col_name in enumerate(schema):
#             col_idx = table_to_columns.get(str(table_idx), [])[col_pos] if col_pos < len(table_to_columns.get(str(table_idx), [])) else None

#             if schema_mode == "matched_column" and col_idx not in column_idx_set:
#                 continue 
            
#             col_type = "TEXT"
#             if any(x in col_name.lower() for x in ['id', 'num', 'count']):
#                 col_type = "INTEGER"
#             elif 'date' in col_name.lower():
#                 col_type = "DATE"

#             col_defs.append(f"    {col_name} {col_type}")
#             if col_name in primary_keys:
#                 pk_cols.append(col_name)

#         if col_defs:
#             if pk_cols:
#                 col_defs.append(f"    PRIMARY KEY ({', '.join(pk_cols)})")
#             schema_parts.append(f"CREATE TABLE {table_name} (\n" + ",\n".join(col_defs) + "\n)")

#     return "\n\n".join(schema_parts) if schema_parts else "/* No relevant tables/columns found */"

# class SQLPromptOptimized(SQLPrompt):
#     template_info = "/* Given the following database schema: */\n{}"
#     template_question = "/* Answer the following: {} */"

#     def format_question(self, example: dict):
#         schema_str = build_filtered_schema(example)

#         prompt_info = self.template_info.format(schema_str)
#         prompt_extra_info = self.get_extra_info(example["db_id"])
#         prompt_question = self.template_question.format(example["question"])

#         prompt_components = [prompt_info]
#         if prompt_extra_info:
#             prompt_components.append(prompt_extra_info)
#         prompt_components.append(prompt_question)

#         prompt = "\n\n".join(prompt_components)
#         return prompt
    
class SQLPromptWithExamples(SQLPrompt):
    def __init__(self, *args, examples=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.examples = examples or {}

    def format_question(self, example: dict):
        sqls = get_sql_for_database(example["path_db"])
        
        example_lines = []
        for table_sql in sqls:
            first_line = table_sql.strip().split('\n')[0]
            if not first_line.lower().startswith("create table"):
                continue
            table_name = first_line.split()[2].strip('(')
            
            lines = table_sql.strip().split('\n')[1:]
            for line in lines:
                line = line.strip().strip(',')
                if not line or line.lower().startswith('primary key') or line.lower().startswith('foreign key'):
                    continue
                col_name = line.split()[0]
                key = f"{table_name}.{col_name}"
                if key in self.examples:
                    example_str = ", ".join(map(str, self.examples[key]))
                    example_lines.append(f"-- Example values for {key}: {example_str}")
        
        prompt_info = self.template_info.format("\n\n".join(sqls) + "\n\n" + "\n".join(example_lines))
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        prompt_components = [prompt_info]
        if prompt_extra_info:
            prompt_components.append(prompt_extra_info)
        prompt_components.append(prompt_question)

        return "\n\n".join(prompt_components)
    



class TextPrompt(BasicPrompt):
    template_info = "Given the following database schema:\n" \
                  "{}"
    template_question = "Answer the following: {}"

    def format_question(self, example: dict):
        schemas = "\n".join([f"{_.name}: {', '.join(_.schema)}" for _ in example["tables"]])

        prompt_info = self.template_info.format(schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt


class NumberSignPrompt(BasicPrompt):
    template_info = "### Complete sqlite SQL query only and with no explanation\n" \
                    "### SQLite SQL tables, with their properties:\n" \
                    "#\n" \
                    "{}\n" \
                    "#"
    template_question = "### {}"

    def format_question(self, example: dict):
        schemas = "\n".join([f"# {_.name}({', '.join(_.schema)})" for _ in example["tables"]])

        prompt_info = self.template_info.format(schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt


class BaselinePrompt(BasicPrompt):
    template_info = "{}\nForeign_keys={}\n"
    template_question = "Q: \"{}\""

    def format_question(self, example: dict):
        # schemas
        schemas = "\n".join([f"Table {_.name}, columns = {_.schema}" for _ in example["tables"]]).replace("'", "")
        # foreign_keys
        foreign_keys = list()
        for table in example["tables"]:
            for pair_str in table["table_info"]["foreign_key"]:
                a, b = [_.strip() for _ in pair_str[1:-1].split(",")]
                foreign_keys.append(f"{a}={b}")

        # format prompt
        prompt_info = self.template_info.format(schemas, str(foreign_keys).replace("'", ""))
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "".join(prompt_components)
        return prompt

    def format_target(self, example: dict):
        return self.format_question(example) + "\nA: SELECT "


class InstructionPrompt(BasicPrompt):
    template_info = (
        "Below is an instruction that describes a task, paired with an input that provides further context. "
        "Write a response that appropriately completes the request.\n\n"
        "### Instruction:\nWrite a sql to answer the question \"{}\"\n\n### Input:\n{}\n"
    )
    template_question = "### Response:"

    def format_question(self, example: dict):
        schemas = "\n".join([f"{_.name}({', '.join(_.schema)})" for _ in example["tables"]])

        prompt_info = self.template_info.format(example["question"], schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            # TODO: extra_info should be after info
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt


class TextWithForeignKeyPrompt(BasicPrompt):
    template_info = "Given the following database schema:\n" \
                    "{} \n" \
                    "And their foreign keys:\n" \
                    "{}"
    template_question = "Answer the following: {}"

    def format_question(self, example: dict):
        schemas = "\n".join([f"{_.name}: {', '.join(_.schema)}" for _ in example["tables"]])
        # foreign_keys
        foreign_keys = list()
        for table in example["tables"]:
            for pair_str in table["table_info"]["foreign_key"]:
                a, b = [_.strip() for _ in pair_str[1:-1].split(",")]
                foreign_keys.append(f"{a}={b}")
        foreign_keys = f"{', '.join(foreign_keys)}"

        prompt_info = self.template_info.format(schemas, foreign_keys)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt


class NumberSignWithForeignKeyPrompt(BasicPrompt):
    template_info = "### Complete sqlite SQL query only and with no explanation\n" \
                    "### SQLite SQL tables, with their properties:\n" \
                    "#\n" \
                    "{}\n" \
                    "#\n" \
                    "### Their foreign keys:\n" \
                    "#\n" \
                    "{}\n" \
                    "#"
    template_question = "### {}"

    def format_question(self, example: dict):
        schemas = "\n".join([f"# {_.name}({', '.join(_.schema)})" for _ in example["tables"]])
        # foreign_keys
        foreign_keys = list()
        for table in example["tables"]:
            for pair_str in table["table_info"]["foreign_key"]:
                a, b = [_.strip() for _ in pair_str[1:-1].split(",")]
                foreign_keys.append(f"{a}={b}")
        foreign_keys = f"# Foreign_keys=({', '.join(foreign_keys)})"

        prompt_info = self.template_info.format(schemas, foreign_keys)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt


class BaselineWithoutForeignKeyPrompt(BasicPrompt):
    template_info = "{}\n"
    template_question = "Q: \"{}\""

    def format_question(self, example: dict):
        # schemas
        schemas = "\n".join([f"Table {_.name}, columns = {_.schema}" for _ in example["tables"]]).replace("'", "")

        # format prompt
        prompt_info = self.template_info.format(schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "".join(prompt_components)
        return prompt

    def format_target(self, example: dict):
        return self.format_question(example) + "\nA: SELECT "


class InstructionWithForeignKeyPrompt(BasicPrompt):
    template_info = (
        "Below is an instruction that describes a task, paired with an input that provides further context. "
        "Write a response that appropriately completes the request.\n\n"
        "### Instruction:\nWrite a sql to answer the question \"{}\"\n\n### Input:\n{}\nForeign Keys:{}\n"
    )
    template_question = "### Response:"

    def format_question(self, example: dict):
        schemas = "\n".join([f"{_.name}({', '.join(_.schema)})" for _ in example["tables"]])
        # foreign_keys
        foreign_keys = list()
        for table in example["tables"]:
            for pair_str in table["table_info"]["foreign_key"]:
                a, b = [_.strip() for _ in pair_str[1:-1].split(",")]
                foreign_keys.append(f"{a}={b}")
        foreign_keys = f"{', '.join(foreign_keys)}"

        prompt_info = self.template_info.format(example["question"], schemas, foreign_keys)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            # TODO: extra_info should be after info
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt


class SQLWithRulePrompt(BasicPrompt):
    template_info =   "/* Given the following database schema: */\n" \
                      "{}"
    template_question =  "/* Answer the following with no explanation: {} */"

    def format_question(self, example: dict):
        sqls = get_sql_for_database(example["path_db"])

        prompt_info = self.template_info.format("\n\n".join(sqls))
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n\n".join(prompt_components)
        return prompt


class TextWithRulePrompt(BasicPrompt):
    template_info = "Given the following database schema:\n" \
                  "{}"
    template_question = "Answer the following with no explanation: {}"

    def format_question(self, example: dict):
        schemas = "\n".join([f"{_.name}: {', '.join(_.schema)}" for _ in example["tables"]])

        prompt_info = self.template_info.format(schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt


class NumberSignWithoutRulePrompt(BasicPrompt):
    template_info = "### Complete sqlite SQL query\n" \
                    "### SQLite SQL tables, with their properties:\n" \
                    "#\n" \
                    "{}\n" \
                    "#"
    template_question = "### {}"

    def format_question(self, example: dict):
        schemas = "\n".join([f"# {_.name}({', '.join(_.schema)})" for _ in example["tables"]])

        prompt_info = self.template_info.format(schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt


class InstructionWithRulePrompt(BasicPrompt):
    template_info = (
        "Below is an instruction that describes a task, paired with an input that provides further context. "
        "Write a response that appropriately completes the request.\n\n"
        "### Instruction:\nWrite a sql only and with no explanation to answer the question \"{}\"\n\n### Input:\n{}\n"
    )
    template_question = "### Response:"

    def format_question(self, example: dict):
        schemas = "\n".join([f"{_.name}({', '.join(_.schema)})" for _ in example["tables"]])

        prompt_info = self.template_info.format(example["question"], schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            # TODO: extra_info should be after info
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt


class SQLCOTPrompt(BasicPrompt):
    template_info =   "/* Given the following database schema: */\n" \
                      "{}"
    template_question =  "/* Let's think step by step. Answer the following: {} */"

    def format_question(self, example: dict):
        sqls = get_sql_for_database(example["path_db"])

        prompt_info = self.template_info.format("\n\n".join(sqls))
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n\n".join(prompt_components)
        return prompt

    def format_target(self, example: dict):
        return self.format_question(example)


class TextCOTPrompt(BasicPrompt):
    template_info = "Given the following database schema:\n" \
                  "{}"
    template_question = "Let's think step by step. Answer the following: {}"

    def format_question(self, example: dict):
        schemas = "\n".join([f"{_.name}: {', '.join(_.schema)}" for _ in example["tables"]])

        prompt_info = self.template_info.format(schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt

    def format_target(self, example: dict):
        return self.format_question(example)


class NumberSignCOTPrompt(BasicPrompt):
    template_info = "### Let's think step by step. Complete sqlite SQL query only and with no explanation\n" \
                    "### SQLite SQL tables, with their properties:\n" \
                    "#\n" \
                    "{}\n" \
                    "#"
    template_question = "### {}"

    def format_question(self, example: dict):
        schemas = "\n".join([f"# {_.name}({', '.join(_.schema)})" for _ in example["tables"]])

        prompt_info = self.template_info.format(schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt

    def format_target(self, example: dict):
        return self.format_question(example)


class InstructionCOTPrompt(BasicPrompt):
    template_info = (
        "Below is an instruction that describes a task, paired with an input that provides further context. "
        "Write a response that appropriately completes the request.\n\n"
        "### Instruction:\nLet's think step by step. Write a sql to answer the question \"{}\"\n\n### Input:\n{}\n"
    )
    template_question = "### Response:"

    def format_question(self, example: dict):
        schemas = "\n".join([f"{_.name}({', '.join(_.schema)})" for _ in example["tables"]])

        prompt_info = self.template_info.format(example["question"], schemas)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info, prompt_question]
        else:
            # TODO: extra_info should be after info
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt

    def format_target(self, example: dict):
        return self.format_question(example)


class CBRPrompt(BasicPrompt):
    template_info = "# The following are the table names and column names needed to generate SQL:\n" \
                    "Tables: {}\n" \
                    "Columns: *, {}\n" \
                    "Foreign keys: {}"
    template_question = '# translate "{}" into SQL query only and with no explanation:'

    def format_question(self, example: dict):
        tables = ", ".join([f"{_.name}" for _ in example["tables"]])
        columns = ", ".join([f"{_.name}.{col}" for _ in example["tables"] for col in _.schema])
        # foreign_keys
        foreign_keys = list()
        for table in example["tables"]:
            for pair_str in table["table_info"]["foreign_key"]:
                a, b = [_.strip() for _ in pair_str[1:-1].split(",")]
                foreign_keys.append(f"{a}={b}")
        foreign_keys = f"{', '.join(foreign_keys)}"

        prompt_info = self.template_info.format(tables, columns, foreign_keys)
        prompt_extra_info = self.get_extra_info(example["db_id"])
        prompt_question = self.template_question.format(example["question"])

        if prompt_extra_info is None or prompt_extra_info == "":
            prompt_components = [prompt_info,prompt_question]
        else:
            prompt_components = [prompt_info, prompt_extra_info, prompt_question]

        prompt = "\n".join(prompt_components)
        return prompt