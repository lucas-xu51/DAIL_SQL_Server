import collections
import json
import os
import re
import sqlite3

from transformers import AutoTokenizer
from utils.enums import LLM
from sql_metadata import Parser


class SqliteTable(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


def get_tables(path_db):
    if not os.path.exists(path_db):
        raise RuntimeError(f"{path_db} not exists")

    # init sqlite connection
    connection = sqlite3.connect(path_db)
    cur = connection.cursor()

    # extract table information
    table_info = parse_db(path_db, cur)
    # TODO: ! add here
    table_names = get_table_names(cur=cur)

    res = list()
    for table_name in table_names:
        # schema
        schema = [_[1] for _ in cur.execute(f'PRAGMA table_info("{table_name}")')]

        # data
        data = None
        # data = cur.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchall()

        # append table
        res.append(
            SqliteTable(
                name=table_name,
                schema=schema,
                data=data,
                table_info=table_info.get(table_name, dict())
            )
        )

    cur.close()
    return res


def parse_db(path_db, cur=None):
    """Parse the sql file and extract primary and foreign keys

    :param path_file:
    :return:
    """
    table_info = dict()
    table_names = get_table_names(path_db, cur)

    for table_name in table_names:
        pks = get_primary_key(table_name, path_db,cur)
        fks = get_foreign_key(table_name, path_db, cur)

        table_info[table_name] = {
            "primary_key": pks,
            "foreign_key": fks
        }
    return table_info


def execute_query(queries, path_db=None, cur=None):
    """Execute queries and return results. Reuse cur if it's not None.

    """
    assert not (path_db is None and cur is None), "path_db and cur cannot be NoneType at the same time"

    close_in_func = False
    if cur is None:
        con = sqlite3.connect(path_db)
        cur = con.cursor()
        close_in_func = True

    if isinstance(queries, str):
        results = cur.execute(queries).fetchall()
    elif isinstance(queries, list):
        results = list()
        for query in queries:
            res = cur.execute(query).fetchall()
            results.append(res)
    else:
        raise TypeError(f"queries cannot be {type(queries)}")

    # close the connection if needed
    if close_in_func:
        con.close()

    return results

def format_foreign_key(table_name: str, res: list):
    # FROM: self key | TO: target key
    res_clean = list()
    for row in res:
        table, source, to = row[2:5]
        row_clean = f"({table_name}.{source}, {table}.{to})"
        res_clean.append(row_clean)
    return res_clean


def get_foreign_key(table_name, path_db=None, cur=None):
    res_raw = execute_query(f'PRAGMA foreign_key_list("{table_name}")', path_db, cur)
    res = format_foreign_key(table_name, res_raw)
    return res


def get_primary_key(table_name, path_db=None, cur=None):
    res_raw = execute_query(f'PRAGMA table_info("{table_name}")', path_db, cur)
    pks = list()
    for row in res_raw:
        if row[5] == 1:
            pks.append(row[1])
    return pks


def get_table_names(path_db=None, cur=None):
    """Get names of all tables within the database, and reuse cur if it's not None

    """
    table_names = execute_query(queries="SELECT name FROM sqlite_master WHERE type='table'", path_db=path_db, cur=cur)
    table_names = [_[0] for _ in table_names]
    return table_names


def filter_json(raw_response: str) -> str:
    try:
        id_s = raw_response.index("{")
        id_e = raw_response.rindex("}")
        if id_s > id_e:
            raise ValueError("Wrong json format")
        else:
            return raw_response[id_s: id_e + 1]
    except ValueError:
        raise ValueError("Wrong json format")


def cost_estimate(n_tokens: int, model):
    return LLM.costs_per_thousand[model] * n_tokens / 1000


def get_sql_for_database(path_db=None, cur=None):
    close_in_func = False
    if cur is None:
        con = sqlite3.connect(path_db)
        cur = con.cursor()
        close_in_func = True

    table_names = get_table_names(path_db, cur)

    queries = [f"SELECT sql FROM sqlite_master WHERE tbl_name='{name}'" for name in table_names]

    sqls = execute_query(queries, path_db, cur)

    if close_in_func:
        cur.close()

    return [_[0][0] for _ in sqls]

# filter both tables & columns
def get_filtered_schema(path_db=None, cur=None, example=None):
    """Extract a filtered database schema, keeping only primary keys, foreign keys, and columns involved in the query from relevant tables"""
    close_in_func = False
    # print(example)

    if cur is None:
        con = sqlite3.connect(path_db)
        cur = con.cursor()
        close_in_func = True

    # Step 1: Get all table names
    table_names = get_table_names(path_db, cur)
    
    # Step 2: Build the mapping "column index → table index" (extracted from the example)
    column_to_table = {}
    for col_idx, table_idx in example.get("column_to_table", {}).items():
        if table_idx is not None and str(table_idx).isdigit():
            column_to_table[int(col_idx)] = int(table_idx)

    # Step 3: Extract explicitly mentioned tables and columns from the example
    sc_link = example.get("sc_link", {})
    included_tables = set()  # Set of table indexes to process
    included_columns = set()  # Set of column indexes to process

    # Extract explicitly mentioned tables (from q_tab_match)
    for key in sc_link.get("q_tab_match", {}):
        try:
            _, tab_idx = key.split(",")  # Format like "2,0"
            included_tables.add(int(tab_idx))
        except (ValueError, IndexError):
            continue

    # Extract explicitly mentioned columns (from q_col_match)
    for key in sc_link.get("q_col_match", {}):
        try:
            _, col_idx = key.split(",")  # Format like "x,y"
            included_columns.add(int(col_idx))
        except (ValueError, IndexError):
            continue

    # Step 4: Add tables that contain related columns but were not explicitly mentioned
    missing_tables = set()
    for col_idx in included_columns:
        if col_idx in column_to_table:
            table_idx = column_to_table[col_idx]  # Table that the column belongs to
            if table_idx not in included_tables:
                missing_tables.add(table_idx)  # Include this table

    included_tables.update(missing_tables)

    # If no tables are specified, include all tables
    if not included_tables:
        included_tables = set(range(len(table_names)))

    # Step 5: Add related tables via foreign key relationships (to ensure referential integrity)
    # Collect foreign key queries for included tables
    fk_queries = []
    for tab_idx in included_tables:
        if tab_idx < len(table_names):
            fk_queries.append(f"PRAGMA foreign_key_list('{table_names[tab_idx]}')")
    
    fk_results = execute_query(fk_queries, path_db, cur)  # Execute foreign key queries
    
    # Add referenced tables from foreign keys
    for result in fk_results:
        for row in result:
            if len(row) >= 3:  # Ensure foreign key info is complete
                ref_table = row[2]  # Referenced table name
                if ref_table in table_names:
                    referenced_idx = table_names.index(ref_table)
                    included_tables.add(referenced_idx)  # Add related table

    # Step 6: Extract key information for each table (all columns, primary keys, foreign keys)
    table_info = {}  # Store details for each table: {table index: {columns: [], pk: [], fk: []}}
    for tab_idx in included_tables:
        if tab_idx >= len(table_names):
            continue
        table_name = table_names[tab_idx]
        
        # 6.1 Get all columns of the table (via PRAGMA table_info)
        col_query = f"PRAGMA table_info('{table_name}')"
        col_result = execute_query([col_query], path_db, cur)[0]
        all_columns = [col[1] for col in col_result]  # Column name list (index 1 is column name)

        # print(f"\nTable '{table_name}' columns extracted from the database:")
        # for col in all_columns:
        #     print(f"  - {col}")
        
        # 6.2 Extract primary key columns (pk=1 in PRAGMA table_info)
        primary_keys = [col[1] for col in col_result if col[5] == 1]  # Index 5 is pk flag
        
        # 6.3 Extract foreign key columns (via PRAGMA foreign_key_list)
        fk_query = f"PRAGMA foreign_key_list('{table_name}')"
        fk_result = execute_query([fk_query], path_db, cur)[0]
        foreign_keys = [row[3] for row in fk_result]  # Index 3 is local foreign key column name
        
        table_info[tab_idx] = {
            "columns": all_columns,
            "pk": primary_keys,
            "fk": foreign_keys
        }

    # Step 7: Determine columns to keep for each table (related columns + primary keys + foreign keys)
    keep_columns = {}  # {table index: set of column names to keep}
    for tab_idx in included_tables:
        if tab_idx not in table_info:
            continue
        ti = table_info[tab_idx]
        table_name = table_names[tab_idx]
        
        # 7.1 Extract "related columns" for the table (explicitly mentioned in the example)
        related_cols = set()
        for col_idx in included_columns:
            if column_to_table.get(col_idx) == tab_idx:  # Column belongs to this table
                col_name = example["column_names_original"][col_idx]
                if col_name in ti["columns"]:  # Validate existence in DB
                    related_cols.add(col_name)

        # print(f"\nTable '{table_name}' related columns (preprocessed vs database):")
        # for col_idx in included_columns:
        #     if column_to_table.get(col_idx) == tab_idx:
        #         preprocessed_col = example["column_names_original"][col_idx]
        #         db_col_exists = preprocessed_col in ti["columns"]
        #         print(f"  Preprocessed column: '{preprocessed_col}' -> Exists in DB: {db_col_exists}")
        
        # 7.2 Merge related columns, primary keys, and foreign keys (remove duplicates)
        must_keep = related_cols.union(ti["pk"]).union(ti["fk"])
        keep_columns[tab_idx] = must_keep
        
    # Step 8: Generate filtered CREATE statements (keep only necessary columns and constraints)
    filtered_sqls = []
    for tab_idx in included_tables:
        if tab_idx >= len(table_names):
            continue
        table_name = table_names[tab_idx]
        ti = table_info[tab_idx]
        must_keep = keep_columns[tab_idx]
        
        # 8.1 Get the original CREATE statement
        create_query = f"SELECT sql FROM sqlite_master WHERE tbl_name='{table_name}'"
        create_result = execute_query([create_query], path_db, cur)[0]
        if not create_result or not create_result[0][0]:
            continue  # Skip if no CREATE statement
        original_create = create_result[0][0]

        # 8.2 Parse the original CREATE statement, filtering column definitions
        # Example: CREATE TABLE customer (cid INT PRIMARY KEY, cname TEXT, ...)
        # Extract the content inside parentheses (table structure part)
        start = original_create.find('(') + 1
        end = original_create.rfind(')')
        if start >= end:
            filtered_sqls.append(original_create)  # If parsing fails, keep the original
            continue
        struct_part = original_create[start:end].strip()
        
        # Split column definitions and constraints (by comma, ignoring commas inside constraints)
        parts = []
        in_constraint = False  # Whether inside a constraint (e.g., FOREIGN KEY (...))
        current_part = []
        for c in struct_part:
            if c == '(':
                in_constraint = True
                current_part.append(c)
            elif c == ')':
                in_constraint = False
                current_part.append(c)
            elif c == ',' and not in_constraint:
                parts.append(''.join(current_part).strip())
                current_part = []
            else:
                current_part.append(c)
        if current_part:
            parts.append(''.join(current_part).strip())
        
        # Filter column definitions: keep only the necessary columns
        filtered_parts = []
        for part in parts:
            part_stripped = part.strip()
            part_upper = part_stripped.upper()
            # Check if it is a column definition (not a constraint)
            is_column_def = not part_upper.startswith(('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE'))
            if is_column_def and ' ' in part_stripped:
                # Extract column name (handle names with special characters)
                col_name = part_stripped.split()[0].strip().strip('`"')  # Remove quotes/backticks
                if col_name in must_keep:
                    filtered_parts.append(part)  # Keep necessary columns
            else:
                # Keep constraints (even if related columns are not kept, to avoid syntax issues)
                filtered_parts.append(part)
        
        # print(f"Table '{table_name}' filtered column definitions: {filtered_parts}")
        
        # Rebuild the CREATE statement
        new_struct = ', '.join(filtered_parts)
        new_create = f"{original_create[:start]}{new_struct}{original_create[end:]}"
        filtered_sqls.append(new_create)

    # print(f"Included table indexes: {included_tables}")
    # print(f"Columns kept per table: { {table_names[k]: v for k, v in keep_columns.items()} }")

    if close_in_func:
        cur.close()

    return filtered_sqls


# filter only tables
# def get_filtered_schema(path_db=None, cur=None, example=None):
#     """Extract filtered schema with automatic inclusion of tables containing required columns"""
#     close_in_func = False
#     if cur is None:
#         con = sqlite3.connect(path_db)
#         cur = con.cursor()
#         close_in_func = True

#     # Step 1: Get all table names and build complete schema info
#     table_names = get_table_names(path_db, cur)
    
#     # Step 2: Build column to table mapping from example
#     column_to_table = {}
#     for col_idx, table_idx in example.get("column_to_table", {}).items():
#         if table_idx is not None and str(table_idx).isdigit():
#             column_to_table[int(col_idx)] = int(table_idx)

#     # Step 3: Extract required tables and columns from example
#     sc_link = example.get("sc_link", {})
#     included_tables = set()
#     included_columns = set()

#     # Get explicitly mentioned tables
#     for key in sc_link.get("q_tab_match", {}):
#         try:
#             _, tab_idx = key.split(",")
#             included_tables.add(int(tab_idx))
#         except (ValueError, IndexError):
#             continue

#     # Get explicitly mentioned columns
#     for key in sc_link.get("q_col_match", {}):
#         try:
#             _, col_idx = key.split(",")
#             included_columns.add(int(col_idx))
#         except (ValueError, IndexError):
#             continue

#     # Step 4: Find tables containing required columns but not included
#     missing_tables = set()
#     for col_idx in included_columns:
#         if col_idx in column_to_table:
#             table_idx = column_to_table[col_idx]
#             if table_idx not in included_tables:
#                 missing_tables.add(table_idx)

#     # Step 5: Add missing tables to included_tables
#     included_tables.update(missing_tables)

#     # If no tables specified, include all tables
#     if not included_tables:
#         included_tables = set(range(len(table_names)))

#     # Step 6: Find related tables through foreign keys
#     # First get all columns from the included tables
#     queries = []
#     for tab_idx in included_tables:
#         if tab_idx < len(table_names):
#             queries.append(f"PRAGMA table_info('{table_names[tab_idx]}')")
    
#     # Execute all queries
#     execute_query(queries, path_db, cur)  # We don't need results here
    
#     # Now find foreign key relations
#     fk_queries = []
#     for tab_idx in included_tables:
#         if tab_idx < len(table_names):
#             fk_queries.append(f"PRAGMA foreign_key_list('{table_names[tab_idx]}')")
    
#     fk_results = execute_query(fk_queries, path_db, cur)
    
#     # Add referenced tables to included_tables
#     for result in fk_results:
#         for row in result:
#             if len(row) >= 3:  # Ensure row has enough elements
#                 ref_table = row[2]  # Referenced table name
#                 if ref_table in table_names:
#                     referenced_idx = table_names.index(ref_table)
#                     included_tables.add(referenced_idx)

#     # Step 7: Get CREATE statements for all included tables
#     queries = []
#     for tab_idx in included_tables:
#         if tab_idx < len(table_names):
#             queries.append(f"SELECT sql FROM sqlite_master WHERE tbl_name='{table_names[tab_idx]}'")
    
#     sqls = execute_query(queries, path_db, cur)

#     print(f"Included tables: {included_tables}")

#     if close_in_func:
#         cur.close()

#     # Filter out None or empty results
#     return [result[0][0] for result in sqls if result and result[0]]

# 
def get_filtered_schema_with_examples(path_db=None, cur=None, example=None):
    """Extract filtered database schema and add example data for retained columns (supports string, numeric, and date types)"""
    close_in_func = False
    if cur is None:
        con = sqlite3.connect(path_db)
        cur = con.cursor()
        close_in_func = True

    # Step 1: Get all table names
    table_names = get_table_names(path_db, cur)  # Assume get_table_names is already implemented
    
    # Step 2: Build "column index → table index" mapping
    column_to_table = {}
    for col_idx, table_idx in example.get("column_to_table", {}).items():
        if table_idx is not None and str(table_idx).isdigit():
            column_to_table[int(col_idx)] = int(table_idx)

    # Step 3: Extract explicitly mentioned tables and columns
    sc_link = example.get("sc_link", {})
    included_tables = set()
    included_columns = set()

    # Extract explicitly mentioned tables
    for key in sc_link.get("q_tab_match", {}):
        try:
            _, tab_idx = key.split(",")
            included_tables.add(int(tab_idx))
        except (ValueError, IndexError):
            continue

    # Extract explicitly mentioned columns
    for key in sc_link.get("q_col_match", {}):
        try:
            _, col_idx = key.split(",")
            included_columns.add(int(col_idx))
        except (ValueError, IndexError):
            continue

    # Step 4: Add tables containing relevant columns
    missing_tables = set()
    for col_idx in included_columns:
        if col_idx in column_to_table:
            table_idx = column_to_table[col_idx]
            if table_idx not in included_tables:
                missing_tables.add(table_idx)
    included_tables.update(missing_tables)

    # If no tables specified, include all tables
    if not included_tables:
        included_tables = set(range(len(table_names)))

    # Step 5: Add tables via foreign key relationships
    fk_queries = []
    for tab_idx in included_tables:
        if tab_idx < len(table_names):
            fk_queries.append(f"PRAGMA foreign_key_list('{table_names[tab_idx]}')")
    fk_results = execute_query(fk_queries, path_db, cur)  # Assume execute_query is already implemented
    for result in fk_results:
        for row in result:
            if len(row) >= 3 and row[2] in table_names:
                referenced_idx = table_names.index(row[2])
                included_tables.add(referenced_idx)

    # Step 6: Extract key info (columns, primary keys, foreign keys) for each table and determine retained columns
    table_info = {}  # {table_index: {columns: [], pk: [], fk: [], must_keep: []}}
    for tab_idx in included_tables:
        if tab_idx >= len(table_names):
            continue
        table_name = table_names[tab_idx]
        
        # Get column info
        cur.execute(f"PRAGMA table_info('{table_name}')")
        col_result = cur.fetchall()
        all_columns = [col[1] for col in col_result]
        primary_keys = [col[1] for col in col_result if col[5] == 1]  # pk flag is at index 5
        
        # Get foreign key columns
        cur.execute(f"PRAGMA foreign_key_list('{table_name}')")
        fk_result = cur.fetchall()
        foreign_keys = [row[3] for row in fk_result]  # Local foreign key column is at index 3
        
        # Determine related columns (columns mentioned in example)
        related_cols = set()
        for col_idx in included_columns:
            if column_to_table.get(col_idx) == tab_idx:
                col_name = example["column_names_original"][col_idx]
                if col_name in all_columns:
                    related_cols.add(col_name)
        
        # Columns to keep = related columns + primary keys + foreign keys
        must_keep = related_cols.union(primary_keys).union(foreign_keys)
        table_info[tab_idx] = {
            "columns": all_columns,
            "pk": primary_keys,
            "fk": foreign_keys,
            "must_keep": must_keep
        }

    # Step 7: Generate filtered schema with examples
    final_schemas = []
    for tab_idx in included_tables:
        if tab_idx not in table_info:
            continue
        ti = table_info[tab_idx]
        table_name = table_names[tab_idx]
        must_keep = ti["must_keep"]
        
        # Get original CREATE statement
        cur.execute(f"SELECT sql FROM sqlite_master WHERE tbl_name='{table_name}'")
        create_stmt = cur.fetchone()
        if not create_stmt or not create_stmt[0]:
            continue
        original_create = create_stmt[0]

        # Get example data for retained columns
        column_examples = {}  # {column_name: [example_values]}
        for col in col_result:
            col_name = col[1]
            col_type = col[2].upper() if col[2] else ""
            if col_name not in must_keep:
                continue  # Only process retained columns
            
            # Query non-null examples (up to 3)
            try:
                cur.execute(f"SELECT {col_name} FROM {table_name} WHERE {col_name} IS NOT NULL LIMIT 2")
                samples = [row[0] for row in cur.fetchall() if row[0] is not None]
                if not samples:
                    continue
            except Exception as e:
                print(f"Failed to get examples {table_name}.{col_name}: {e}")
                continue
            
            # Format examples (process based on column type)
            formatted = []
            for s in samples:
                if isinstance(s, str) or any(t in col_type for t in ["TEXT", "VARCHAR", "CHAR"]):
                    # String type: add quotes and escape inner quotes
                    escaped = s.replace('"', '\\"').replace("'", "\\'")
                    formatted.append(f"'{escaped}'")
                elif any(t in col_type for t in ["INT", "NUM", "DEC", "FLOAT"]):
                    # Numeric type: keep as is
                    formatted.append(str(s))
                elif any(t in col_type for t in ["DATE", "DATETIME", "TIMESTAMP"]):
                    # Date type: add quotes
                    formatted.append(f"'{s}'")
                else:
                    # Other types: default to adding quotes
                    formatted.append(f"'{s}'")
            column_examples[col_name] = formatted

        # Modify CREATE statement to add example comments
        start = original_create.find('(') + 1
        end = original_create.rfind(')')
        if start >= end:
            final_schemas.append(original_create)
            continue
        struct_part = original_create[start:end].strip()
        
        # Split column definitions and constraints
        parts = []
        in_constraint = False
        current_part = []
        for c in struct_part:
            if c == '(':
                in_constraint = True
                current_part.append(c)
            elif c == ')':
                in_constraint = False
                current_part.append(c)
            elif c == ',' and not in_constraint:
                parts.append(''.join(current_part).strip())
                current_part = []
            else:
                current_part.append(c)
        if current_part:
            parts.append(''.join(current_part).strip())
        
        # Filter and add examples
        filtered_parts = []
        for part in parts:
            # Process column definitions (non-constraints)
            if ' ' in part and not part.strip().upper().startswith(('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')):
                col_name = part.split()[0].strip()
                if col_name not in must_keep:
                    continue  # Filter out non-retained columns
                # Add example comment
                if col_name in column_examples:
                    example_str = ", ".join(column_examples[col_name])
                    part += f"  # e.g.: {example_str}"
                filtered_parts.append(part)
            else:
                # Keep constraints
                filtered_parts.append(part)
        
        # Rebuild CREATE statement
        new_struct = ', '.join(filtered_parts)
        new_create = f"{original_create[:start]}{new_struct}{original_create[end:]}"
        final_schemas.append(new_create)

    if close_in_func:
        cur.close()
        con.close()

    return final_schemas

# def get_filtered_schema_with_examples(path_db=None, cur=None, example=None):
#     """Extract filtered schema with automatic inclusion of tables containing required columns"""
#     close_in_func = False
#     if cur is None:
#         con = sqlite3.connect(path_db)
#         cur = con.cursor()
#         close_in_func = True

#     table_names = get_table_names(path_db, cur)
#     column_to_table = {}
#     for col_idx, table_idx in example.get("column_to_table", {}).items():
#         if table_idx is not None and str(table_idx).isdigit():
#             column_to_table[int(col_idx)] = int(table_idx)
#     sc_link = example.get("sc_link", {})
#     included_tables = set()
#     included_columns = set()
#     for key in sc_link.get("q_tab_match", {}):
#         try:
#             _, tab_idx = key.split(",")
#             included_tables.add(int(tab_idx))
#         except (ValueError, IndexError):
#             continue
#     for key in sc_link.get("q_col_match", {}):
#         try:
#             _, col_idx = key.split(",")
#             included_columns.add(int(col_idx))
#         except (ValueError, IndexError):
#             continue
#     missing_tables = set()
#     for col_idx in included_columns:
#         if col_idx in column_to_table:
#             table_idx = column_to_table[col_idx]
#             if table_idx not in included_tables:
#                 missing_tables.add(table_idx)
#     included_tables.update(missing_tables)
#     if not included_tables:
#         included_tables = set(range(len(table_names)))
#     queries = []
#     for tab_idx in included_tables:
#         if tab_idx < len(table_names):
#             queries.append(f"PRAGMA table_info('{table_names[tab_idx]}')")
#     execute_query(queries, path_db, cur)
#     fk_queries = []
#     for tab_idx in included_tables:
#         if tab_idx < len(table_names):
#             fk_queries.append(f"PRAGMA foreign_key_list('{table_names[tab_idx]}')")
#     fk_results = execute_query(fk_queries, path_db, cur)
#     for result in fk_results:
#         for row in result:
#             if len(row) >= 3:  # Ensure row has enough elements
#                 ref_table = row[2]  # Referenced table name
#                 if ref_table in table_names:
#                     referenced_idx = table_names.index(ref_table)
#                     included_tables.add(referenced_idx)

#     # Step 7: Get CREATE statements and sample data for all included tables
#     final_schemas = []
#     for tab_idx in included_tables:
#         if tab_idx >= len(table_names):
#             continue
            
#         table_name = table_names[tab_idx]
        
#         # Get CREATE statement
#         cur.execute(f"SELECT sql FROM sqlite_master WHERE tbl_name='{table_name}'")
#         create_stmt = cur.fetchone()
#         if not create_stmt or not create_stmt[0]:
#             continue
            
#         # Get column info
#         cur.execute(f"PRAGMA table_info('{table_name}')")
#         columns = cur.fetchall()
        
#         # Get sample data only for string-type columns
#         column_examples = {}
#         for col in columns:
#             col_name = col[1]
#             col_type = col[2].upper() if col[2] else ""
            
#             # Only process string-type columns
#             if any(s_type in col_type for s_type in ['TEXT', 'VARCHAR', 'CHAR', 'STRING']):
#                 try:
#                     cur.execute(f"SELECT {col_name} FROM {table_name} WHERE {col_name} IS NOT NULL LIMIT 1")
#                     samples = [row[0] for row in cur.fetchall() if row[0] is not None]
                    
#                     # Format string values with quotes, others as-is
#                     formatted_samples = []
#                     for sample in samples:
#                         if isinstance(sample, str):
#                             # Escape quotes in the string
#                             escaped = sample.replace('"', '\\"')
#                             formatted_samples.append(f'"{escaped}"')
#                         else:
#                             formatted_samples.append(str(sample))
                    
#                     if formatted_samples:
#                         column_examples[col_name] = formatted_samples
#                 except Exception as e:
#                     print(f"Error getting examples for {table_name}.{col_name}: {str(e)}")
#                     continue
        
#         # Modify CREATE statement to include examples
#         modified_stmt = create_stmt[0]
#         for col in columns:
#             col_name = col[1]
#             if col_name in column_examples:
#                 example_str = ", ".join(column_examples[col_name])
#                 comment = f"  # e.g.: {example_str}"
                
#                 # Find the column definition in CREATE statement
#                 # More robust replacement to avoid partial matches
#                 col_def_pattern = re.compile(rf"\b{re.escape(col_name)}\s+[^,\n)]+")
#                 modified_stmt = col_def_pattern.sub(
#                     lambda m: m.group() + comment, 
#                     modified_stmt
#                 )
        
#         final_schemas.append(modified_stmt)
    
#     if close_in_func:
#         cur.close()
#         con.close()
    
#     return final_schemas


# def get_filtered_schema(path_db=None, cur=None, example=None):
    # """extract the filtered schema from the database based on the example provided"""
    # close_in_func = False
    # if cur is None:
    #     con = sqlite3.connect(path_db)
    #     cur = con.cursor()
    #     close_in_func = True

    # # Get all table names
    # table_names = get_table_names(path_db, cur)

    # # Extract the tables and columns to include from the example
    # sc_link = example.get("sc_link", {})
    # included_tables = set()
    # included_columns = set()


    # # Extract the table indices to include
    # for key in sc_link.get("q_tab_match", {}):
    #     _, tab_idx = key.split(",")
    #     included_tables.add(int(tab_idx))

    # # Extract the column indices to include
    # for key in sc_link.get("q_col_match", {}):
    #     _, col_idx = key.split(",")
    #     included_columns.add(int(col_idx))

    # # If no tables are specified, include all tables
    # if not included_tables:
    #     included_tables = set(range(len(table_names)))

    # # Get the complete CREATE statements for each included table
    # queries = []
    # for tab_idx in included_tables:
    #     if tab_idx < len(table_names):
    #         queries.append(f"SELECT sql FROM sqlite_master WHERE tbl_name='{table_names[tab_idx]}'")
    
    # sqls = execute_query(queries, path_db, cur)

    # # print(sqls)
    # return [_[0][0] for _ in sqls]


# def parse_create_statement(create_stmt):
#     start = create_stmt.find("(")
#     end = create_stmt.rfind(")")
#     if start == -1 or end == -1:
#         return {}
    
#     content = create_stmt[start+1:end].strip()
#     column_defs = []
#     current = ""
#     paren_level = 0
#     for c in content:
#         if c == '(':
#             paren_level += 1
#         elif c == ')':
#             paren_level -= 1
#         if c == ',' and paren_level == 0:
#             column_defs.append(current.strip())
#             current = ""
#         else:
#             current += c
#     if current:
#         column_defs.append(current.strip())
    
#     return {i: col for i, col in enumerate(column_defs)}

# def get_global_column_index(example, table_idx, column_idx):
#     table_to_columns = example.get("table_to_columns", {})
#     columns_for_table = table_to_columns.get(str(table_idx), [])
#     if column_idx < len(columns_for_table):
#         return int(columns_for_table[column_idx])
#     return -1


def get_tokenizer(tokenizer_type: str):
    return 0
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_type, use_fast=False)
    return tokenizer


def count_tokens(string: str, tokenizer_type: str=None, tokenizer=None):
    return 0
    # if tokenizer is None:
    #     tokenizer = get_tokenizer(tokenizer_type)
    #
    # n_tokens = len(tokenizer.encode(string))
    # return n_tokens


def sql_normalization(sql):
    sql = sql.strip()
    def white_space_fix(s):
        parsed_s = Parser(s)
        s = " ".join([token.value for token in parsed_s.tokens])

        return s

    # convert everything except text between single quotation marks to lower case
    def lower(s):
        in_quotation = False
        out_s = ""
        for char in s:
            if in_quotation:
                out_s += char
            else:
                out_s += char.lower()

            if char == "'":
                if in_quotation:
                    in_quotation = False
                else:
                    in_quotation = True

        return out_s

    # remove ";"
    def remove_semicolon(s):
        if s.endswith(";"):
            s = s[:-1]
        return s

    # double quotation -> single quotation
    def double2single(s):
        return s.replace("\"", "'")

    def add_asc(s):
        pattern = re.compile(r'order by (?:\w+ \( \S+ \)|\w+\.\w+|\w+)(?: (?:\+|\-|\<|\<\=|\>|\>\=) (?:\w+ \( \S+ \)|\w+\.\w+|\w+))*')
        if "order by" in s and "asc" not in s and "desc" not in s:
            for p_str in pattern.findall(s):
                s = s.replace(p_str, p_str + " asc")

        return s

    def sql_split(s):
        while "  " in s:
            s = s.replace("  ", " ")
        s = s.strip()
        i = 0
        toks = []
        while i < len(s):
            tok = ""
            if s[i] == "'":
                tok = tok + s[i]
                i += 1
                while i < len(s) and s[i] != "'":
                    tok = tok + s[i]
                    i += 1
                if i < len(s):
                    tok = tok + s[i]
                    i += 1
            else:
                while i < len(s) and s[i] != " ":
                    tok = tok + s[i]
                    i += 1
                while i < len(s) and s[i] == " ":
                    i += 1
            toks.append(tok)
        return toks

    def remove_table_alias(s):
        tables_aliases = Parser(s).tables_aliases
        new_tables_aliases = {}
        for i in range(1, 11):
            if "t{}".format(i) in tables_aliases.keys():
                new_tables_aliases["t{}".format(i)] = tables_aliases["t{}".format(i)]
        table_names = []
        for tok in sql_split(s):
            if '.' in tok:
                table_names.append(tok.split('.')[0])
        for table_name in table_names:
            if table_name in tables_aliases.keys():
                new_tables_aliases[table_name] = tables_aliases[table_name]
        tables_aliases = new_tables_aliases

        new_s = []
        pre_tok = ""
        for tok in sql_split(s):
            if tok in tables_aliases.keys():
                if pre_tok == 'as':
                    new_s = new_s[:-1]
                elif pre_tok != tables_aliases[tok]:
                    new_s.append(tables_aliases[tok])
            elif '.' in tok:
                split_toks = tok.split('.')
                for i in range(len(split_toks)):
                    if len(split_toks[i]) > 2 and split_toks[i][0] == "'" and split_toks[i][-1] == "'":
                        split_toks[i] = split_toks[i].replace("'", "")
                        split_toks[i] = split_toks[i].lower()
                    if split_toks[i] in tables_aliases.keys():
                        split_toks[i] = tables_aliases[split_toks[i]]
                new_s.append('.'.join(split_toks))
            else:
                new_s.append(tok)
            pre_tok = tok

        # remove as
        s = new_s
        new_s = []
        for i in range(len(s)):
            if s[i] == "as":
                continue
            if i > 0 and s[i-1] == "as":
                continue
            new_s.append(s[i])
        new_s = ' '.join(new_s)

        # for k, v in tables_aliases.items():
        #     s = s.replace("as " + k + " ", "")
        #     s = s.replace(k, v)

        return new_s

    processing_func = lambda x: remove_table_alias(add_asc(lower(white_space_fix(double2single(remove_semicolon(x))))))

    return processing_func(sql.strip())


def sql2skeleton(sql: str, db_schema):
    sql = sql_normalization(sql)

    table_names_original, table_dot_column_names_original, column_names_original = [], [], []
    column_names_original.append("*")
    for table_id, table_name_original in enumerate(db_schema["table_names_original"]):
        table_names_original.append(table_name_original.lower())
        table_dot_column_names_original.append(table_name_original + ".*")
        for column_id_and_name in db_schema["column_names_original"]:
            column_id = column_id_and_name[0]
            column_name_original = column_id_and_name[1]
            table_dot_column_names_original.append(table_name_original.lower() + "." + column_name_original.lower())
            column_names_original.append(column_name_original.lower())

    parsed_sql = Parser(sql)
    new_sql_tokens = []
    for token in parsed_sql.tokens:
        # mask table names
        if token.value in table_names_original:
            new_sql_tokens.append("_")
        # mask column names
        elif token.value in column_names_original \
                or token.value in table_dot_column_names_original:
            new_sql_tokens.append("_")
        # mask string values
        elif token.value.startswith("'") and token.value.endswith("'"):
            new_sql_tokens.append("_")
        # mask positive int number
        elif token.value.isdigit():
            new_sql_tokens.append("_")
        # mask negative int number
        elif isNegativeInt(token.value):
            new_sql_tokens.append("_")
        # mask float number
        elif isFloat(token.value):
            new_sql_tokens.append("_")
        else:
            new_sql_tokens.append(token.value.strip())

    sql_skeleton = " ".join(new_sql_tokens)

    # remove JOIN ON keywords
    sql_skeleton = sql_skeleton.replace("on _ = _ and _ = _", "on _ = _")
    sql_skeleton = sql_skeleton.replace("on _ = _ or _ = _", "on _ = _")
    sql_skeleton = sql_skeleton.replace(" on _ = _", "")
    pattern3 = re.compile("_ (?:join _ ?)+")
    sql_skeleton = re.sub(pattern3, "_ ", sql_skeleton)

    # "_ , _ , ..., _" -> "_"
    while ("_ , _" in sql_skeleton):
        sql_skeleton = sql_skeleton.replace("_ , _", "_")

    # remove clauses in WHERE keywords
    ops = ["=", "!=", ">", ">=", "<", "<="]
    for op in ops:
        if "_ {} _".format(op) in sql_skeleton:
            sql_skeleton = sql_skeleton.replace("_ {} _".format(op), "_")
    while ("where _ and _" in sql_skeleton or "where _ or _" in sql_skeleton):
        if "where _ and _" in sql_skeleton:
            sql_skeleton = sql_skeleton.replace("where _ and _", "where _")
        if "where _ or _" in sql_skeleton:
            sql_skeleton = sql_skeleton.replace("where _ or _", "where _")

    # remove additional spaces in the skeleton
    while "  " in sql_skeleton:
        sql_skeleton = sql_skeleton.replace("  ", " ")

    # double check for order by
    split_skeleton = sql_skeleton.split(" ")
    for i in range(2, len(split_skeleton)):
        if split_skeleton[i-2] == "order" and split_skeleton[i-1] == "by" and split_skeleton[i] != "_":
            split_skeleton[i] = "_"
    sql_skeleton = " ".join(split_skeleton)

    return sql_skeleton


def isNegativeInt(string):
    if string.startswith("-") and string[1:].isdigit():
        return True
    else:
        return False


def isFloat(string):
    if string.startswith("-"):
        string = string[1:]

    s = string.split(".")
    if len(s) > 2:
        return False
    else:
        for s_i in s:
            if not s_i.isdigit():
                return False
        return True


def jaccard_similarity(skeleton1, skeleton2):
    tokens1 = skeleton1.strip().split(" ")
    tokens2 = skeleton2.strip().split(" ")
    total = len(tokens1) + len(tokens2)

    def list_to_dict(tokens):
        token_dict = collections.defaultdict(int)
        for t in tokens:
            token_dict[t] += 1
        return token_dict
    token_dict1 = list_to_dict(tokens1)
    token_dict2 = list_to_dict(tokens2)

    intersection = 0
    for t in token_dict1:
        if t in token_dict2:
            intersection += min(token_dict1[t], token_dict2[t])
    union = (len(tokens1) + len(tokens2)) - intersection
    return float(intersection) / union
