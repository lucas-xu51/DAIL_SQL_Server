#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import re
from typing import Set, Tuple, Optional

def build_schema(db_path: str, schema_sql_path: Optional[str] = None):
    """从数据库提取 schema，并补充 SQL schema 文件中的外键。
    返回 (conn, formal_schema)
    formal_schema: {'Tabs': set(), 'Cols': {}, 'FKs': set()}
    """
    formal_schema = {'Tabs': set(), 'Cols': {}, 'FKs': set()}
    conn = None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        formal_schema['Tabs'] = set(t.lower() for t in tables)

        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            cols = [row[1] for row in cursor.fetchall()]
            formal_schema['Cols'][table.lower()] = set(c.lower() for c in cols)

        for table in tables:
            cursor.execute(f"PRAGMA foreign_key_list({table})")
            for row in cursor.fetchall():
                local_col = row[3]
                ref_table = row[2]
                ref_col = row[4]
                formal_schema['FKs'].add((
                    table.lower(),
                    local_col.lower(),
                    ref_table.lower(),
                    ref_col.lower()
                ))

        # print(f"✓ 从 SQLite PRAGMA 获取 {len(formal_schema['FKs'])} 个外键关系")

        # 从 SQL schema 文件补充外键定义
        if schema_sql_path:
            try:
                with open(schema_sql_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()

                create_table_pattern = r'CREATE\s+TABLE\s+"?(\w+)"?\s*\(([^)]+)\);'
                fk_pattern = r'FOREIGN\s+KEY\s*\(\s*"?(\w+)"?\s*\)\s*REFERENCES\s+"?(\w+)"?\s*\(\s*"?(\w+)"?\s*\)'

                schema_fks = set()
                for match in re.finditer(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL):
                    table_name = match.group(1).lower()
                    table_definition = match.group(2)
                    for fk_match in re.finditer(fk_pattern, table_definition, re.IGNORECASE):
                        local_col = fk_match.group(1).lower()
                        ref_table = fk_match.group(2).lower()
                        ref_col = fk_match.group(3).lower()
                        schema_fks.add((table_name, local_col, ref_table, ref_col))

                # print(f"✓ 从 SQL 文件获取 {len(schema_fks)} 个外键定义")
                formal_schema['FKs'].update(schema_fks)
            except Exception as e:
                pass  # print(f"⚠️  从 SQL 文件提取外键失败: {e}")

        # print(f"✓ 数据库加载成功: {len(formal_schema['Tabs'])} 表")
        # for table, cols in formal_schema['Cols'].items():
        #     print(f"  - {table}: {len(cols)} 列")
        # print(f"✓ 总共 {len(formal_schema['FKs'])} 个外键关系:")
        # for src_t, src_c, tgt_t, tgt_c in sorted(formal_schema['FKs']):
        #     print(f"  - {src_t}.{src_c} -> {tgt_t}.{tgt_c}")

    except Exception as e:
        print(f"✗ 数据库加载失败: {e}")
        if conn:
            conn.close()
        raise

    return conn, formal_schema
