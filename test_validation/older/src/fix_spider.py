from modules_all_ds import FormalSQLValidator, connect_sql, correct_sql_using_schema, re_try, extract_sql
from typing import List, Dict
import json
import sqlite3, os

db_root_path = '/TA-SQL/data/spider/database/'
INPUT_FILE = '/TA-SQL/error_log.txt' 

INPUT_SQL_FILE = '/TA-SQL/gensql/32b/32b.txt.fix'  
OUTPUT_FILE = INPUT_SQL_FILE+'.fix' 


# INPUT_FILE = '/TA-SQL/timeout_sql.txt'
# INPUT_SQL_FILE = '/TA-SQL/outputs/predict_dev.json'
# OUTPUT_FILE = '/TA-SQL/qwen-7b/tasql/fix-7b.json'

# db_root_path = '/TA-SQL/data/dev_databases'

def send_sql(): 
    with open(INPUT_FILE, 'r') as f:
        lines = f.readlines()
        lines = [line.strip() for line in lines if line.strip()]
    
    with open(INPUT_SQL_FILE, 'r') as f:
        sqls = f.readlines()
        sqls = [sql.strip() for sql in sqls if sql.strip()]
    # sqls = []
    # with open(INPUT_SQL_FILE, 'r') as f:
    #     datas = json.load(f)
    #     for key, value in datas.items():
    #         value = value.split('\t----- bird -----\t')[0].strip()
    #         sqls.append(value)

    # with open('/TA-SQL/data/dev_databases/dev.json') as f:
    with open('/TA-SQL/data/spider/dev.json') as f:
        datas = json.load(f)
    
    mode = 'dev'
    validator = FormalSQLValidator(db_root_path, mode)
    
    for line in lines:
    # if 1:
        # line =237
        print("第%s条sql" % line)
        line_id = int(line)
        line_id -= 1
        db_id = datas[line_id]['db_id']
        sql = sqls[line_id]
        print(f"正在处理SQL: {sql}")
        print(f"对应的数据库ID: {db_id}")
        fixed_sql = fix_error(sql, db_id, validator)
        # print(f"修正后的SQL: {fixed_sql}")
        if fixed_sql == None:
            fixed_sql = sql
        sqls[line_id] = fixed_sql
        print(f"修正后的SQL: {fixed_sql}")
        
        
    assert len(sqls) == len(datas), "SQLs and datas length mismatch!"
    with open(OUTPUT_FILE, 'w') as f:
        for sql in sqls:
            f.write(sql)
            f.write('\n')
    print(f"修正后的SQL已保存到 {OUTPUT_FILE}")

def fix_error(sql, db_id, validator: FormalSQLValidator) -> str:
    formal_schema = validator.formalize_schema(db_id)
    print(f"formalize_schema: {formal_schema}")
    res_sql = connect_sql(sql, db_id)
    db_path = os.path.join(db_root_path, db_id, f"{db_id}.sqlite")
    conn = sqlite3.connect(db_path)
    
    if 'Error executing SQL:' in res_sql:
        print("SQL验证失败, validator重新生成SQL...")
        sql_feedback = validator.integrate_with_talog(formal_schema, sql=sql)
        print(f"sql 第一次验证结果: {sql_feedback}")
        check_errors = sql_feedback.get('errors')
        if check_errors is None or check_errors == set():
            check_errors = res_sql
        else:
            check_errors.add(res_sql)
        
        if res_sql != 'Error executing SQL: No result returned.':
            sql = re_try(db_id, formal_schema, sql, check_errors, validator=validator)
            sql = sql.replace('\n', ' ')
        if sql is None:
            return "give up!" 
            
        print("开始替换sql...")
        sql = correct_sql_using_schema(sql, formal_schema, conn, validator)
        if sql is None:
            return "give up!"
        print("修正后的SQL验证通过编译和表验证!")
        return sql
    else:
        # Heuristic_repair
        sql = correct_sql_using_schema(sql, formal_schema, conn, validator)
        print(f"SQL验证通过编译和表验证!{sql}")
        return sql
    

if __name__ == '__main__':
    send_sql()