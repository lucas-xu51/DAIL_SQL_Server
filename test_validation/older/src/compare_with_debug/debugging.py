import json
import sqlite3
import os
from typing import Tuple, Any
import time

from openai import OpenAI
from schema import get_schema

def get_cursor_from_path(sqlite_path):
    try:
        if not os.path.exists(sqlite_path):
            print("Opening a new connection %s" % sqlite_path)
        connection = sqlite3.connect(sqlite_path, check_same_thread = False)
    except Exception as e:
        print(sqlite_path)
        raise e
    connection.text_factory = lambda b: b.decode(errors="ignore")
    cursor = connection.cursor()
    return cursor


def timeout_after(seconds: float):
    from multiprocessing import Process, Manager

    def func_wrapper(fn):
        
        def wrapper(*args, **kwargs):

            with Manager() as mgr:
                res = mgr.dict()
            
                def f():
                    res['ret'] = fn(*args, **kwargs)
                
                p = Process(target=f)
                p.start()
                p.join(seconds)
                if p.exitcode is None:
                    p.terminate()
                    raise TimeoutError('timeout')
                else:
                    return res['ret']

        return wrapper

    return func_wrapper

@timeout_after(10)
def exec_on_db(sqlite_path: str, query: str) -> Tuple[bool, Any]:
    cursor = get_cursor_from_path(sqlite_path)
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return True, result
    except Exception as e:
        return False, e
    finally:
        cursor.close()
        cursor.connection.close()


def check_sql_executability(generated_sql: str, db: str):
    if generated_sql.strip() == "":
        return "Error: empty string"
    try:
        success, res = exec_on_db(db, generated_sql)
        if success:
            execution_error = None
        else:
            execution_error = str(res)
        return execution_error
    except Exception as e:
        return str(e)

def load_json(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

i = 0
tokens = 0
prompt_tokens = 0
completion_tokens = 0
max_retries = 5
retry_delay = 5  # seconds

client1 = OpenAI(
      base_url = "https://integrate.api.nvidia.com/v1",
    # 1
      api_key = "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"
    # 2
    #   api_key="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"
    # 3
    # api_key = "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"
)

def call_fix(error_msg:str):
    
    # openai.api_base = "https://integrate.api.nvidia.com/v1"
    # openai.api_key = "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" 
    # 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
    system_message = "You are an SQLite experienced database expert, you can help user analysis database and text correct SQL, user will give you many valid information. You must trust user's prompts and follow prompts to code SQL or fix SQL."
    
    retry_count = 0
    global clients, i
    # client1 = clients[i]
    # i+=1
    # if i >= len(clients):
    i = 0
    
    while retry_count <= max_retries:
    # if 1:
        try:
        # if 1:
            messages = [
                {
                    'role': 'system',
                    'content': system_message
                },
                {
                    'role': 'user',
                    'content': error_msg
                }
            ]
            
            completion = client1.chat.completions.create(
              model="qwen/qwen2.5-coder-7b-instruct",
              messages=messages,
              temperature=1,
              top_p=0.8,
              max_tokens=1024,
              stream=False,
            )
            
            
            full_response = ""
            current_call_prompt_tokens = 0
            current_call_completion_tokens = 0
            current_call_total_tokens = 0
            
            # Collect response content
            if completion.choices and len(completion.choices) > 0:
                message_content = completion.choices[0].message.content
                if message_content:
                    full_response = message_content

            if hasattr(completion, 'usage') and completion.usage is not None:
                current_call_prompt_tokens = completion.usage.prompt_tokens
                current_call_completion_tokens = completion.usage.completion_tokens
                current_call_total_tokens = completion.usage.total_tokens
            
            global tokens, prompt_tokens, completion_tokens
            prompt_tokens += current_call_prompt_tokens
            completion_tokens += current_call_completion_tokens
            tokens += current_call_total_tokens
            # After the stream completes
            print(f"Prompt tokens: {prompt_tokens}")
            print(f"Completion tokens: {completion_tokens}")
            print(f"Total tokens: {tokens}")

            # print(full_response)
            return full_response
            
        except Exception as e:
            if "This model's maximum context length is" in str(e):
                # Handle the case where the model's context length is exceeded
                return "Model's maximum context length exceeded."
            print(f"Attempt {retry_count+1}/{max_retries+1} failed: {e}")
            time.sleep(retry_delay)
            retry_count += 1
        retry_count += 1
        
    return "Maximum retry count reached, unable to get response"

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
            sql_part = sql_part.replace('\n', ' ')
            return sql_part.strip()
    
    # Method 2: Look for generic code blocks that might contain SQL
    if "```" in text:
        code_blocks = re.findall(r"```(?:\w*)\n(.*?)\n```", text, re.DOTALL)
        for block in code_blocks:
            # Check if block contains SQL keywords
            sql_keywords = ["SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", 
                          "CREATE", "ALTER", "DROP", "JOIN", "GROUP BY", "ORDER BY"]
            if any(keyword in block.upper() for keyword in sql_keywords):
                block = block.replace('\n', ' ')
                return block.strip()
    
    if "---" in text:
        parts = text.split("---")
        if len(parts) >= 2:
            sql_part = parts[1].split("---")[0]
            sql_part = sql_part.replace('\n', ' ')
            return sql_part.strip()
    
    # Method 3: Try to find SQL statements directly in text
    # Look for patterns like "SELECT * FROM table WHERE condition"
    sql_pattern = re.search(r"(SELECT\s+.+?FROM\s+.+?(?:WHERE\s+.+?)?(?:;|$))", text, re.IGNORECASE | re.DOTALL)
    if sql_pattern:
        return sql_pattern.group(1).strip()
    
    # No SQL found, return original text
    return text
        

if __name__ == "__main__":
    sqls = load_json('/TA-SQL/outputs/predict_dev.json')
    fix_sqls = {}
    
    for key, value in sqls.items():
        try_times = 1  # 每个查询重置重试次数
        
        # 修正：正确解析SQL和db_id
        parts = value.split('\t----- bird -----\t')
        sql = parts[0]
        db_id = parts[1]
        
        db = '/TA-SQL/data/dev_databases/' + db_id + '/' + db_id + '.sqlite'
        msg = check_sql_executability(sql, db)
        
        schema = get_schema(db)
        
        # 修正：循环条件应该是 and 而不是 or
        while msg is not None and try_times < 5:
            msg_to_fix = f"SQL: {sql}\nDB: {db_id}\nError: {msg}\n Schema: {schema}\n Task: think step by step, Please fix the SQL."
            response = call_fix(msg_to_fix)
            sql = extract_sql(response)
            msg = check_sql_executability(sql, db)
            try_times += 1
        if msg is None:
            print("修复第%s条SQL: %s 成功! " % (key, sql))
        else:
            print("修复第%s条SQL: %s 失败!" % (key, sql))
        # 修正：使用db_id变量而不是sql['db_id']
        sqls[key] = sql + '\t----- bird -----\t' + db_id
        
    
    # 修正：输出文件路径
    with open('/TA-SQL/compare_with_debug/gensql/debugging-fixed1.json', 'w') as f:
        json.dump(sqls, f, indent=4, ensure_ascii=False)
