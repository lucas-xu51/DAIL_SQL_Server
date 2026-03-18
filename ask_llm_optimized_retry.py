import argparse
import os
import json
import time
import sys
import re
from tqdm import tqdm
from utils.enums import LLM
from test_validation.sql_validator_v2 import ImprovedSQLValidator
from utils.post_process import process_duplication, get_sqls

"""
Optimized retry wrapper script with progressive error accumulation:

First attempt: Uses original full prompt (with few-shot examples)
Retry attempts: Uses only schema + previous wrong answers + error reasons

Key behaviors:
- First call: Full original prompt with few-shot examples
- Retry calls: Simplified prompt with only:
  * Database schema
  * Previous wrong SQL attempts
  * Detailed error explanations for each attempt
- Progressive error accumulation: Each retry includes ALL previous attempts and errors
- Configurable retry behavior via --max_retries parameter
- All prompts logged with timestamps for debugging
"""

QUESTION_FILE = "questions.json"


def build_validator_for_db(db_dir: str, db_id: str):
    """Build a validator for a specific database."""
    db_path = os.path.join(db_dir, db_id, f"{db_id}.sqlite")
    # 尝试多种可能的 SQL 文件名
    sql_path = None
    possible_sql_names = [f"{db_id}.sql", "schema.sql", "database.sql"]
    
    for sql_name in possible_sql_names:
        potential_path = os.path.join(db_dir, db_id, sql_name)
        if os.path.exists(potential_path):
            sql_path = potential_path
            break
    
    if not os.path.exists(db_path):
        db_path = os.path.join(db_dir, f"{db_id}.sqlite")
    
    return ImprovedSQLValidator(db_path, sql_path)


def log_prompt(log_file: str, prompt: str, attempt_num: int, qindex: int = None):
    """Log the prompt to a file with a timestamp and attempt number."""
    with open(log_file, 'a', encoding='utf-8') as f:
        attempt_type = "INITIAL" if attempt_num == 1 else f"RETRY-{attempt_num-1}"
        f.write(f"\n{'='*80}\n")
        f.write(f"TIMESTAMP: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        if qindex is not None:
            f.write(f"QUESTION_INDEX: {qindex}\n")
        f.write(f"ATTEMPT: {attempt_type}\n")
        f.write(f"{'='*80}\n")
        f.write(prompt + "\n")


def log_answer(log_file: str, qindex: int, attempt: int, sqls, validation_result=None):
    """Log the LLM answers and validation results."""
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n--- QUESTION: {qindex} | ATTEMPT: {attempt} ---\n")
        if isinstance(sqls, list):
            for k, s in enumerate(sqls, 1):
                f.write(f"Candidate {k}: {s}\n")
        else:
            f.write(f"SQL: {sqls}\n")
        
        if validation_result:
            f.write(f"Validation: {'PASSED' if validation_result.get('overall_passed') else 'FAILED'}\n")
            if not validation_result.get('overall_passed'):
                f.write(f"Errors: {validation_result}\n")


def extract_schema_from_prompt(prompt: str) -> str:
    """Extract database schema from the original prompt."""
    # Look for CREATE TABLE statements or similar schema information
    lines = prompt.split('\n')
    schema_lines = []
    in_schema = False
    
    for line in lines:
        # Start collecting when we see CREATE TABLE or database schema markers
        if any(marker in line.upper() for marker in ['CREATE TABLE', 'TABLE:', 'DATABASE SCHEMA', 'SCHEMA:']):
            in_schema = True
        
        # Stop when we hit few-shot examples (usually marked by Q: or Question:)
        if in_schema and any(marker in line for marker in ['Q:', 'Question:', '/* Answer the following:']):
            break
            
        if in_schema:
            schema_lines.append(line)
    
    # If no clear schema found, try to extract everything before examples
    if not schema_lines:
        for i, line in enumerate(lines):
            if any(marker in line for marker in ['Q:', 'Question:', 'SELECT', '/* Answer the following:']):
                schema_lines = lines[:i]
                break
        if not schema_lines:
            # Fallback: take first half of prompt
            schema_lines = lines[:len(lines)//2]
    
    return '\n'.join(schema_lines).strip()


def extract_nl_question(prompt: str) -> str:
    """Extract the NL question from the prompt."""
    # Find the last occurrence of "Answer the following:"
    matches = re.findall(r'/\*\s*Answer the following:\s*([^*]*)\*/', prompt, re.IGNORECASE | re.DOTALL)
    if matches:
        return matches[-1].strip()
    
    # Fallback: look for the question at the end of the prompt
    lines = prompt.strip().split('\n')
    for line in reversed(lines):
        if line.strip() and not line.strip().startswith('SELECT'):
            return line.strip()
    
    return ""


def build_retry_prompt(schema: str, nl_question: str, error_history: list) -> str:
    """Build a simplified retry prompt with schema + error history.
    
    Args:
        schema: Database schema
        nl_question: The natural language question
        error_history: List of dicts with 'sql' and 'error' keys
    """
    parts = [
        "You are a SQL expert. Generate a correct SQL query for the given database schema and question.",
        "",
        "DATABASE SCHEMA:",
        schema,
        ""
    ]
    
    if error_history:
        parts.append("PREVIOUS FAILED ATTEMPTS:")
        for i, attempt in enumerate(error_history, 1):
            parts.append(f"Attempt {i}:")
            parts.append(f"SQL: {attempt['sql']}")
            parts.append(f"Error: {attempt['error']}")
            parts.append("")
        
        parts.append("Please learn from these errors and generate a correct SQL query.")
        parts.append("")
    
    parts.extend([
        f"Question: {nl_question}",
        "",
        "Generate only the SQL query without any explanation:",
        "SELECT"
    ])
    
    return '\n'.join(parts)


def normalize_sql(sql: str) -> str:
    """Normalize SQL: remove extra whitespace and ensure SELECT prefix."""
    sql = " ".join(sql.replace('\n', ' ').split())
    sql = process_duplication(sql)
    if sql.startswith("SELECT"):
        return sql
    elif sql.startswith(" "):
        return "SELECT" + sql
    else:
        return "SELECT " + sql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--question", type=str, required=True, help="Path to question directory")
    parser.add_argument("--openai_api_key", type=str, required=True, help="OpenAI API key")
    parser.add_argument("--openai_group_id", type=str, default="", help="OpenAI organization ID (optional)")
    parser.add_argument("--model", type=str, choices=[LLM.TEXT_DAVINCI_003,
                                                      LLM.GPT_35_TURBO,
                                                      LLM.GPT_35_TURBO_0613,
                                                      LLM.GPT_35_TURBO_16K,
                                                      LLM.GPT_4],
                        default=LLM.GPT_35_TURBO, help="LLM model to use")
    parser.add_argument("--temperature", type=float, default=0, help="LLM temperature")
    parser.add_argument("--n", type=int, default=1, help="Number of candidate SQLs")
    parser.add_argument("--db_dir", type=str, default="dataset/spider/database", help="Path to database directory")
    parser.add_argument("--max_retries", type=int, default=2, help="Maximum number of retries when validation fails")
    args = parser.parse_args()

    questions_json = json.load(open(os.path.join(args.question, QUESTION_FILE), "r", encoding='utf-8'))
    questions = [_["prompt"] for _ in questions_json["questions"]]
    db_ids = [_["db_id"] for _ in questions_json["questions"]]

    # Import LLM runtime
    try:
        from llm.chatgpt import init_chatgpt, ask_llm
    except Exception as e:
        print("ERROR: Failed to import LLM runtime (llm.chatgpt).\n"
              f"Import error details: {e}")
        sys.exit(1)

    # Initialize OpenAI API
    init_chatgpt(args.openai_api_key, args.openai_group_id, args.model)

    out_file = f"{args.question}/RESULTS_MODEL-{args.model}_optimized_retry{args.max_retries}.txt"
    prompt_log = f"{args.question}/LLM_PROMPTS-{args.model}_optimized_retry{args.max_retries}.log"

    validators = {}
    
    # Clear the log file at start
    with open(prompt_log, 'w', encoding='utf-8') as f:
        f.write(f"LLM PROMPT LOG (OPTIMIZED RETRY) - Model: {args.model} - Max Retries: {args.max_retries}\n")
        f.write(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")

    # 预加载所有数据库并显示信息
    print("=" * 80)
    print("DATABASE ANALYSIS PHASE")
    print("=" * 80)
    
    unique_db_ids = list(set(db_ids))  # 去重获取唯一数据库ID
    print(f"Found {len(unique_db_ids)} unique databases to analyze:\n")
    
    for db_id in unique_db_ids:
        try:
            print(f"数据库名：{db_id}")
            validator = build_validator_for_db(args.db_dir, db_id)
            validators[db_id] = validator
            
            # 显示数据库信息
            schema = validator.formal_schema
            fk_count = len(schema['FKs'])
            pragma_fks = len([fk for fk in schema['FKs']])  # 实际上就是所有外键
            
            print(f"从 SQLite PRAGMA 获取 {fk_count} 个外键关系")
            print(f"✓ 数据库加载成功: {len(schema['Tabs'])} 表\n")
            
            # 显示表结构
            for table, cols in schema['Cols'].items():
                print(f"{table}: {len(cols)} 列")
            
            print(f"✓ 总共 {fk_count} 个外键关系:")
            for src_t, src_c, tgt_t, tgt_c in sorted(schema['FKs']):
                print(f"{src_t}.{src_c} -> {tgt_t}.{tgt_c}")
                
            print("-" * 60)
            
        except Exception as e:
            print(f"✗ 数据库 '{db_id}' 加载失败: {e}")
            validators[db_id] = None
            print("-" * 60)
    
    print(f"\nDatabase analysis complete. Starting LLM processing for {len(questions)} questions...")
    print("=" * 80 + "\n")

    with open(out_file, 'w', encoding='utf-8') as outf:
        # 创建进度条
        progress_bar = tqdm(total=len(questions), desc="Processing", unit="question")
        
        for i, (original_prompt, db_id) in enumerate(zip(questions, db_ids)):
            # Extract components from original prompt
            nl_question = extract_nl_question(original_prompt)
            schema = extract_schema_from_prompt(original_prompt)
            
            # 获取已预加载的验证器
            validator = validators.get(db_id)
            error_history = []  # Track all previous attempts and their errors
            
            # ===== FIRST ATTEMPT: USE ORIGINAL FULL PROMPT =====
            log_prompt(prompt_log, original_prompt, 1, i)
            
            try:
                res = ask_llm(args.model, [original_prompt], args.temperature, args.n)
                candidates = res['response'] if isinstance(res['response'], list) else [res['response']]
                current_sql = normalize_sql(candidates[0]) if candidates else "SELECT"
            except Exception as e:
                # Silently handle LLM call errors
                outf.write("SELECT\n")
                continue

            # Validate first attempt
            validation_result = None
            if validator and args.max_retries > 0:
                validation_result = validator.validate_comprehensive(current_sql)
                
            log_answer(prompt_log, i, 1, candidates, validation_result)
            
            # ===== RETRY LOOP: USE SIMPLIFIED PROMPT WITH ERROR HISTORY =====
            attempt_num = 1
            while (attempt_num <= args.max_retries and 
                   validator and 
                   validation_result and 
                   not validation_result.get('overall_passed', False)):
                
                # Add current attempt to error history
                error_desc = validator.get_natural_error_description(validation_result)
                error_history.append({
                    'sql': current_sql,
                    'error': error_desc
                })
                
                # Build simplified retry prompt
                retry_prompt = build_retry_prompt(schema, nl_question, error_history)
                attempt_num += 1
                
                # Log retry prompt
                log_prompt(prompt_log, retry_prompt, attempt_num, i)
                
                # Make retry LLM call
                try:
                    retry_res = ask_llm(args.model, [retry_prompt], args.temperature, args.n)
                    retry_candidates = retry_res['response'] if isinstance(retry_res['response'], list) else [retry_res['response']]
                    current_sql = normalize_sql(retry_candidates[0]) if retry_candidates else current_sql
                except Exception as e:
                    # Silently handle retry LLM call errors
                    break

                # Validate retry attempt
                validation_result = validator.validate_comprehensive(current_sql)
                log_answer(prompt_log, i, attempt_num, retry_candidates, validation_result)
                
                if validation_result.get('overall_passed', False):
                    break
            
            # Write final SQL immediately and flush
            outf.write(current_sql + "\n")
            outf.flush()  # 立即写入磁盘
            
            # 更新进度条
            progress_bar.update(1)
        # 关闭进度条
        progress_bar.close()
    
    print(f"\nCompleted! Results written to: {out_file}")
    print(f"Prompts logged to: {prompt_log}")


if __name__ == '__main__':
    main()