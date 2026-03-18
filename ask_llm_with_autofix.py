import argparse
import os
import json
import time
import sys
import re
from utils.enums import LLM
from test_validation.sql_validator_v2 import ImprovedSQLValidator
from utils.post_process import process_duplication, get_sqls

"""
Auto-fix enhanced wrapper script with progressive error handling:

This script implements a three-tier error correction strategy:
1. Auto-fix: Automatically fix simple errors (syntax, case, typos)
2. LLM retry: For complex errors that require semantic understanding
3. Progressive retry: Accumulate error history for better context

Key behaviors:
- First attempt: Uses original full prompt (with few-shot examples)
- Auto-fix attempt: Tries to automatically fix simple errors before LLM retry
- LLM retry attempts: Uses simplified prompt with schema + error history
- Progressive error accumulation: Each retry includes ALL previous attempts
- Comprehensive logging: All steps logged with timestamps
"""

QUESTION_FILE = "questions.json"


def build_validator_for_db(db_dir: str, db_id: str):
    """Build a validator for a specific database."""
    db_path = os.path.join(db_dir, db_id, f"{db_id}.sqlite")
    sql_path = os.path.join(db_dir, db_id, f"{db_id}.sql")
    if not os.path.exists(db_path):
        db_path = os.path.join(db_dir, f"{db_id}.sqlite")
    if not os.path.exists(sql_path):
        sql_path = None
    return ImprovedSQLValidator(db_path, sql_path)


def log_prompt(log_file: str, prompt: str, attempt_num: int, qindex: int = None):
    """Log the prompt to a file with a timestamp and attempt number."""
    with open(log_file, 'a', encoding='utf-8') as f:
        attempt_type = "INITIAL" if attempt_num == 1 else f"LLM-RETRY-{attempt_num-1}"
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


def log_autofix_attempt(log_file: str, qindex: int, original_sql: str, fixed_sql: str, 
                       fix_operations: list, needs_llm: bool):
    """Log auto-fix attempts."""
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n--- AUTO-FIX ATTEMPT for QUESTION {qindex} ---\n")
        f.write(f"Original SQL: {original_sql}\n")
        f.write(f"Fixed SQL: {fixed_sql}\n")
        f.write(f"Fix Operations:\n")
        for op in fix_operations:
            f.write(f"  - {op}\n")
        f.write(f"Needs LLM: {needs_llm}\n")


def extract_schema_from_prompt(prompt: str) -> str:
    """Extract database schema from the original prompt."""
    lines = prompt.split('\n')
    schema_lines = []
    in_schema = False
    
    for line in lines:
        # Start collecting when we see CREATE TABLE or similar schema markers
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
    """Build a simplified retry prompt with schema + error history."""
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
    parser.add_argument("--openai_group_id", type=str, default="org-ktBefi7n9aK7sZjwc2R9G1Wo", help="OpenAI organization ID")
    parser.add_argument("--model", type=str, choices=[LLM.TEXT_DAVINCI_003,
                                                      LLM.GPT_35_TURBO,
                                                      LLM.GPT_35_TURBO_0613,
                                                      LLM.GPT_35_TURBO_16K,
                                                      LLM.GPT_4],
                        default=LLM.GPT_35_TURBO, help="LLM model to use")
    parser.add_argument("--temperature", type=float, default=0, help="LLM temperature")
    parser.add_argument("--n", type=int, default=1, help="Number of candidate SQLs")
    parser.add_argument("--db_dir", type=str, default="dataset/spider/database", help="Path to database directory")
    parser.add_argument("--max_retries", type=int, default=2, help="Maximum number of LLM retries when auto-fix fails")
    parser.add_argument("--enable_autofix", action="store_true", default=True, help="Enable auto-fix before LLM retry")
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

    autofix_str = "autofix" if args.enable_autofix else "nofix"
    out_file = f"{args.question}/RESULTS_MODEL-{args.model}_{autofix_str}_retry{args.max_retries}.txt"
    prompt_log = f"{args.question}/LLM_PROMPTS-{args.model}_{autofix_str}_retry{args.max_retries}.log"

    validators = {}
    
    # Statistics tracking
    stats = {
        'total': 0,
        'first_attempt_success': 0,
        'autofix_success': 0,
        'llm_retry_success': 0,
        'final_failures': 0,
        'autofix_attempts': 0,
        'llm_retry_attempts': 0
    }
    
    # Clear the log file at start
    with open(prompt_log, 'w', encoding='utf-8') as f:
        f.write(f"LLM PROMPT LOG (AUTO-FIX + RETRY) - Model: {args.model}\n")
        f.write(f"Auto-fix enabled: {args.enable_autofix} | Max LLM Retries: {args.max_retries}\n")
        f.write(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")

    with open(out_file, 'w', encoding='utf-8') as outf:
        for i, (original_prompt, db_id) in enumerate(zip(questions, db_ids)):
            print(f"\n[{i:3d}] Processing question for db_id '{db_id}'")
            stats['total'] += 1
            
            # Extract components from original prompt
            nl_question = extract_nl_question(original_prompt)
            schema = extract_schema_from_prompt(original_prompt)
            
            # Prepare validator for this DB
            if db_id not in validators:
                try:
                    validators[db_id] = build_validator_for_db(args.db_dir, db_id)
                except Exception as e:
                    print(f"[{i:3d}] WARNING: Cannot build validator for db_id '{db_id}': {e}")
                    validators[db_id] = None

            validator = validators[db_id]
            error_history = []  # Track all previous attempts and their errors
            
            # ===== FIRST ATTEMPT: USE ORIGINAL FULL PROMPT =====
            log_prompt(prompt_log, original_prompt, 1, i)
            
            try:
                res = ask_llm(args.model, [original_prompt], args.temperature, args.n)
                candidates = res['response'] if isinstance(res['response'], list) else [res['response']]
                current_sql = normalize_sql(candidates[0]) if candidates else "SELECT"
            except Exception as e:
                print(f"[{i:3d}] ERROR: Initial LLM call failed: {e}")
                outf.write("SELECT\n")
                continue

            # Validate first attempt
            validation_result = None
            if validator and args.max_retries > 0:
                validation_result = validator.validate_comprehensive(current_sql)
                
            log_answer(prompt_log, i, 1, candidates, validation_result)
            
            # Check if first attempt succeeded
            if validation_result and validation_result.get('overall_passed', False):
                print(f"[{i:3d}] ✓ PASSED on first attempt")
                stats['first_attempt_success'] += 1
                outf.write(current_sql + "\n")
                continue
            
            # ===== TRY AUTO-FIX BEFORE LLM RETRY =====
            if (validator and args.enable_autofix and validation_result and 
                not validation_result.get('overall_passed', False)):
                
                print(f"[{i:3d}] Initial validation failed, trying auto-fix...")
                stats['autofix_attempts'] += 1
                
                fixed_sql, fix_operations, needs_llm = validator.auto_fix_sql(current_sql)
                
                # Log auto-fix attempt
                log_autofix_attempt(prompt_log, i, current_sql, fixed_sql, fix_operations, needs_llm)
                
                if fixed_sql != current_sql:
                    current_sql = fixed_sql
                    validation_result = validator.validate_comprehensive(current_sql)
                    
                    if validation_result.get('overall_passed', False):
                        print(f"[{i:3d}] ✓ AUTO-FIX SUCCESSFUL! Fixed with: {', '.join(fix_operations[:2])}")
                        stats['autofix_success'] += 1
                        outf.write(current_sql + "\n")
                        continue
                    else:
                        print(f"[{i:3d}] ⚠️ Auto-fix partial: {', '.join(fix_operations[:2])}")
                
                if not needs_llm:
                    print(f"[{i:3d}] No auto-fix possible, using result as-is")
                    outf.write(current_sql + "\n")
                    continue
            
            # ===== LLM RETRY LOOP: USE SIMPLIFIED PROMPT WITH ERROR HISTORY =====
            # Only retry if auto-fix failed and LLM is needed
            attempt_num = 1
            while (attempt_num <= args.max_retries and 
                   validator and 
                   validation_result and 
                   not validation_result.get('overall_passed', False)):
                
                stats['llm_retry_attempts'] += 1
                
                # Add current attempt to error history
                error_desc = validator.get_natural_error_description(validation_result)
                error_history.append({
                    'sql': current_sql,
                    'error': error_desc
                })
                
                print(f"[{i:3d}] Auto-fix failed, LLM retry {attempt_num}/{args.max_retries}")
                
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
                    print(f"[{i:3d}] ERROR: LLM retry {attempt_num-1} failed: {e}")
                    break

                # Validate retry attempt
                validation_result = validator.validate_comprehensive(current_sql)
                log_answer(prompt_log, i, attempt_num, retry_candidates, validation_result)
                
                if validation_result.get('overall_passed', False):
                    print(f"[{i:3d}] ✓ LLM RETRY SUCCESSFUL after {attempt_num-1} attempts")
                    stats['llm_retry_success'] += 1
                    break
            
            # Final result
            if not (validation_result and validation_result.get('overall_passed', False)):
                if attempt_num > args.max_retries:
                    print(f"[{i:3d}] ✗ FAILED after auto-fix + {args.max_retries} LLM retries")
                    stats['final_failures'] += 1
                else:
                    print(f"[{i:3d}] ✗ FAILED (no further attempts)")
                    stats['final_failures'] += 1

            # Write final SQL
            outf.write(current_sql + "\n")
    
    # Print final statistics
    print(f"\n{'='*60}")
    print("FINAL STATISTICS")
    print(f"{'='*60}")
    print(f"Total questions: {stats['total']}")
    print(f"First attempt success: {stats['first_attempt_success']} ({stats['first_attempt_success']/stats['total']*100:.1f}%)")
    print(f"Auto-fix success: {stats['autofix_success']} ({stats['autofix_success']/stats['total']*100:.1f}%)")
    print(f"LLM retry success: {stats['llm_retry_success']} ({stats['llm_retry_success']/stats['total']*100:.1f}%)")
    print(f"Final failures: {stats['final_failures']} ({stats['final_failures']/stats['total']*100:.1f}%)")
    print(f"Auto-fix attempts: {stats['autofix_attempts']}")
    print(f"LLM retry attempts: {stats['llm_retry_attempts']}")
    
    total_success = stats['first_attempt_success'] + stats['autofix_success'] + stats['llm_retry_success']
    print(f"Overall success rate: {total_success}/{stats['total']} ({total_success/stats['total']*100:.1f}%)")
    
    print(f"\nCompleted! Results written to: {out_file}")
    print(f"Prompts logged to: {prompt_log}")


if __name__ == '__main__':
    main()