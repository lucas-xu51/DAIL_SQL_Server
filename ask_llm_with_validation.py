import argparse
import os
import json
import time
import sys
from utils.enums import LLM
from test_validation.sql_validator_v2 import ImprovedSQLValidator
from utils.post_process import process_duplication, get_sqls

"""
Wrapper script that: 
- for each question, calls ask_llm to get SQLs
- logs the prompt sent to the LLM
- validates the chosen SQL using ImprovedSQLValidator
- if validation fails, constructs an augmented prompt describing the validation errors in natural language
- re-asks LLM up to max_retries times with the augmented prompt
- writes final SQLs to an output file just like ask_llm.py

Key behaviors:
- This script is intentionally non-invasive: it doesn't modify existing files.
- Validation occurs before deciding whether to retry.
- Configurable retry behavior via --max_retries parameter:
  * 0: No validation, use original SQL regardless of errors
  * 1: Validate once, retry once if failed (max 2 LLM calls per question)
  * 2: Validate and retry up to 2 times if failed (max 3 LLM calls per question)
  * N: Validate and retry up to N times if failed (max N+1 LLM calls per question)
- Prompts are logged with timestamps for debugging.
- The final prompt always ends with /* Answer the following: <nl_question> */
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


def log_prompt(log_file: str, prompt: str, is_retry: bool = False, qindex: int = None):
    """Log the prompt to a file with a timestamp and optional question index."""
    with open(log_file, 'a', encoding='utf-8') as f:
        prefix = "[RETRY] " if is_retry else "[INITIAL] "
        f.write(f"\n{'='*80}\n")
        f.write(f"TIMESTAMP: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        if qindex is not None:
            f.write(f"QUESTION_INDEX: {qindex}\n")
        f.write(f"TYPE: {prefix}\n")
        f.write(f"{'='*80}\n")
        f.write(prompt + "\n")


def log_answer(log_file: str, qindex: int, attempt: int, sqls):
    """Log the LLM answers (one or more candidates) for a question attempt.

    sqls may be a single string or a list of strings.
    """
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n--- QUESTION: {qindex} | ATTEMPT: {attempt} ---\n")
        if isinstance(sqls, list):
            for k, s in enumerate(sqls, 1):
                f.write(f"Candidate {k}: {s}\n")
        else:
            f.write(str(sqls) + "\n")


def extract_nl_question(prompt: str) -> str:
    """Extract the NL question from the prompt (text after last /* Answer the following: ... */)."""
    import re
    # Find all matches and return the last one (more robust if there are examples)
    matches = re.findall(r'/\*\s*Answer the following:\s*([^*]*)\*/', prompt, re.IGNORECASE | re.DOTALL)
    if matches:
        return matches[-1].strip()
    return ""


def build_augmented_prompt(original_prompt: str, error_desc: str, nl_question: str) -> str:
    """Build an augmented prompt that inserts error_desc before the final '/* Answer the following: ... */' block.

    This ensures the final ordering is:
      <examples + schema>
      <error description>
      /* Answer the following: <nl_question> */
      SELECT
    """
    import re

    # Split original_prompt into part before the last Answer block and the final Answer block (if any)
    split_pattern = re.compile(r'(.*)(/\*\s*Answer the following:\s*[^*]*\*/\s*)(SELECT\s*)?$', re.IGNORECASE | re.DOTALL)
    m = split_pattern.match(original_prompt)
    if m:
        prefix = m.group(1).rstrip()
        # We'll rebuild the Answer block using nl_question to ensure it's correct
    else:
        # If pattern not found, treat whole prompt as prefix
        prefix = original_prompt.rstrip()

    # Build the new prompt
    parts = [prefix]
    if error_desc:
        parts.append(error_desc.strip())
    parts.append(f"/* Answer the following: {nl_question} */")
    # Add the SELECT placeholder at the end so model completes the SQL after the question
    parts.append("SELECT ")

    return "\n\n".join(parts)


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


def ensure_prompt_ends_with_nl_question(base_prompt: str, nl_question: str) -> str:
    """
    Ensure the prompt ends with /* Answer the following: <nl_question> */
    This is important for consistency.
    """
    # Remove any trailing whitespace
    base_prompt = base_prompt.rstrip()
    
    # Check if prompt already ends with the NL question marker
    if "/* Answer the following:" in base_prompt:
        # Already has it, extract and rebuild
        import re
        base_without_nl = re.sub(
            r'/\*\s*Answer the following:\s*[^*]*\*/',
            '',
            base_prompt,
            flags=re.IGNORECASE | re.DOTALL
        ).rstrip()
    else:
        base_without_nl = base_prompt
    
    # Rebuild with NL question at the end
    return f"{base_without_nl}\n\n/* Answer the following: {nl_question} */"


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
    parser.add_argument("--n", type=int, default=1, help="Number of candidate SQLs (for self-consistency)")
    parser.add_argument("--db_dir", type=str, default="dataset/spider/database", help="Path to database directory")
    parser.add_argument("--max_retries", type=int, default=1, help="Maximum number of retries when validation fails (0=no validation, 1=retry once, 2=retry twice, etc.)")
    args = parser.parse_args()

    questions_json = json.load(open(os.path.join(args.question, QUESTION_FILE), "r", encoding='utf-8'))
    questions = [_["prompt"] for _ in questions_json["questions"]]
    db_ids = [_["db_id"] for _ in questions_json["questions"]]

    # Import LLM runtime lazily so module can be imported for static checks
    try:
        from llm.chatgpt import init_chatgpt, ask_llm
    except Exception as e:
        print("ERROR: Failed to import LLM runtime (llm.chatgpt).\n"
              "This usually means your `openai` Python package version is incompatible or not installed.\n"
              "Suggested fixes:\n"
              "  1) Install/upgrade the official OpenAI package: `pip install --upgrade openai`\n"
              "  2) Ensure your local `llm/chatgpt.py` is compatible with the installed OpenAI client.\n"
              f"Import error details: {e}")
        sys.exit(1)

    # init openai api
    init_chatgpt(args.openai_api_key, args.openai_group_id, args.model)

    out_file = f"{args.question}/RESULTS_MODEL-{args.model}_with_validation_retries{args.max_retries}.txt"
    prompt_log = f"{args.question}/LLM_PROMPTS-{args.model}_retries{args.max_retries}.log"

    validators = {}
    
    # Clear the log file at start
    with open(prompt_log, 'w', encoding='utf-8') as f:
        f.write(f"LLM PROMPT LOG - Model: {args.model} - Max Retries: {args.max_retries}\n")
        f.write(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")

    with open(out_file, 'w', encoding='utf-8') as outf:
        for i, (original_prompt, db_id) in enumerate(zip(questions, db_ids)):
            # Extract NL question from the original prompt
            nl_question = extract_nl_question(original_prompt)
            
            # Prepare validator for this DB
            if db_id not in validators:
                try:
                    validators[db_id] = build_validator_for_db(args.db_dir, db_id)
                except Exception as e:
                    print(f"[{i:3d}] WARNING: Cannot build validator for db_id '{db_id}': {e}")
                    validators[db_id] = None

            validator = validators[db_id]
            
            # ===== FIRST LLM CALL =====
            # Log the initial prompt
            log_prompt(prompt_log, original_prompt, is_retry=False, qindex=i)
            
            try:
                res = ask_llm(args.model, [original_prompt], args.temperature, args.n)
            except Exception as e:
                print(f"[{i:3d}] ERROR: LLM call failed: {e}")
                outf.write("SELECT\n")
                continue

            # Parse response and normalize
            raw_sqls = res['response'][0] if args.n > 1 else res['response']
            if isinstance(raw_sqls, list):
                candidates = raw_sqls
            else:
                candidates = [raw_sqls]

            # Log the LLM's first attempt answers
            log_answer(prompt_log, i, 1, candidates)

            final_sql = normalize_sql(candidates[0]) if candidates else "SELECT"
            
            # ===== VALIDATION AND RETRY LOOP =====
            retry_count = 0
            current_sql = final_sql
            current_prompt = original_prompt
            
            # Skip validation entirely if max_retries is 0
            if args.max_retries == 0:
                print(f"[{i:3d}] Validation SKIPPED (max_retries=0) for db_id '{db_id}'")
            elif validator is not None:
                while retry_count < args.max_retries:
                    result = validator.validate_comprehensive(current_sql)
                    
                    if result.get('overall_passed', False):
                        print(f"[{i:3d}] Validation PASSED for db_id '{db_id}' (attempt {retry_count + 1})")
                        break
                    else:
                        print(f"[{i:3d}] Validation FAILED for db_id '{db_id}' (attempt {retry_count + 1}/{args.max_retries + 1}) -> {'Retrying' if retry_count < args.max_retries - 1 else 'Max retries reached'}")
                        
                        # If we've reached max retries, stop trying
                        if retry_count >= args.max_retries - 1:
                            break
                        
                        # Prepare for retry
                        error_desc = validator.get_natural_error_description(result)
                        
                        # Build augmented prompt: insert error description before the final Answer block
                        augmented_prompt = build_augmented_prompt(original_prompt, error_desc, nl_question)
                        
                        # Log the retry prompt
                        log_prompt(prompt_log, augmented_prompt, is_retry=True, qindex=i)
                        
                        # ===== RETRY LLM CALL =====
                        try:
                            retry_res = ask_llm(args.model, [augmented_prompt], args.temperature, args.n)
                        except Exception as e:
                            print(f"[{i:3d}] ERROR: LLM retry call {retry_count + 1} failed: {e}")
                            break

                        if retry_res:
                            retry_raw_sqls = retry_res['response'][0] if args.n > 1 else retry_res['response']
                            if isinstance(retry_raw_sqls, list):
                                retry_candidates = retry_raw_sqls
                            else:
                                retry_candidates = [retry_raw_sqls]
                            
                            # Log the LLM's retry answers
                            log_answer(prompt_log, i, retry_count + 2, retry_candidates)

                            current_sql = normalize_sql(retry_candidates[0]) if retry_candidates else current_sql
                        else:
                            break
                        
                        retry_count += 1
                        
                # Update final_sql with the last attempt
                final_sql = current_sql
            else:
                print(f"[{i:3d}] No validator available for db_id '{db_id}' -> Using first response as-is")

            # Write final SQL
            outf.write(final_sql + "\n")
            
            if retry_count > 0:
                print(f"        -> Wrote SQL after {retry_count} retry(ies)")
            else:
                print(f"        -> Wrote original SQL")


if __name__ == '__main__':
    main()

