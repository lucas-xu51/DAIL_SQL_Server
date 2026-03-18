#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
from pathlib import Path
from typing import Dict, List

from sql_validator_v2 import ImprovedSQLValidator


def load_sqls_from_file(file_path: str) -> List[str]:
    sqls = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                sqls.append(line)
    return sqls


def load_dev_json(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def find_db_files(spider_root: Path, db_id: str):
    """Search spider_root/db_id for .sqlite and .sql files. Returns (sqlite_path, sql_path) or (None,None)."""
    # Try direct child first
    folder = spider_root / db_id
    if not (folder.exists() and folder.is_dir()):
        # Try case-insensitive sibling lookup
        for child in spider_root.iterdir():
            if child.is_dir() and child.name.lower() == db_id.lower():
                folder = child
                break

    # If still not found, perform a recursive search for a directory named db_id (case-insensitive)
    if not (folder.exists() and folder.is_dir()):
        for candidate in spider_root.rglob('*'):
            if candidate.is_dir() and candidate.name.lower() == db_id.lower():
                folder = candidate
                break

    if not (folder.exists() and folder.is_dir()):
        return (None, None)

    sqlite_path = None
    sql_path = None
    for p in folder.iterdir():
        if p.suffix.lower() in ('.sqlite', '.db'):
            sqlite_path = str(p)
        if p.suffix.lower() == '.sql':
            sql_path = str(p)

    return (sqlite_path, sql_path)


def main():
    parser = argparse.ArgumentParser(description='Validate predicted SQLs across all Spider DBs (per-dev item).')
    parser.add_argument('--spider_root', required=False,
                        default=r'C:\Users\grizz\OneDrive\Desktop\COSC448\ideas\model\DAIL-SQL\dataset\spider\database',
                        help='Root folder containing spider DB subfolders')
    parser.add_argument('--dev_json', required=False,
                        default=r'C:\Users\grizz\OneDrive\Desktop\COSC448\ideas\model\DAIL-SQL\dataset\spider\dev.json')
    parser.add_argument('--predicted_sql_file', required=False,
                        default=r'C:\Users\grizz\OneDrive\Desktop\COSC448\ideas\model\DAIL-SQL\results\DAIL-SQL+GPT-4.txt')
    parser.add_argument('--error_dev_output', required=False,
                        default=r'C:\Users\grizz\OneDrive\Desktop\COSC448\ideas\model\DAIL-SQL\dataset\spider\error_dev_all.json')
    args = parser.parse_args()

    spider_root = Path(args.spider_root)
    dev_json_path = args.dev_json
    predicted_sql_file = args.predicted_sql_file
    error_dev_output = args.error_dev_output

    predicted_sqls = load_sqls_from_file(predicted_sql_file)
    dev_data = load_dev_json(dev_json_path)

    if len(predicted_sqls) != len(dev_data):
        print(f"Warning: predicted SQL count ({len(predicted_sqls)}) != dev.json count ({len(dev_data)}). Using min length.")
        n = min(len(predicted_sqls), len(dev_data))
        predicted_sqls = predicted_sqls[:n]
        dev_data = dev_data[:n]

    print(f"Validating {len(predicted_sqls)} items across Spider DBs under: {spider_root}")

    # cache validators per db_id
    validators: Dict[str, ImprovedSQLValidator] = {}
    missing_db_ids = set()

    error_dev_data = []
    error_details = []  # 保存每个错误的详细信息
    passed = 0
    failed = 0
    
    # 统计错误类型
    error_type_stats = {}  # error_type -> count
    execution_status_stats = {}  # (stage2_result, exec_status) -> count

    for i, (sql, dev_item) in enumerate(zip(predicted_sqls, dev_data), 1):
        db_id = dev_item.get('db_id')
        if not db_id:
            print(f"[{i}] Missing db_id in dev item, skipping")
            failed += 1
            error_dev_data.append(dev_item)
            error_details.append({
                'index': i,
                'db_id': 'unknown',
                'question': dev_item.get('question', ''),
                'sql': sql,
                'error_type': 'MISSING_DB_ID',
                'errors': ['Missing db_id in dev item']
            })
            continue

        if db_id in missing_db_ids:
            # we previously couldn't find this DB
            print(f"[{i}] SKIP {db_id} (db files missing)")
            failed += 1
            error_dev_data.append(dev_item)
            error_details.append({
                'index': i,
                'db_id': db_id,
                'question': dev_item.get('question', ''),
                'sql': sql,
                'error_type': 'DB_FILES_MISSING',
                'errors': [f'Database files not found for db_id: {db_id}']
            })
            continue

        if db_id not in validators:
            sqlite_path, sql_path = find_db_files(spider_root, db_id)
            if not sqlite_path:
                print(f"⚠️  Could not find sqlite file for db_id '{db_id}' under {spider_root}/{db_id}")
                missing_db_ids.add(db_id)
                failed += 1
                error_dev_data.append(dev_item)
                error_details.append({
                    'index': i,
                    'db_id': db_id,
                    'question': dev_item.get('question', ''),
                    'sql': sql,
                    'error_type': 'DB_FILES_NOT_FOUND',
                    'errors': [f'Could not find sqlite file for db_id {db_id}']
                })
                continue
            try:
                validators[db_id] = ImprovedSQLValidator(sqlite_path, sql_path)
            except Exception as e:
                print(f"✗ Failed to init validator for {db_id}: {e}")
                missing_db_ids.add(db_id)
                failed += 1
                error_dev_data.append(dev_item)
                error_details.append({
                    'index': i,
                    'db_id': db_id,
                    'question': dev_item.get('question', ''),
                    'sql': sql,
                    'error_type': 'VALIDATOR_INIT_FAILED',
                    'errors': [str(e)]
                })
                continue

        validator = validators[db_id]
        result = validator.validate_comprehensive(sql)

        if result.get('overall_passed'):
            passed += 1
        else:
            failed += 1
            error_dev_data.append(dev_item)
            
            # 收集错误信息
            all_errors = []
            rule_based_errors = []
            execution_info = ''
            
            # 收集 rule-based 错误 (syntax + logic)
            if not result['stage1_syntax']['passed']:
                rule_based_errors.extend(result['stage1_syntax']['errors'])
                all_errors.extend(result['stage1_syntax']['errors'])
            if not result['stage2_logic']['passed']:
                rule_based_errors.extend(result['stage2_logic']['errors'])
                all_errors.extend(result['stage2_logic']['errors'])
            
            # 收集执行结果信息
            if not result['stage3_execution']['passed']:
                exec_status = result['stage3_execution'].get('status', 'failed')
                all_errors.extend(result['stage3_execution']['errors'])
                
                # 生成执行结果描述
                if exec_status == 'empty':
                    execution_info = 'Execution returned empty result'
                elif exec_status == 'failed':
                    execution_info = 'Execution failed'
                else:
                    execution_info = 'Execution error'
            else:
                execution_info = 'Execution successful'
            
            # 统计错误类型
            error_type = result['error_summary']
            error_type_stats[error_type] = error_type_stats.get(error_type, 0) + 1
            
            # 统计 rule-based + execution 的组合
            rule_based_status = 'pass' if result['stage2_logic']['passed'] else 'fail'
            exec_status = result['stage3_execution'].get('status', 'N/A')
            combination_key = f"{rule_based_status}_rule_based + {exec_status}_exec"
            execution_status_stats[combination_key] = execution_status_stats.get(combination_key, 0) + 1
            
            error_details.append({
                'index': i,
                'db_id': db_id,
                'question': dev_item.get('question', ''),
                'sql': sql,
                'error_type': result['error_summary'],
                'rule_based_errors': rule_based_errors,
                'execution_status': result['stage3_execution'].get('status', 'N/A'),
                'execution_info': execution_info,
                'all_errors': all_errors
            })
            
            print(f"[{i}] ✗ FAIL (db_id: {db_id})")
            print(f"  Question: {dev_item.get('question', '')}")
            print(f"  Pred SQL: {sql[:120]}...")
            
            # 打印 rule-based 错误
            if rule_based_errors:
                print(f"  [Rule-based errors]:")
                for err in rule_based_errors:
                    print(f"    • {err}")
            
            # 打印执行结果
            print(f"  [Execution result]: {execution_info}")
            if result['stage3_execution']['errors']:
                for err in result['stage3_execution']['errors']:
                    print(f"    • {err}")
            print()

    print("\n" + "="*80)
    print("验证 SUMMARY")
    print("="*80)
    print()
    print(f"总验证数: {len(predicted_sqls)}")
    print(f"✓ 通过: {passed} ({passed*100//len(predicted_sqls)}%)")
    print(f"✗ 失败: {failed} ({failed*100//len(predicted_sqls)}%)")
    print(f"⚠️  缺失数据库: {len(missing_db_ids)}")
    print()

    # 统计按 db_id 的错误分布
    error_by_db = {}
    for item in error_dev_data:
        db_id = item.get('db_id', 'unknown')
        error_by_db[db_id] = error_by_db.get(db_id, 0) + 1

    if error_by_db:
        print("错误分布（按数据库）:")
        sorted_errors = sorted(error_by_db.items(), key=lambda x: x[1], reverse=True)
        for db_id, count in sorted_errors:
            pct = count * 100 // failed if failed > 0 else 0
            print(f"  {db_id:25s}: {count:3d} ({pct:2d}%)")
    
    print()
    
    # 统计错误类型
    if error_type_stats:
        print("错误类型分布:")
        sorted_error_types = sorted(error_type_stats.items(), key=lambda x: x[1], reverse=True)
        for error_type, count in sorted_error_types:
            pct = count * 100 // failed if failed > 0 else 0
            print(f"  {error_type:30s}: {count:3d} ({pct:2d}%)")
    
    print()
    
    # 统计 rule-based + execution 组合
    if execution_status_stats:
        print("Rule-based + Execution 组合:")
        sorted_combinations = sorted(execution_status_stats.items(), key=lambda x: x[1], reverse=True)
        for combination, count in sorted_combinations:
            pct = count * 100 // failed if failed > 0 else 0
            print(f"  {combination:50s}: {count:3d} ({pct:2d}%)")
    
    print()
    print(f"✓ 错误题目已保存到: {error_dev_output}")

    # 生成详细错误报告 txt
    error_report_path = error_dev_output.replace('.json', '_detailed_report.txt')
    with open(error_report_path, 'w', encoding='utf-8') as f:
        f.write("="*100 + "\n")
        f.write("SQL 验证详细错误报告\n")
        f.write("="*100 + "\n\n")
        
        f.write(f"总验证数: {len(predicted_sqls)}\n")
        f.write(f"✓ 通过: {passed} ({passed*100//len(predicted_sqls)}%)\n")
        f.write(f"✗ 失败: {failed} ({failed*100//len(predicted_sqls)}%)\n\n")
        
        # 错误类型分布
        if error_type_stats:
            f.write("错误类型分布:\n")
            sorted_error_types = sorted(error_type_stats.items(), key=lambda x: x[1], reverse=True)
            for error_type, count in sorted_error_types:
                pct = count * 100 // failed if failed > 0 else 0
                f.write(f"  {error_type:30s}: {count:3d} ({pct:2d}%)\n")
            f.write("\n")
        
        # Rule-based + Execution 组合统计
        if execution_status_stats:
            f.write("Rule-based + Execution 组合:\n")
            sorted_combinations = sorted(execution_status_stats.items(), key=lambda x: x[1], reverse=True)
            for combination, count in sorted_combinations:
                pct = count * 100 // failed if failed > 0 else 0
                f.write(f"  {combination:50s}: {count:3d} ({pct:2d}%)\n")
            f.write("\n")
        
        f.write("="*100 + "\n")
        f.write("错误详情\n")
        f.write("="*100 + "\n\n")
        
        for idx, err in enumerate(error_details, 1):
            f.write(f"[{idx}] 问题索引: {err['index']}\n")
            f.write(f"    数据库: {err['db_id']}\n")
            f.write(f"    问题: {err['question']}\n")
            f.write(f"    预测SQL: {err['sql']}\n\n")
            
            # Rule-based 错误
            if err['rule_based_errors']:
                f.write(f"    [Rule-based 错误]:\n")
                for err_msg in err['rule_based_errors']:
                    f.write(f"      • {err_msg}\n")
            else:
                f.write(f"    [Rule-based]: 通过\n")
            
            f.write(f"\n    [执行结果]: {err['execution_info']}\n")
            
            # 执行阶段的其他错误信息
            if err['execution_status'] != 'success':
                # 收集执行阶段的错误（不包括 rule-based 的）
                exec_errors = []
                for err_msg in err['all_errors']:
                    if err_msg.startswith('EXECUTION:'):
                        exec_errors.append(err_msg)
                if exec_errors:
                    for err_msg in exec_errors:
                        f.write(f"      • {err_msg}\n")
            
            f.write("\n" + "-"*100 + "\n\n")
    
    print(f"✓ 详细报告已保存到: {error_report_path}")

    with open(error_dev_output, 'w', encoding='utf-8') as f:
        json.dump(error_dev_data, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    main()
