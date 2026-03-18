#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import re
import json
import argparse
from collections import defaultdict
from typing import Set, List, Tuple, Dict
from pathlib import Path

# 使用改进的验证器
from sql_validator_v2 import ImprovedSQLValidator


def load_sqls_from_file(file_path: str) -> List[str]:
    """从文件加载 SQL"""
    sqls = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    sqls.append(line)
    except Exception as e:
        print(f"✗ 加载文件失败 {file_path}: {e}")
    
    return sqls


def load_dev_json(file_path: str) -> List[Dict]:
    """加载 dev.json，保持原始格式"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            dev_data = json.load(f)
        print(f"✓ 加载 dev.json 成功，共 {len(dev_data)} 条题目")
        return dev_data
    except Exception as e:
        print(f"✗ 加载 dev.json 失败 {file_path}: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description='Validate predicted SQLs against a Spider-like SQLite DB (v2).')
    parser.add_argument('--db_path', required=False, help='Path to sqlite3 database file',
                        default=r'C:\Users\grizz\Downloads\postsqlfix-1BD2\database\spider\car_1\car_1.sqlite')
    parser.add_argument('--schema_sql', required=False, help='Path to SQL schema file to supplement FK info',
                        default=r'C:\Users\grizz\Downloads\postsqlfix-1BD2\database\spider\car_1\car_1.sql')
    parser.add_argument('--predicted_sql_file', required=False, help='File with one predicted SQL per line',
                        default=r'C:\Users\grizz\OneDrive\Desktop\COSC448\ideas\model\DAIL-SQL\dataset\process\SPIDER-TEST_SQL_3-SHOT_EUCDISQUESTIONMASK_QA-EXAMPLE_CTX-200_ANS-4096\car1_gold.txt')
    parser.add_argument('--dev_json', required=False, help='dev.json path',
                        default=r'C:\Users\grizz\OneDrive\Desktop\COSC448\ideas\model\DAIL-SQL\dataset\spider\dev.json')
    parser.add_argument('--error_dev_output', required=False, help='Output path for filtered error dev.json',
                        default=r'C:\Users\grizz\OneDrive\Desktop\COSC448\ideas\model\DAIL-SQL\dataset\spider\error_dev.json')
    args = parser.parse_args()

    db_path = args.db_path
    schema_sql_path = args.schema_sql
    predicted_sql_file = args.predicted_sql_file
    dev_json_path = args.dev_json
    error_dev_output = args.error_dev_output

    # 初始化验证器
    validator = ImprovedSQLValidator(db_path, schema_sql_path)
    print("\n" + "="*80)
    print("开始验证预测 SQL 并生成 error_dev.json")
    print("="*80 + "\n")
    
    # 加载数据（确保题目顺序与 SQL 顺序一致）
    predicted_sqls = load_sqls_from_file(predicted_sql_file)
    dev_data = load_dev_json(dev_json_path)
    
    # 校验数据长度一致性
    if len(predicted_sqls) != len(dev_data):
        print(f"⚠️  警告：预测 SQL 数量（{len(predicted_sqls)}）与 dev.json 题目数量（{len(dev_data)}）不一致！")
        print("将仅处理前 N 条（N 为较小值）")
        min_len = min(len(predicted_sqls), len(dev_data))
        predicted_sqls = predicted_sqls[:min_len]
        dev_data = dev_data[:min_len]
    
    print(f"✓ 开始验证 {len(predicted_sqls)} 条 SQL + 对应题目\n")
    
    # 验证并筛选错误条目
    error_dev_data = []  # 存储有问题的题目（与 dev.json 格式一致）
    predicted_passed = 0
    predicted_failed = 0
    
    for i, (sql, dev_item) in enumerate(zip(predicted_sqls, dev_data), 1):
        result = validator.validate_comprehensive(sql)
        
        if result['overall_passed']:
            predicted_passed += 1
            status = "✓ PASS"
        else:
            predicted_failed += 1
            status = "✗ FAIL"
            # 验证失败：将原始 dev_item 加入错误列表
            error_dev_data.append(dev_item)
        
        # 只显示失败的详细信息
        if not result['overall_passed']:
            print(f"[{i:2d}] {status} (db_id: {dev_item['db_id']})")
            print(f"  问题: {dev_item['question']}")
            print(f"  预测 SQL: {sql[:100]}...")
            
            if not result['stage1_syntax']['passed']:
                for err in result['stage1_syntax']['errors']:
                    print(f"    • SYNTAX: {err}")
            
            if not result['stage2_logic']['passed']:
                for err in result['stage2_logic']['errors']:
                    print(f"    • LOGIC: {err}")
            
            if not result['stage3_execution']['passed']:
                for err in result['stage3_execution']['errors']:
                    print(f"    • EXECUTION: {err}")
            print()
    
    # 输出验证统计
    print(f"\n" + "="*80)
    print(f"验证统计:")
    print(f"  ✓ 通过: {predicted_passed}/{len(predicted_sqls)} ({predicted_passed*100//len(predicted_sqls) if len(predicted_sqls) > 0 else 0}%)")
    print(f"  ✗ 失败: {predicted_failed}/{len(predicted_sqls)} ({predicted_failed*100//len(predicted_sqls) if len(predicted_sqls) > 0 else 0}%)")
    print(f"  📝 错误题目数量: {len(error_dev_data)}")
    
    # 保存 error_dev.json（与 dev.json 格式完全一致）
    with open(error_dev_output, 'w', encoding='utf-8') as f:
        json.dump(error_dev_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ 错误题目已保存到: {error_dev_output}")
    print(f"  格式与 dev.json 一致，仅包含验证失败的题目")


if __name__ == '__main__':
    main()