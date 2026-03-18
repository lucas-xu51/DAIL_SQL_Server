#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path

def analyze_failures():
    """分析验证报告中的失败情况"""
    
    report_file = Path(__file__).parent / 'validation_report_v2.json'
    
    with open(report_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    results = data['predicted']['results']
    
    # 分类失败的 SQL
    failures = {
        'column_not_found': [],  # 列不存在
        'fk_missing': [],         # 缺失外键
        'execution_error': [],    # 执行错误
        'other': []               # 其他错误
    }
    
    for result in results:
        if not result['overall_passed']:
            sql = result['sql']
            errors = []
            
            # 收集所有错误信息
            for stage_name in ['stage1_syntax', 'stage2_logic', 'stage3_execution']:
                stage = result.get(stage_name, {})
                if not stage.get('passed'):
                    errors.extend(stage.get('errors', []))
            
            # 分类
            error_str = ' | '.join(errors).lower()
            
            if '列不存在' in error_str or 'no such column' in error_str:
                failures['column_not_found'].append({
                    'sql': sql,
                    'errors': errors
                })
            elif 'join 缺失外键' in error_str or 'fk' in error_str:
                failures['fk_missing'].append({
                    'sql': sql,
                    'errors': errors
                })
            elif 'execution' in error_str:
                failures['execution_error'].append({
                    'sql': sql,
                    'errors': errors
                })
            else:
                failures['other'].append({
                    'sql': sql,
                    'errors': errors
                })
    
    # 输出分析报告
    output_file = Path(__file__).parent / 'failure_analysis.txt'
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("SQL 验证失败分析报告\n")
        f.write("="*80 + "\n\n")
        
        f.write("总体统计\n")
        f.write("-"*80 + "\n")
        total_failed = sum(len(v) for v in failures.values())
        f.write(f"总失败: {total_failed}/92\n\n")
        
        # 按类型分析
        for failure_type, cases in failures.items():
            if not cases:
                continue
            
            type_name = {
                'column_not_found': '❌ 列不存在错误',
                'fk_missing': '❌ 缺失外键关系',
                'execution_error': '⚠️  执行错误',
                'other': '❓ 其他错误'
            }[failure_type]
            
            f.write(f"\n{type_name}\n")
            f.write("-"*80 + "\n")
            f.write(f"数量: {len(cases)}\n\n")
            
            for i, case in enumerate(cases, 1):
                f.write(f"[{i}] SQL:\n")
                sql = case['sql']
                # 换行显示长 SQL
                if len(sql) > 100:
                    f.write(f"    {sql[:100]}...\n")
                else:
                    f.write(f"    {sql}\n")
                
                f.write(f"    错误:\n")
                for err in case['errors']:
                    f.write(f"      • {err}\n")
                f.write("\n")
        
        f.write("\n" + "="*80 + "\n")
        f.write("建议\n")
        f.write("="*80 + "\n")
        f.write("""
1. 列不存在错误: 
   - 检查 SQL 中使用的列名是否与表中定义的列名一致
   - 某些列名可能被 LLM 错误拼写（如 MakeId vs MakerId）

2. 缺失外键关系:
   - JOIN 条件中使用了不存在外键关系的列
   - 这些通常是真正的逻辑错误，SQL 无法正确执行

3. 执行错误:
   - 语法正确但执行时出错，如 ambiguous column name
   - 这类错误需要在 SELECT 列前加上表别名来消除歧义

4. 其他错误:
   - 需要逐一检查和分类
""")
    
    print(f"✓ 失败分析报告已生成: {output_file}")
    
    # 也输出到控制台
    print("\n" + "="*80)
    print("失败分类总结")
    print("="*80)
    for failure_type, cases in failures.items():
        if cases:
            type_name = {
                'column_not_found': '列不存在',
                'fk_missing': '缺失外键',
                'execution_error': '执行错误',
                'other': '其他'
            }[failure_type]
            print(f"{type_name}: {len(cases)} 个")


if __name__ == '__main__':
    analyze_failures()
