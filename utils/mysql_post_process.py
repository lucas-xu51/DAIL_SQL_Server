#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL Post Process Adapter for DAIL-SQL
适配DAIL-SQL项目使用MySQL数据库的后处理模块
"""

import mysql.connector
from mysql.connector import Error
import asyncio
import json
import os
from typing import Tuple, Any


class MySQLSpiderConnector:
    """MySQL Spider数据库连接器"""
    
    def __init__(self, config_file="mysql_config.json"):
        with open(config_file, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        self.mysql_config = self.config['mysql']
    
    def get_connection(self, db_name):
        """获取数据库连接"""
        try:
            config = self.mysql_config.copy()
            config['database'] = db_name
            
            connection = mysql.connector.connect(**config)
            return connection
        except Error as e:
            print(f"连接MySQL数据库 {db_name} 时出错: {e}")
            return None
    
    async def exec_on_mysql_db(self, db_name: str, query: str) -> Tuple[str, Any]:
        """在MySQL数据库上执行查询"""
        try:
            connection = self.get_connection(db_name)
            if not connection:
                return ("exception", Error("无法连接到数据库"))
            
            cursor = connection.cursor()
            cursor.execute(query)
            
            # 获取结果
            if cursor.description:
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                result = [tuple(row) for row in result]
            else:
                result = []
            
            cursor.close()
            connection.close()
            
            return ("success", result)
            
        except Error as e:
            return ("exception", e)
        except Exception as e:
            return ("exception", e)


def get_mysql_exec_output(db_name: str, sql: str, mysql_connector=None):
    """
    MySQL版本的SQL执行输出函数
    替代原始的get_exec_output函数
    """
    if mysql_connector is None:
        mysql_connector = MySQLSpiderConnector()
    
    # 后处理SQL查询
    from utils.post_process import postprocess, remove_distinct
    sql = postprocess(sql)
    
    try:
        sql = remove_distinct(sql)
    except Exception as e:
        return "exception", []
    
    # 执行查询
    flag, result = asyncio.run(mysql_connector.exec_on_mysql_db(db_name, sql))
    return flag, result


# 导出主要函数
__all__ = ['MySQLSpiderConnector', 'get_mysql_exec_output']
