import os

db_root_path = '/TA-SQL/data/dev_databases'
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

db_root_path = '/TA-SQL/data/dev_databases'

import os
import sqlite3
import multiprocessing
import time
from multiprocessing import Process, Queue, Manager

db_root_path = '/TA-SQL/data/dev_databases'

def execute_sql_in_process(sql, db_id, result_queue, timeout_flag):
    """
    在独立进程中执行SQL查询
    """
    try:
        path = os.path.join(db_root_path, db_id, db_id)
        conn = sqlite3.connect(f'{path}.sqlite')
        cursor = conn.cursor()
        
        # 设置SQLite级别的超时（busytimeout）
        cursor.execute("PRAGMA busy_timeout = 1000")  # 1秒
        
        cursor.execute(sql)
        result = cursor.fetchall()
        
        # 检查是否被超时标记
        if timeout_flag.value:
            return
            
        result_queue.put(('success', result))
        
    except Exception as e:
        if not timeout_flag.value:
            result_queue.put(('error', str(e)))
    finally:
        try:
            conn.close()
        except:
            pass

def connect_sql_with_timeout(sql, db_id, timeout=1):
    """
    使用进程隔离实现强制超时终止
    """
    # 创建进程间通信对象
    result_queue = Queue()
    manager = Manager()
    timeout_flag = manager.Value('b', False)
    
    # 创建执行进程
    process = Process(
        target=execute_sql_in_process, 
        args=(sql, db_id, result_queue, timeout_flag)
    )
    
    try:
        print(f"[DEBUG] Executing SQL with {timeout}s timeout...")
        
        process.start()
        process.join(timeout=timeout)  # 等待指定时间
        
        if process.is_alive():
            print(f"[DEBUG] Query timeout after {timeout}s - forcefully terminating process!")
            
            # 设置超时标记
            timeout_flag.value = True
            
            # 强制终止进程
            process.terminate()
            process.join(timeout=2)  # 给进程2秒时间优雅退出
            
            if process.is_alive():
                # 如果还活着，使用kill信号
                process.kill()
                process.join()
            
            return f'Error executing SQL: Query timeout after {timeout}s (likely cartesian product)'
        
        # 检查执行结果
        if not result_queue.empty():
            result_type, result_data = result_queue.get_nowait()
            
            if result_type == 'error':
                error = f"Error executing SQL: {result_data}"
                print(f"[DEBUG] SQL Error: {sql}, Error: {error}")
                return error
            
            # 检查结果有效性
            if result_data is None or result_data == [] or len(result_data) == 0:
                return 'Error executing SQL: No result returned.'
            elif all(row[0] is None for row in result_data):
                return 'Error executing SQL: No result returned.'
            
            print("SQL executed successfully.")
            return None
        else:
            return 'Error executing SQL: Process terminated without result.'
            
    except Exception as e:
        error = f"Error in process execution: {str(e)}"
        print(f"[DEBUG] {error}")
        return error
    finally:
        # 确保进程被清理
        if process.is_alive():
            process.terminate()
            process.join(timeout=1)
            if process.is_alive():
                process.kill()