import prestodb
import os
import time
import csv
import re

def run_query(cursor: prestodb.dbapi.Cursor, database_name: str, sql_dir: str, res_dir: str) -> str:
    # Query14需要更多的阶段数
    cursor.execute("SET SESSION query_max_stage_count = 300")
    csv_headers = ['query', 'state', 'elapsedTimeMillis', 'wallTimeMillis', 'cpuTimeMillis', 'waitingForPrerequisitesTimeMillis', 'queuedTimeMillis', 'dateTime']
    csv_content = []
    for filename in os.listdir(sql_dir):
        if filename.endswith(".sql"):
            file_path = os.path.join(sql_dir, filename)
            print(f"Starting execution of {file_path}")
            # 打开文件并读取内容
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_content = ""
                for line in file:
                    if line.strip().startswith('--'):
                        continue  # 跳过注释行
                    if line.strip().endswith(';'):
                        sql_content += line.strip()[:-1]  # 去掉分号并添加到SQL内容中
                        break
                    sql_content += line  # 将非注释行添加到SQL内容中

                # 执行SQL内容
                try:
                    cursor.execute(sql_content)
                    res = cursor.stats
                    csv_line = []
                    for header in csv_headers:
                        if header == 'query':
                            csv_line.append(filename)
                        elif header == 'dateTime':
                            csv_line.append(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
                        else:
                            csv_line.append(res[header])
                    csv_content.append(csv_line)
                except Exception as e:
                    print(f"Error executing {file_path}: {e}")
                    csv_line = []
                    for header in csv_headers:
                        if header == 'query':
                            csv_line.append(filename)
                        elif header == 'state':
                            csv_line.append('FAILED')
                        elif header == 'dateTime':
                            csv_line.append(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
                        else:
                            csv_line.append(0)
                    csv_content.append(csv_line)

    # print(csv_content)
    csv_path = os.path.join(res_dir, f"presto-{time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime())}-{database_name}.csv")
    with open(csv_path, "x", encoding="utf-8", newline="") as file:
        # 基于文件对象构建 csv写入对象
        csv_writer = csv.writer(file)
        csv_writer.writerow(csv_headers)
        csv_content.sort(key=lambda x: int(re.search(r'\d+', x[0]).group()))
        csv_writer.writerows(csv_content)
    
    print(f"Read finished: database_name = {database_name}, sql_dir = {sql_dir}, csv_path = {csv_path}")

    return csv_path

def run_all_queries(
        cursor: prestodb.dbapi.Cursor,
        target_database_prefix: str,
        sql_dir: str,
        res_dir: str,
        partition_nums: list):
    for partition_num in partition_nums:
        database_name = f"{target_database_prefix}_{partition_num}"
        cursor.execute(f"USE {database_name}")
        run_query(cursor=cursor, database_name=database_name, sql_dir=sql_dir, res_dir=res_dir)
