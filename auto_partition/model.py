import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import joblib
import optuna

def compute_relative_time(df):
    def process_group(group):
        # 去除为 0 的 time，计算 min 和 max
        non_zero_times = group['time'][group['time'] > 0]
        if non_zero_times.empty:
            # 如果全是 0，则相对时间全设为 0
            group['relative_time'] = 0
            return group
        min_time = non_zero_times.min()
        max_time = non_zero_times.max()

        # 替换 time == 0 为 max_time
        group['adjusted_time'] = group['time'].replace(0, max_time)

        # 计算相对时间
        denom = max_time - min_time
        if denom == 0:
            group['relative_time'] = 0  # 或者设为 NaN
        else:
            group['relative_time'] = (group['adjusted_time'] - min_time) / denom
        return group

    return df.groupby('query_id', group_keys=False).apply(process_group)

# 为指定的table将所有csv文件合并为一个大的DataFrame对象
def generate_csv(presto_res_dir: str, csv_dir: str, table_name: str, table_size: dict):
    # 找到所有以presto开头的csv文件
    presto_files = [f for f in os.listdir(presto_res_dir) if f.startswith('presto') and f.endswith('.csv')]
    # 读取每个文件，并合并为一个大的csv文件
    presto_data = []
    for file in presto_files:
        # 从文件名中得到scale、partition_num、batch_size
        # 文件名格式为presto-2025-05-01-07-08-55-tpcds_iceberg_100_16_1.csv
        parts = file.split(".")[0].split('-')
        scale = int(parts[-1].split('_')[-3])
        partition_num = int(parts[-1].split('_')[-2])
        batch_size = int(parts[-1].split('_')[-1])
        # 读取文件内容并合并到一个DataFrame中
        file_path = os.path.join(presto_res_dir, file)
        data = pd.read_csv(file_path)
        # 只保留需要的列
        data = data[['query', 'elapsedTimeMillis']]
        data['query'] = data['query'].str.split('_').str[-1].str.split('.').str[0]
        data.columns = ['query_id', 'time']
        data['query_id'] = data['query_id'].astype(int)  # 强制转为整数
        # 将scale、partition_num、batch_size作为列添加到数据中
        data['scale'] = scale
        data['partition_num'] = partition_num
        data['batch_size'] = batch_size
        data['total_files_size_mb'] = table_size[scale] / 1024 / 1024 * (batch_size - 1) / batch_size
        data['read_data_size_mb'] = table_size[scale] / 1024 / 1024 * 1 / batch_size
        presto_data.append(data)
    res = compute_relative_time(pd.concat(presto_data, ignore_index=True))
    res.to_csv(os.path.join(csv_dir, f"{table_name}.csv"), index=False)


def generate_all_csv(presto_res_dir: str, csv_dir: str):
    fact_table = ["store_sales", "store_returns", "web_sales", "web_returns", "catalog_sales", "catalog_returns", "inventory"]
    table_size_dict = {
        "store_sales" : {
            1 : 99550937,
            10 : 1021410131,
            100 : 20735497882,
        },
        "store_returns" : {
            1 : 11731295,
            10 : 120221172,
            100 : 2453635737,
        },
        "web_sales" : {
            1 : 36548459,
            10 : 371825523,
            100 : 7599542077,
        },
        "web_returns" : {
            1 : 3820405,
            10 : 38718326,
            100 : 794933495,
        },
        "catalog_sales" : {
            1 : 79556958,
            10 : 818807345,
            100 : 8310394482,
        },
        "catalog_returns" : {
            1 : 7616093,
            10 : 77841650,
            100 : 790578326,
        },
        "inventory" : {
            1 : 24897976,
            10 : 516552136,
            100 : 3165001551,
        }
    }
    for table_name in fact_table:
        generate_csv(presto_res_dir, csv_dir, table_name, table_size_dict[table_name])
        
        
generate_all_csv("./res", "./model")

from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.neural_network import MLPRegressor


def train_model(model_dir: str, csv_dir:str, table_name: str):
    # 读取数据
    df = pd.read_csv(os.path.join(csv_dir, f"{table_name}.csv"))
    # 特征和标签
    X = df[['query_id', 'partition_num', 'total_files_size_mb', 'read_data_size_mb']]
    y = df['relative_time']
    # 建模
    model = GradientBoostingRegressor(
        n_estimators=100,         # 总共构建多少棵树
        learning_rate=0.1,        # 每棵树的权重（步长）
        max_depth=100,              # 每棵树的最大深度（控制过拟合）
        random_state=42
    )

    model.fit(X, y)
    joblib.dump(model, os.path.join(model_dir, f"{table_name}.pkl"))

def train_all_models(csv_dir:str, save_dir: str):
    fact_tables = ["store_sales", "store_returns", "web_sales", "web_returns", "catalog_sales", "catalog_returns", "inventory"]
    for table_name in fact_tables:
        train_model("./model", "./model", table_name)

train_all_models("./model", "./model")

def plant_model(model_dir: str, table_name: str, total_files_size_mb: int, read_data_size_mb: int):
    # 加载模型
    model = joblib.load(os.path.join(model_dir, f"{table_name}.pkl"))
    # 预测不同分区数下的执行时间（以 data_size=1_000_000 为例）
    partition_nums = np.array([2**i for i in range(0, 10)])
    predicted_times = []
    for partition_num in partition_nums:
        # 生成测试数据
        X_test = pd.DataFrame({
            'query_id': [i for i in range(1, 100)],
            'partition_num': [partition_num] * 99,
            'total_files_size_mb': [total_files_size_mb] * 99,
            'read_data_size_mb': [read_data_size_mb] * 99
        })
        # 预测
        y_pred = model.predict(X_test)
        # print(y_pred)
        # print(y_pred.sum())
        # break
        predicted_times.append(y_pred.sum())
    print(predicted_times)
    # 找到最优分区数
    optimal_index = np.argmin(np.array(predicted_times))
    optimal_partition = partition_nums[optimal_index]
    min_time = y_pred[optimal_index]
    print(f"最优分区数是 {optimal_partition}，预测执行时间为 {min_time:.2f} 秒")

    # 可视化
    plt.plot(partition_nums, predicted_times)
    plt.xlabel("Partition Number")
    plt.ylabel("Predicted Execution Time")
    plt.title("Optimal Partition Search")
    plt.axvline(x=optimal_partition, color='r', linestyle='--')
    plt.show()


def get_best_partition(model_dir: str, table_name: str, total_files_size_mb: int, read_data_size_mb: int):
    # 加载模型
    model = joblib.load(os.path.join(model_dir, f"{table_name}.pkl"))
    # 预测不同分区数下的执行时间（以 data_size=1_000_000 为例）
    partition_nums = np.array([2**i for i in range(0, 10)])
    predicted_times = []
    for partition_num in partition_nums:
        # 生成测试数据
        X_test = pd.DataFrame({
            'query_id': [i for i in range(1, 100)],
            'partition_num': [partition_num] * 99,
            'total_files_size_mb': [total_files_size_mb] * 99,
            'read_data_size_mb': [read_data_size_mb] * 99
        })
        # 预测
        y_pred = model.predict(X_test)
        # print(y_pred)
        # print(y_pred.sum())
        # break
        predicted_times.append(y_pred.sum())
    print(predicted_times)
    # 找到最优分区数
    optimal_index = np.argmin(np.array(predicted_times))
    optimal_partition = partition_nums[optimal_index]
    min_time = y_pred[optimal_index]
    print(f"最优分区数是 {optimal_partition}，预测执行时间为 {min_time:.2f} 秒")
    return optimal_partition


def plant_best_partition(model_dir: str, table_name: str):
    best_partitions = []
    for i in range(1, 1000):
        total_files_size_mb = 10
        read_data_size_mb = i
        best_partition = get_best_partition(model_dir, table_name, total_files_size_mb, read_data_size_mb)
        best_partitions.append(best_partition)

    # 可视化
    plt.scatter(range(1, 1000), best_partitions)
    plt.xlabel("read_data_size_mb")
    plt.ylabel("best_partition")
    plt.title("Optimal Partition Search")
    plt.show()


plant_model("./model", "store_sales", 1000, 1000)

def predict_best_partition(model_dir: str, table_name: str, total_files_size_kb: int, read_data_size_kb: int) -> int:
    # 加载模型
    model = joblib.load(os.path.join(model_dir, f"{table_name}.pkl"))
    def objective(trial):
        parts = trial.suggest_categorical("partition_num", [2**i for i in range(0, 20)])
        features = np.column_stack((np.arange(1, 100), np.full(99, parts), np.full(99, total_files_size_kb), np.full(99, read_data_size_kb)))
        total_pred_time = model.predict(features).sum()
        return total_pred_time
    
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=50)
    print(study.best_params["partition_num"])

predict_best_partition("./model", "store_sales", 1, 1)
