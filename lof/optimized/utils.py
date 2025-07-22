import os

import numpy as np
from pyspark import SparkContext
def load_data(sc,file_path):
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()
    if ext == '.csv':
        # 读取 CSV 文件为 RDD（每一行是一个字符串）
        csv_rdd = sc.textFile(file_path)
        # 如果文件有表头，跳过表头
        header = csv_rdd.first()
        data_rdd = csv_rdd.filter(lambda row: row != header)
        # 将字符串按逗号拆分为 float 列表
        parsed_rdd = data_rdd.map(lambda line: [float(x) for x in line.split(",")[:-1]])
        # 给每条记录编号（从 1 开始）
        indexed_rdd = parsed_rdd.zipWithIndex().map(lambda x: (x[1] + 1, x[0]))
    elif ext == '.npy':
        # 加载 .npy 文件
        data = np.load(file_path)
        # 转换为 RDD
        data_rdd = sc.parallelize(data.tolist())
        print(data_rdd.take(2))
        # 添加编号（从1开始）
        indexed_rdd = data_rdd.zipWithIndex().map(lambda x: [x[1] + 1] + x[0])
        # 转换为字符串格式
        parsed_rdd  = indexed_rdd.map(lambda row: ",".join(map(str, row)))
    # 打印前几条数据查看
    print("number of indexed_rdd:", indexed_rdd.count(), indexed_rdd.take(2))
    return indexed_rdd

def chi_square(frequency):
    expected = sum(frequency) / len(frequency)
    chi_square_value = sum([(f - expected) ** 2 / expected for f in frequency])
    return chi_square_value

def output_data_info(can_calculate_lrd, sorted_data):
    data_info = []
    sep1 = ","  # 相当于 SQConfig.sepStrForRecord
    sep2 = "|"  # 相当于 SQConfig.sepStrForIDDist
    # output records

    for i in range(len(sorted_data)):
        o_R = sorted_data[i]
        line = [o_R.get_obj()[-1],o_R.get_obj()[:-1],o_R.get_partition_id(),o_R.get_type(),o_R.get_kdist(),o_R.get_lrd(),o_R.get_lof()]
        knn_info =""
        for keyMap, valueMap in o_R.get_knn_in_detail().items():
            tempKdist = 0.0
            tempLrd = 0.0
            if keyMap in can_calculate_lrd:
                tempObject = can_calculate_lrd[keyMap]  # lof modify lrd
                tempKdist = tempObject.get_kdist()
                tempLrd = tempObject.get_lrd()
            knn_info += f"{keyMap}{sep2}{valueMap[0]}{sep2}{valueMap[1]}{sep2}{tempKdist}{sep2}{tempLrd}{sep1}"
        line.append(knn_info[:-1])
        line.append(o_R.get_whose_support())
        data_info.append(line)

    return data_info

 # 初始化 SparkContext
# sc = SparkContext("local", "CSV to RDD with Index")
# raw_data = load_data(sc,"hdfs://192.168.36.128:9000/rdd/Musk.csv")
