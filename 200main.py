# 文件结构说明：
# - config.py: 初始化配置、广播变量等
# - partitioning.py: 分区规划、划分及扩展逻辑
# - knn_lrd_lof.py: 第二阶段KNN、LRD和LOF计算
# - utils.py: 公共函数
# - main.py: 主函数，调用各模块逻辑
import sys

# 以下是重构后的 main.py 文件

from pyspark import SparkContext, SparkConf, Broadcast, SparkFiles
from pyspark.sql import SparkSession

from optimized.config import initialize_configuration
from optimized.cell import compute_cell_plan
from optimized.knn_lrd_lof import compute_second_knn, compute_nof
from optimized.utils import load_data
from optimized.partitioning import compute_partition_plan, expand_partitions, assign_partitions, assign_support_partitions
# import pydevd_pycharm

# from knn_lrd_lof import compute_second_knn, compute_lrd, compute_lof


def run_spark():
    # pydevd_pycharm.settrace('你的本地IP', port=12345, stdoutToServer=True, stderrToServer=True, suspend=True)

    # conf = SparkConf().setAppName("KNN LOF Detection").setMaster("local[*]")\
    # .set("spark.network.timeout", "300s") \
    # .set("spark.executor.heartbeatInterval", "100s")
    # sc = SparkContext(conf=conf)
    # sc.addPyFile("/home/ljy/ddlof.zip")
    # 初始化 Spark
    spark = SparkSession.builder \
        .appName("DDLOF") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://172.23.186.223:9000") \
        .config("spark.network.timeout", 600) \
        .config("spark.executor.heartbeatInterval", 600) \
        .getOrCreate()
    sc = spark.sparkContext
    sc.addPyFile("/tmp/pycharm_project_714/ddlof.zip")
        #.master("yarn") \
    # 初始化配置和广播变量
    config = initialize_configuration()
    # global bro_config_dict
    bro_config_dict = sc.broadcast(config)

    # 加载并解析数据
    # data_path = "data.txt"
    # raw_data = sc.textFile(data_path).cache()
    # raw_data.cache()
    # data_path = "hdfs://192.168.36.128:9000/paper3/real/output/Musk_parquet"
    data_path = "hdfs://172.23.186.223:9000/tiger/point/200/tiger_parquet"

    #raw_data = spark.read.parquet(data_path).rdd.map(
     #                   lambda row: tuple(row[i] for i in range(167))
    #)
    raw_data = spark.read.parquet(data_path).rdd
    #print("raw_data:",raw_data.take(2))
    # sample_data = raw_data.takeSample(False, 10000, seed=42)
    # sample_rdd = sc.parallelize(sample_data)
    # print("sample_rdd:", sample_rdd.count())
    print("......Selecting part of data to part......")
    part_data = raw_data.map(lambda line: process_record(line,bro_config_dict)).filter(lambda x: x is not None)
    #print(part_data.count(),part_data.take(2))
    print("**" * 50)

    # 分区与扩展逻辑
    print("1.Generating partitionplan...")
    partition_rdd,partition_plan_dict = compute_partition_plan(part_data,bro_config_dict)
    print("**" * 50)
    print("2.Getting partition for each cell...")
    core_cell_rdd,cell_plan_dict = compute_cell_plan(partition_rdd,bro_config_dict)
    print("**" * 50)
    #
    bro_cell_plan = sc.broadcast(cell_plan_dict)
    print("3.Getting partition for each record...")
    data_partition_rdd = assign_partitions(raw_data,bro_config_dict,bro_cell_plan)
    # 过滤掉 None 或格式不正确的元素
    # data_partition_rdd = data_partition_rdd.filter(
    #     lambda x: x is not None and isinstance(x, tuple) and len(x) == 2
    # )
    print("**" * 50)
    #
    print("4.Calculating KNN distances and getting expand border for each partition...")
    data_info_rdd,extend_partitionplan = expand_partitions(data_partition_rdd, bro_config_dict,partition_plan_dict)
    first_unfinished_rdd = data_info_rdd.filter(lambda x: (x[3] != 'O'))  # F/L，next to calculate
    if first_unfinished_rdd.count() == 0:
        print("all of lof have been calculated")
        return
    print("**" * 50)
    # #
    global countExceedPartitions
    countExceedPartitions = sc.accumulator(0)
    print("5.Getting support_partition for each cell...")
    cell_support_dict = assign_support_partitions(extend_partitionplan,bro_config_dict,countExceedPartitions)
    print("**" * 50)
    # #
    print("6.second calculate knn...")
    bro_cell_support = sc.broadcast(cell_support_dict)
    support_data_plan_rdd,knn_dist_second_rdd =compute_second_knn(data_info_rdd,bro_config_dict,bro_cell_support)

    sec_unfinished_rdd = knn_dist_second_rdd.filter(lambda x: (x[3] != 'O'))  # L，next to calculate
    sec_unfinished_rdd.cache()
    # if sec_unfinished_rdd.count()==0:
    #     print("all of lof have been calculated................................")
    #     print("--" * 50)
    #     return
    print("**" * 50)#
    #
    print("7.cal nof...")
    nof_rdd= compute_nof(support_data_plan_rdd,knn_dist_second_rdd,sec_unfinished_rdd,bro_config_dict)

    third_unfinished_rdd = nof_rdd.filter(lambda x: (x[3] != 'O'))  # L，next to calculate
    third_unfinished_rdd.cache()
    # if third_unfinished_rdd.count()==0:
    #     print("all of lof have been calculated................................")
    #     print("--" * 50)
    #     return
    print("**" * 50)

    # 输出结果
    # lof_result.saveAsTextFile(config['output_path'])
    sc.stop()


def process_record(line,bro_config_dict):
    '''
            # 处理每一行数据的 map 函数(比例选择部分数据分区)
            Args:
                line: [1,40.92849880008652,28.587665115364025]

            Returns:[(0, ['53.88706251469083', '90.45257679028818']),...]

            '''
    # 获取行数据并进行处理
    data = line
    value = [round(float(x), 2) for x in data[1:]]  # 数据
    index = int(data[0])  # id
    # value = line[1]
    id = index % bro_config_dict.value['denominator']  # Calculate partition i
    # Only process records with id == 0
    if id == 0:
        return id, value
    else:
        return None


if __name__ == "__main__":
    run_spark()

