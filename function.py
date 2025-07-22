from pyspark.sql import SparkSession
#from sedona.core.SpatialRDD import SpatialRDD
#from sedona.spark import SedonaContext
import json
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min, max, lit
from py4j.java_gateway import java_import
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import round
#from sedona.utils.adapter import Adapter
import os
import shutil
import subprocess
#A. process-shp
#1 将 shp 转化为 geojson(hdfs)
def process_hdfs_directory(hdfs_dir,geojson_dir):
    '''
    将各个目录下的文件shp,shx,bdf 转化为geojson格式
    Args:
        hdfs_dir: "/paper3/Openstreet/tj/input"
        geojson_dir: ""hdfs://192.168.36.128:9000/paper3/Openstreet/tj/geojson/
    Returns:
    '''
    print("-----" * 25)
    print("process_hdfs_directory,shp to geojson..............")
    HDFS_CMD = "/opt/hadoop-3.4.0/bin/hdfs"
    result = subprocess.run(
        [HDFS_CMD, "dfs", "-ls", hdfs_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if result.returncode != 0:
        print(f"Error listing HDFS directory: {result.stderr}")
        return []
    lines = result.stdout.strip().split("\n")
    for line in lines:
        print(line)
        if line.startswith("d"):  # d 表示是目录
            parts = line.strip().split()
            if len(parts) >= 8:
                dir_path = parts[-1]  # 最后一列是路径
                dir_name = dir_path.strip("/").split("/")[-1]  # 提取目录名
                shp_to_geojson(dir_path,geojson_dir+f"{dir_name}_geojson")
    return []
#2 normalize geoson point, and save to parquet(hdfs)
def normalize_geojson_points(geojson_dir,parquet_path,bounds_path):
    '''

    Args:
        geojson_dir: "hdfs://192.168.36.128:9000/paper3/Openstreet/tj/geojson/"
        parquet_path: "hdfs://192.168.36.128:9000/paper3/Openstreet/tj/parquet"
        bounds_path: "hdfs://192.168.36.128:9000/paper3/bounds.csv"

    Returns:

    '''
    print("-----" * 25)
    print("normalize_geojson_points..............")
    # 初始化 Spark
    spark = init_spark_and_cleanup(bounds_path, parquet_path)
    sc = spark.sparkContext
    # ====== Step 1: 读取你提供的 GeoJSON 数据为 RDD（每行为一个 JSON Feature） ======
    print("geojson_dir:",geojson_dir)
    lines = sc.textFile(geojson_dir+"*",minPartitions=10) # read hdfs://192.168.36.128:9000/paper3/Openstreet/tj/geojson/building_geojson
    # ====== Step 2: 提取所有坐标点 ======
    def extract_points(json_line):
        try:
            feature = json.loads(json_line)
            type =feature["geometry"]['type']
            coords = feature["geometry"]["coordinates"]
            if type == "LineString":
                return [tuple(coord) for coord in coords]  # 每个为 (lon, lat)
            elif type == "Polygon":
                return [tuple(coord) for ring in coords for coord in ring]
            elif type =="Point":
                return [tuple(coords)]
            else:
                return []
        except Exception:
            return []
    points_rdd = lines.flatMap(extract_points)
    print("*****" * 20)
    print("first ,convert geojson to rdd_id........")
    print("point_rdd:",points_rdd.count())
    id_points_rdd = points_rdd.zipWithIndex().map(lambda x: (x[1] + 1, x[0][0], x[0][1]))
    # ====== 可选: 输出示例 ======
    print("id_points_rdd.take(5):",id_points_rdd.take(2))
    # 指定字段名并转为 DataFrame
    df = id_points_rdd.map(lambda x: Row(id=x[0], lon=x[1], lat=x[2])).toDF()
    df.show(5)
    normalize_columns(df, bounds_path,parquet_path)
    print("-----" * 25)

#B. process-csv(normalize csv point, and save to parquet(hdfs))
def normalize_csv_points(csv_path,parquet_path,bounds_path):
    print("-----" * 25)
    print("normalize_csv_points..............")
    # 初始化 Spark
    spark = init_spark_and_cleanup(bounds_path, parquet_path)
    # 2. 读取 CSV 文件
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    # 生成带编号的新列（编号列名为 "id"）
    df_with_id = df.withColumn("id", monotonically_increasing_id())
    # 将编号列移到最前面
    cols = ["id"] + [col for col in df.columns[:-1]]
    df_with_id = df_with_id.select(cols)
    # 显示结果
    df_with_id.show(5)
    normalize_columns(df_with_id, bounds_path, parquet_path)
def init_spark_and_cleanup(bounds_path, parquet_path):
    spark = SparkSession.builder \
        .appName("ExtractPointsFromGeoJSON") \
        .master(f"spark://{master}:7077") \
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{master}:9000") \
        .config("spark.network.timeout", 600) \
        .config("spark.executor.heartbeatInterval", 60) \
        .getOrCreate()
    spark.conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    clean_output_paths(spark, parquet_path)
    clean_output_paths(spark, bounds_path)
    return spark
def clean_output_paths(spark,path):
    # 获取 Hadoop FileSystem 对象
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    # 转换为 Hadoop Path 对象
    parquet_path_obj = spark._jvm.org.apache.hadoop.fs.Path(path)
    # 如果存在则删除
    if fs.exists(parquet_path_obj):
        fs.delete(parquet_path_obj, True)  # True 表示递归删
    return
def shp_to_geojson(filepath,geojson_path):
    # 初始化 SparkSession
    spark = SparkSession.builder \
        .appName("SedonaApp") \
        .master("yarn") \
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{master}:9000") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .config("spark.jars",
                "/home/ljy/.ivy2/jars/org.apache.sedona_sedona-common-1.7.2.jar,"
                "/home/ljy/.ivy2/jars/org.apache.sedona_sedona-spark-3.5_2.12-1.7.2.jar,"
                "/home/ljy/.ivy2/jars/org.apache.sedona_sedona-spark-common-3.5_2.12-1.7.2.jar,"
                "/home/ljy/.ivy2/jars/jts-core-1.18.2.jar,"
                "/home/ljy/.ivy2/jars/jts2geojson-0.14.3.jar")\
        .getOrCreate()
    # org.locationtech.jts_
    # print("1:",spark.sparkContext.getConf().get("spark.jars"))
    # print("2:",spark._jvm.org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader)
    # print("3:",spark._jvm.org.apache.sedona.core.spatialRDD.SpatialRDD)
    # print("4:",spark._jvm.org.wololo.jts2geojson.GeoJSONWriter)
    # 1. 加载 shapefiles 作为 GeometryRDD（Java 对象）
    jvm_rdd = spark._jvm.org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader.readToGeometryRDD(spark._jsc,filepath)
    # 2. 输出 GeoJSON，每个分区一个文件（HDFS、本地或 S3 路径均可）
    clean_output_paths(spark, geojson_path)
    jvm_rdd.saveAsGeoJSON(geojson_path)
    return
def normalize_columns(df: DataFrame, bounds_path: str, parquet_path: str) -> DataFrame:
    print("*****" * 20)
    print("Second,normalize_columns..............")
    # 1. 构造聚合表达式
    agg_exprs = []
    for c in df.columns[1:]:
        agg_exprs.append(min(col(c)).alias(f"min_{c}"))
        agg_exprs.append(max(col(c)).alias(f"max_{c}"))

    # 2. 执行聚合，收集最小/最大值
    stats = df.agg(*agg_exprs).collect()[0]
    stats_dict = stats.asDict()

    # 3. 打印所有列的 min/max，并保存 bounds 到 CSV
    bounds = []
    bounds_schema = []

    for c in df.columns[1:]:
        min_val = stats_dict[f"min_{c}"]
        max_val = stats_dict[f"max_{c}"]
        bounds.extend([min_val, max_val])
        bounds_schema.extend([f"min_{c}", f"max_{c}"])
        print(f"{c}: min = {min_val}, max = {max_val}")

    bounds_df = df.sparkSession.createDataFrame([bounds], schema=bounds_schema)
    # bounds_df.printSchema()
    bounds_df.write.mode("overwrite").csv(bounds_path)
    # 4. 构造归一化后的新 DataFrame
    df_norm = df
    for c in df.columns[1:]:
        min_val = stats_dict[f"min_{c}"]
        max_val = stats_dict[f"max_{c}"]

        if max_val != min_val:
            df_norm = df_norm.withColumn(
                c,
                round((col(c) - lit(min_val)) / (max_val - min_val),4)
            )
        else:
            df_norm = df_norm.withColumn(c, lit(0.0))  # 避免除以0
    df_norm.show(2)
    # 5. 写为 Parquet 格式
    df_norm.write.mode("overwrite").parquet(parquet_path)
    return df_norm
def organize_files_by_prefix(hdfs_directory):
    '''
    将某目录下的文件按名称存放，如buildings,roads
    Args:
        hdfs_directory: "/paper3/Openstreet/tj/input"

    Returns:

    '''
    print("-----" * 25)
    print("organize_files_by_prefix..............")
    HDFS_CMD = "/opt/hadoop-3.4.0/bin/hdfs"
    # 列出 HDFS 文件
    result = subprocess.run(
        [HDFS_CMD, "dfs", "-ls", hdfs_directory],
        capture_output=True, text=True
    )
    for line in result.stdout.splitlines():
        if not line.startswith("Found"):
            parts = line.split()
            hdfs_file_path = parts[-1]
            filename = os.path.basename(hdfs_file_path)
            name, ext = os.path.splitext(filename)

            if ext.lower() in [".shp", ".shx",".dbf"]:
                target_dir = f"{hdfs_directory}/{name}"
                # 创建目录（如果不存在）
                subprocess.run([HDFS_CMD, "dfs", "-mkdir", "-p", target_dir])
                # 移动文件
                subprocess.run([HDFS_CMD, "dfs", "-mv", hdfs_file_path, f"{target_dir}/{filename}"])
                print(f"Moved {filename} to {target_dir}/")
            else:
                subprocess.run([HDFS_CMD, "dfs", "-rm", hdfs_file_path])

master="8.130.215.252"
if __name__ == "__main__":

    base_dir = f"hdfs://{master}:9000/"
    #shp-tiger
    input = base_dir + "tiger/shp/2/"
    geojson_path= base_dir + "tiger/geojson"
    shp_to_geojson(input,geojson_path)
    parquet_path=base_dir + "tiger/tiger_parquet"
    bounds_path=base_dir + "tiger/tiger_bounds.csv"
    normalize_geojson_points(geojson_path,parquet_path,bounds_path)

    #shp-openstreet
    hdfs_dir="/paper3/Openstreet/tj/input"
    # organize_files_by_prefix(hdfs_dir)
    # geojson_dir= base_dir + "Openstreet/tj/geojson/"
    # process_hdfs_directory(hdfs_dir,geojson_dir)
    # parquet_path=base_dir + "Openstreet/tj/parquet"
    # bounds_path=base_dir + "Openstreet/tj/bounds.csv"
    # normalize_geojson_points(geojson_dir,parquet_path,bounds_path)

    # csv-real
    # csv_name =["Musk","data_MF"]
    # for name in csv_name:
    #     csv_path=base_dir+f"real/input/{name}.csv"
    #     parquet_path=base_dir + f"real/output/{name}_parquet"
    #     bounds_path=base_dir + f"real/output/{name}_bounds.csv"
    #     normalize_csv_points(csv_path,parquet_path,bounds_path)