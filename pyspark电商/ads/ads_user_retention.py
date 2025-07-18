
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# 1. 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE gmall")
    return spark


def select_to_hive(jdbcDF, tableName, partition_date):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"gmall.{tableName}")

# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    select_sql = f"""
        select * from ads_user_retention
    union
    select '20250701' ds,
           login_date_first create_date,
           datediff('2022-06-08', login_date_first) retention_day,
           sum(if(login_date_last = '2022-06-08', 1, 0)) retention_count,
           count(*) new_user_count,
           cast(sum(if(login_date_last = '2022-06-08', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
    from (
             select user_id,
                    login_date_last,
                    login_date_first
             from dws_user_user_login_td
             where ds = '20250701'
               and login_date_first >= date_add('2022-06-15', -7)
               and login_date_first < '2022-07-30'
         ) t1
    group by login_date_first;
    """

    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)
    spark.sql("select * from gmall.ads_user_retention limit 10").show()

if __name__ == "__main__":
    target_date = '2025-07-01'

    execute_hive_insert(target_date,'ads_user_retention')



