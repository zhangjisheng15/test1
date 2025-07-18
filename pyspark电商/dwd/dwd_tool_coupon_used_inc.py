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


def execute_hive_insert(partition_date: str,tableName):
    spark = get_spark_session()

    select_sql = f"""
    select
        data.id,
        data.coupon_id,
        data.user_id,
        data.order_id,
        date_format(data.used_time,'yyyy-MM-dd') date_id,
        data.used_time,
        date_format(data.used_time,'yyyy-MM-dd') as ds
    from ods_coupon_use data
    where ds='20250625'
    and data.used_time is not null;
    """

    print(f"开始执行SQL插入，分区日期:{partition_date}")
    df1 = spark.sql(select_sql)

    df_with_partition = df1.withColumn("ds",lit(partition_date))

    print(f"SQL执行完成,分区{partition_date}操作成功")
    df_with_partition.show()

    select_to_hive(df_with_partition,tableName,partition_date)

    spark.sql("select * from gmall.dwd_tool_coupon_used_inc limit 10").show()


if __name__ == "__main__":
    target_date = "2025-06-25"

    execute_hive_insert(target_date,"dwd_tool_coupon_used_inc")


