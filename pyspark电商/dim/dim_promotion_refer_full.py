from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_spark_session():
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("use gmall")
    return spark

def select_to_hive(jdbcDF,tableName,partition_date):
    jdbcDF.write \
        .mode("append") \
        .insertInto(f"gmall.{tableName}")

def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    select_sql = f"""
    select
      `id`,
      `refer_name`,
      `create_time`,
      `operate_time`
    from ods_promotion_refer
    where ds='20211214'
    """

    print(f"开始执行sql插入,分区日期:{partition_date}")
    df1 = spark.sql(select_sql)

    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    select_to_hive(df_with_partition,tableName,partition_date)


if __name__ == "__main__":
    target_date = '2021-12-14'

    execute_hive_insert(target_date,"dim_promotion_refer_full")
