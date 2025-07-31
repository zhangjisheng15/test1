
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

    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("use gmall")
    return spark

def select_to_hive(jdbcDF,tableName,partition_date):
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"gmall.{tableName}")

def execute_hive_insert(partition_date: str,tableName):
    spark = get_spark_session()

    select_sql = """
    select
        id,
        user_id,
        sku_id,
        sku_name,
        sku_num
    from ods_cart_info
    where ds='20250625'
    and is_ordered='0';
    """

    print(f"开始执行SQL插入，分区日期:{partition_date}")
    df1 = spark.sql(select_sql)

    df_with_partition = df1.withColumn("ds",lit(partition_date))

    print(f"SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    select_to_hive(df_with_partition,tableName,partition_date)

    spark.sql("select * from gmall.dwd_trade_cart_full limit 10").show()

if __name__ == "__main__":
     target_date = "2025-06-25"

     execute_hive_insert(target_date,"dwd_trade_cart_full")

