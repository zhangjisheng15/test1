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
    select
        province_id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2,
        sum(if(ds>=date_add('2021-12-14',-6),order_count_1d,0)),
        sum(if(ds>=date_add('2021-12-14',-6),order_original_amount_1d,0)),
        sum(if(ds>=date_add('2021-12-14',-6),activity_reduce_amount_1d,0)),
        sum(if(ds>=date_add('2021-12-14',-6),coupon_reduce_amount_1d,0)),
        sum(if(ds>=date_add('2021-12-14',-6),order_total_amount_1d,0)),
        sum(order_count_1d),
        sum(order_original_amount_1d),
        sum(activity_reduce_amount_1d),
        sum(coupon_reduce_amount_1d),
        sum(order_total_amount_1d)
    from dws_trade_province_order_1d
    where ds>=date_add('2021-12-14',-29)
      and ds<='2021-12-14'
    group by province_id,province_name,area_code,iso_code,iso_3166_2;
    """

    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)

    spark.sql("select * from gmall.dws_trade_province_order_nd limit 10").show()

if __name__ == "__main__":
    target_date = '2021-12-14'

    execute_hive_insert(target_date,'dws_trade_province_order_nd')


