

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
    oi.id,
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    create_time,
    date_format(callback_time,'yyyy-MM-dd'),
    callback_time,
    date_format(finish_time,'yyyy-MM-dd'),
    finish_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0),
    nvl(date_format(finish_time,'yyyy-MM-dd'),'9999-12-31')
from
    (
        select
            data.id,
            data.user_id,
            data.province_id,
            data.create_time,
            data.original_total_amount,
            data.activity_reduce_amount,
            data.coupon_reduce_amount,
            data.total_amount
        from ods_order_info data
        where ds='20250625'
    )oi
        left join
    (
        select
            data.order_id,
            data.callback_time,
            data.total_amount payment_amount
        from ods_payment_info data
        where ds='20250625'
          and data.payment_status='1602'
    )pi
    on oi.id=pi.order_id
        left join
    (
        select
            data.order_id,
            data.create_time finish_time
        from ods_order_status_log data
        where ds='20250625'
          and data.order_status='1004'
    )log
    on oi.id=log.order_id;
    """

    print(f"开始执行SQL插入，分区日期:{partition_date}")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;").show()
    df1 = spark.sql(select_sql)

    df_with_partition = df1.withColumn("ds",lit(partition_date))

    print(f"SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    select_to_hive(df_with_partition,tableName,partition_date)

    spark.sql("select * from gmall.dwd_trade_trade_flow_acc limit 5").show()

if __name__ == "__main__":
    target_date = '2025-06-25'

    execute_hive_insert(target_date,"dwd_trade_trade_flow_acc")


