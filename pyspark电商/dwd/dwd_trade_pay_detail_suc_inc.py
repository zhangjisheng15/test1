
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# 1. 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
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
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
    from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods_order_detail data
        where ds = '20250625'
    ) od
        join
    (
        select
            data.user_id,
            data.order_id,
            data.payment_type,
            data.callback_time
        from ods_payment_info data
        where ds='20250625'
          and data.payment_status='1602'
    ) pi
    on od.order_id=pi.order_id
        left join
    (
        select
            data.id,
            data.province_id
        from ods_order_info data
        where ds = '20250625'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods_order_detail_activity data
        where ds = '20250625'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods_order_detail_coupon data
        where ds = '20250625'
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20250625'
          and parent_code='11'
    ) pay_dic
    on pi.payment_type=pay_dic.dic_code;
    """

    print(f"开始执行SQL输入,分区日期,{partition_date}")
    df1 = spark.sql(select_sql)

    df_with_partition = df1.withColumn("ds",lit(partition_date))

    print(f"SQL执行完成,分区:{partition_date}操作完成")
    df_with_partition.show()

    select_to_hive(df_with_partition,tableName,partition_date)

    spark.sql("select * from gmall.dwd_trade_pay_detail_suc_inc limit 10").show()


if __name__ == "__main__":
    target_date = '2025-06-25'

    execute_hive_insert(target_date,"dwd_trade_pay_detail_suc_inc")



