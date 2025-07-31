
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveDwdETL") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()




# 创建数据库
spark.sql("CREATE DATABASE IF NOT EXISTS gongdan")
spark.sql("USE gongdan")

# 商品表
spark.sql("""
    CREATE TABLE IF NOT EXISTS gongdan.dim_product (
        sku_id STRING,
        sku_name STRING,
        is_main_sku INT  -- 1:引流/热销主商品，0:普通商品
    )
    STORED AS PARQUET
""")

# 商品访问日志
spark.sql("""
    CREATE TABLE IF NOT EXISTS gongdan.dwd_product_visit_log (
        user_id STRING,
        sku_id STRING,
        visit_time TIMESTAMP
    )
    STORED AS PARQUET
""")

# 商品加购日志
spark.sql("""
    CREATE TABLE IF NOT EXISTS gongdan.dwd_product_cart_log (
        user_id STRING,
        sku_id STRING,
        cart_time TIMESTAMP
    )
    STORED AS PARQUET
""")

# 商品支付日志
spark.sql("""
    CREATE TABLE IF NOT EXISTS gongdan.dwd_product_order_log (
        user_id STRING,
        sku_id STRING,
        pay_time TIMESTAMP
    )
    STORED AS PARQUET
""")

print("✅ Hive 表创建完成（库：gongdan）")
