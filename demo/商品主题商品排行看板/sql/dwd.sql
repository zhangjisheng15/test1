
use gd_02;

-- 创建用户行为明细宽表，整合用户行为与商品维度
CREATE  TABLE dwd_user_behavior_detail (
                                                    log_id INT COMMENT '行为日志ID',
                                                    session_id STRING COMMENT '用户会话ID',
                                                    user_id INT COMMENT '用户ID（去重后用于计算访客数）',
                                                    page_id STRING COMMENT '访问页面ID',
                                                    product_id STRING COMMENT '关联商品ID（从商品维度表匹配）',
                                                    product_name STRING COMMENT '商品名称（关联维度表）',
                                                    category_name STRING COMMENT '商品分类名称（支持分类筛选）',
                                                    action_type STRING COMMENT '行为类型（浏览/下单等，映射is_order）',
                                                    device_type STRING COMMENT '设备类型（无线/PC）',
                                                    visit_time DATE COMMENT '访问时间（用于时间维度筛选）',
                                                    stay_duration INT COMMENT '停留时长（秒）',
                                                    is_order TINYINT COMMENT '是否下单（1=是，0=否，用于计算支付转化率）',
                                                    refer_page_id STRING COMMENT '来源页面ID（用于流量来源分析）',
                                                    ds STRING COMMENT '分区日期'
);

-- 插入数据（关联商品维度，补充明细字段）
INSERT INTO TABLE dwd_user_behavior_detail
SELECT
    u.log_id ,
        u.session_id ,
        u.user_id ,
        u.page_id ,
        p.product_id,
        p.product_name,
        p.category_name ,
        CASE  -- 行为类型映射，与文档"指标展示"对应
            WHEN u.is_order = 1 THEN 'payment'
            ELSE 'view'
            END AS action_type,
    u.device_type ,
        u.visit_time ,
        u.stay_duration ,
        u.is_order,
        u.refer_page_id,
        '2025-08-04' AS ds
FROM ods_user_log u
         LEFT JOIN dim_product p ON u.page_id = p.page_id  -- 关联商品维度
WHERE u.ds = '20250805';


select * from dwd_user_behavior_detail;