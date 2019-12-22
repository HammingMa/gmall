create external table sale_detail_daycount
(
user_id string comment '用户 id',
sku_id string comment '商品 Id',
user_gender string comment '用户性别',
user_age string comment '用户年龄',
user_level string comment '用户等级',
order_price decimal(10,2) comment '商品价格',
sku_name string comment '商品名称',
sku_tm_id string comment '品牌id',
sku_category3_id string comment '商品三级品类id',
sku_category2_id string comment '商品二级品类id',
sku_category1_id string comment '商品一级品类id',
sku_category3_name string comment '商品三级品类名称',
sku_category2_name string comment '商品二级品类名称',
sku_category1_name string comment '商品一级品类名称',
spu_id string comment '商品 spu',
sku_num int comment '购买个数',
order_count string comment '当日下单单数',
order_amount string comment '当日下单金额'
) COMMENT '用户购买商品明细表'
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_sale_detail_daycount/'
tblproperties ('parquet.compression'='snappy');




create table tmp_sale_01 as
select * from
(SELECT
  '0001' AS user_id,
  '00010' AS sku_id,
  '男' AS user_gender,
  '23' AS user_age,
  '12' AS user_level,
  '20' AS order_price,
  '十月稻田 沁州黄小米 (黄小米 五谷杂粮 山西特产 真空装 大米伴侣 粥米搭档) 2.5kg' AS sku_name,
  '1001' AS sku_tm_id,
  '01' AS sku_category3_id,
  '02' AS sku_category2_id,
  '03' AS sku_category1_id,
  '01' AS sku_category3_name,
  '02' AS sku_category2_name,
  '03' AS sku_category1_name,
  '商品 spu' AS spu_id,
  '2' AS sku_num,
  '10' AS order_count,
  '200' AS order_amount
UNION ALL
SELECT
  '0002' AS user_id,
  '00010' AS sku_id,
  '男' AS user_gender,
  '23' AS user_age,
  '12' AS user_level,
  '20' AS order_price,
  '小米（MI） 小米路由器4 双千兆路由器 无线家用穿墙1200M高速双频wifi 千兆版 千兆端口光纤适用' AS sku_name,
  '1001' AS sku_tm_id,
  '01' AS sku_category3_id,
  '02' AS sku_category2_id,
  '03' AS sku_category1_id,
  '01' AS sku_category3_name,
  '02' AS sku_category2_name,
  '03' AS sku_category1_name,
  '商品 spu' AS spu_id,
  '2' AS sku_num,
  '10' AS order_count,
  '200' AS order_amount
UNION ALL
SELECT
  '0003' AS user_id,
  '00010' AS sku_id,
  '男' AS user_gender,
  '23' AS user_age,
  '12' AS user_level,
  '20' AS order_price,
  '荣耀10青春版 幻彩渐变 2400万AI自拍 全网通版4GB+64GB 渐变蓝 移动联通电信4G全面屏手机 双卡双待' AS sku_name,
  '1001' AS sku_tm_id,
  '01' AS sku_category3_id,
  '02' AS sku_category2_id,
  '03' AS sku_category1_id,
  '01' AS sku_category3_name,
  '02' AS sku_category2_name,
  '03' AS sku_category1_name,
  '商品 spu' AS spu_id,
  '2' AS sku_num,
  '10' AS order_count,
  '200' AS order_amount ) a

create table tmp_sale_02 as
select
  *
from
  (
    select
      '0001' as user_id,
      sku_id,
      user_gender,
      user_age,
      user_level,
      order_price,
      sku_name,
      sku_tm_id,
      sku_category3_id,
      sku_category2_id,
      sku_category1_id,
      sku_category3_name,
      sku_category2_name,
      sku_category1_name,
      spu_id,
      sku_num,
      order_count,
      order_amount
    from
      tmp_sale_01
    where
      user_id <> '0001'
    UNION ALL
    select
      '0002' as user_id,
      sku_id,
      user_gender,
      user_age,
      user_level,
      order_price,
      sku_name,
      sku_tm_id,
      sku_category3_id,
      sku_category2_id,
      sku_category1_id,
      sku_category3_name,
      sku_category2_name,
      sku_category1_name,
      spu_id,
      sku_num,
      order_count,
      order_amount
    from
      tmp_sale_01
    where
      user_id <> '0002'
    UNION ALL
    select
      '0003' as user_id,
      sku_id,
      user_gender,
      user_age,
      user_level,
      order_price,
      sku_name,
      sku_tm_id,
      sku_category3_id,
      sku_category2_id,
      sku_category1_id,
      sku_category3_name,
      sku_category2_name,
      sku_category1_name,
      spu_id,
      sku_num,
      order_count,
      order_amount
    from
      tmp_sale_01
    where
      user_id <> '0003'
  ) a;


insert overWrite table gmall_dws.sale_detail_daycount partition(dt = '20191221')
select
  *
from
  (
    select
      *
    from
      tmp_sale_02
    UNION all
    select
      *
    from
      tmp_sale_01
  ) a;




select
  user_id,
  sku_id,
  if(user_id =='001',user_gender,'女') as user_gender,
  if(user_id =='001',18,if(user_id=='002',28,40)) user_age,
  user_level,
  cast(100.6 as double) as sku_price,
  sku_name,
  sku_tm_id,
  sku_category3_id,
  sku_category2_id,
  sku_category1_id,
  sku_category3_name,
  sku_category2_name,
  sku_category1_name,
  spu_id,
  sku_num,
  cast(order_count as bigint) order_count,
  cast(order_amount as double) order_amount,
  dt
from
  gmall_dws.sale_detail_daycount