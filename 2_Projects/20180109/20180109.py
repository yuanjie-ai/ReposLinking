# -*- coding: utf-8 -*-
from Jie import *
t = time.strftime("%Y%m%d",time.localtime(time.time()))
t_0, t_1, t_2, t_3, t_4, t_5, t_6, t_7 = [time.strftime("%Y-%m-%d", time.localtime(time.time() - i)) for i in 60*60*24*np.arange(8)]

t_3M = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*3))
t_6M = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*6))
t_1Y = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*12))

# 推荐基础信息表
df = spark.table('fdm_dpa.mls_recommender_base_info_detail') \
          .selectExpr('acct_no', 'member_id', 'l2_prod_group_cd cate', 'l3_prod_group_cd item', 'entry_time dt') \
          .dropna() \
          .filter(" item not in ('', '1', '2') ") \
          .filter(col('dt') > t_3M) \
          .withColumn('business_type', when(col('cate').isin([str(i) for i in range(1018, 1025)]), 'chp') \
                                      .when(col('cate').isin([str(i) for i in range(1031, 1041)]), 'ins') \
                                      .otherwise('fin'))
df_ins = df[df.business_type == 'ins'].cache()

# 在售商品流行度: ins => channel_code = 'SC1001'
ins_in_sale = spark.table('fdm_sor.sor_prd_ins_product') \
                   .filter("channel_code = 'SC1001' and status = 8") \
                   .replace(['R' + str(i) for i in range(8004001,8004011)], [str(i) for i in range(1031,1041)], 'cate_code') \
                   .select(concat_ws('-', 'cate_code', 'product_code').name('ins_in_sale'))
ins_in_sale = SparkCol.colToList(ins_in_sale)
df = df_ins[concat_ws('-', 'cate', 'item').isin(ins_in_sale)]
item_hot = df.groupBy('cate', 'item').count()

# 用户品类偏好
u_c = df_ins.withColumn('dt', datediff(lit(t_0), 'dt')) \
            .groupBy('acct_no', 'member_id', 'cate', 'dt').count() \
            .groupBy('acct_no', 'member_id', 'cate').agg(collect_list(array('dt', 'count')).name('score'))

a = 0.15
f = udf(lambda x: float(np.sum(np.array(x)[:,1]/(1. + a*np.array(x)[:, 0]))), FloatType())
u_c = u_c.withColumn('score', f('score')) \
         .join(item_hot, 'cate') \
         .selectExpr('acct_no', 'member_id', "concat_ws('-', cate, item) c_i", "score*count score") \
         .withColumn('rank', row_number().over(Window.partitionBy('acct_no', 'member_id').orderBy(desc('score'))))

df = u_c.groupBy('acct_no', 'member_id') \
        .agg(collect_list('c_i').name('c_i'))

df = df.selectExpr('acct_no', 'member_id', 'c_i rec_list')

# 热门列表
item_hot_list = ins_in_sale
item_hot_list = spark.createDataFrame([['ins_pc', 'ins_pc', item_hot_list]])
df = df.union(item_hot_list)


# 存表
try:
    df.write.saveAsTable('fbidm.yuanjie_test', mode='overwrite')
except:
    print('----------\n %s \n----------' % 'Fail!!!')
else:
    print('----------\n %s \n----------' % 'Success!!!')
