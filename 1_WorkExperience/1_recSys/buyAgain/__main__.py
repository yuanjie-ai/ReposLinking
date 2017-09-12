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
          .withColumn('item_id', concat_ws('-', 'cate', 'item')) \
          .withColumn('business_type', when(col('cate').isin([str(i) for i in range(1018, 1025)]), 'chp') \
                                      .when(col('cate').isin([str(i) for i in range(1031, 1041)]), 'ins') \
                                      .otherwise('fin'))
                                      
df = df[df.business_type == 'chp'].cache()

# 在售商品
chp_in_sale = spark.table('finance.sor_cfs_project') \
                   .filter(" status not in ('00', '01', '02', '03', '91') and  type <> '02' and catalog <> '07'") \
                   .filter(col('end_date') > t_0) \
                   .filter(col('begin_date') < t_0) \
                   .replace(['01', '02', '03', '04', '05', '06'], ['1018', '1021', '1020', '1019', '1022', '1023'], 'catalog') \
                   .select(concat_ws('-', 'catalog', 'project_id').name('l'))
chp_in_sale_list = SparkCol.colToList(chp_in_sale)

# 最近1天热门商品列表(在售)
t_1_hot = df.filter(col('dt') > t_1)[col('item_id').isin(chp_in_sale_list)] \
            .groupBy('item_id').count() \
            .orderBy(desc('count')) \
            .select('item_id')
t_1_hot.cache()
t_1_hot = SparkCol.takeN(t_1_hot, 20)

# 关联规则
f = udf(lambda x: float(len(x)), FloatType())
df_fpm = df.groupBy('member_id').agg(collect_set(concat_ws('-', 'cate', 'item')).name('items')).filter(f('items')>1)
df_fpm.cache()
df_fpm = SparkFPM.getLastRules(df = df_fpm, support = 0.0005)
df_fpm = df_fpm[col('r').isin(chp_in_sale_list)].orderBy('l', desc('confidence')) \
                                                .dropDuplicates(['l']).select('r', 'l')

df = chp_in_sale.join(df_fpm, 'l', 'left_outer') \
                .select('l', 'r') \
                .union(spark.createDataFrame([['88888888', str(t_1_hot)]]))

df.write.saveAsTable('fbidm.yuanjie_chp_buy_again', mode='overwrite')
