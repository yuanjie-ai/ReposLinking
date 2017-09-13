# -*- coding: utf-8 -*-
from Jie import *
t = time.strftime("%Y%m%d",time.localtime(time.time()))
t_0, t_1, t_2, t_3, t_4, t_5, t_6, t_7 = [time.strftime("%Y-%m-%d", time.localtime(time.time() - i)) for i in 60*60*24*np.arange(8)]

t_3M = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*3))
t_6M = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*6))
t_1Y = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*12))

# df_look
df_look = spark.table('fbidm.yuanjie_chp_look_again')

# df_buy
df_buy = spark.table('fbidm.yuanjie_chp_buy_again') \
              .selectExpr("l item", "r") \
              .cache()
item_hot_list = eval(SparkCol.takeN(df_buy[col('item')=='88888888'].select('r'), 1)[0])[:5]

df_buy = df_look.join(df_buy, 'item', 'left_outer')
df_buy = df_buy.withColumn('r', when(col('r').isNull(), col('similar_item')[0]).otherwise(col('r'))) \
               .selectExpr('item', 'r similar_item') \
               .withColumn('item_hot', SparkArray.litArray(item_hot_list)) \
               .cache()

# udf尽量分层，避免高阶函数
f1 = udf(lambda x: [x[0]] + x[1], ArrayType(StringType()))
f2 = udf(SparkArray.listUnique, ArrayType(StringType()))
df_buy = df_buy.withColumn('similar_item', f1(struct('similar_item', 'item_hot'))) \
               .withColumn('similar_item', f2('similar_item')) \
               .select('item', 'similar_item')

# saveAsTable
df_look = df_look.withColumn('p', split(concat_ws('-', 'similar_item'), '-'))
df_look_002 = df_look.selectExpr("split(item, '-')[1] acct_no", 
                                 "split(item, '-')[1] member_id", 
                                 "'CHP_FPM_002' rec_id",
                                 "concat_ws('|', p[0], p[2], p[4], p[6]) product_type", 
                                 "concat_ws('|', p[1], p[3], p[5], p[7]) product_id", 
                                 "1 ab_flag", 
                                 "'%s' stat_date" %t)
df_look_003 = df_look_002.withColumn('rec_id', lit('CHP_FPM_003'))

df_buy = df_buy.withColumn('p', split(concat_ws('-', 'similar_item'), '-'))
df_buy_004 = df_buy.selectExpr("split(item, '-')[1] acct_no", 
                                 "split(item, '-')[1] member_id", 
                                 "'CHP_FPM_004' rec_id",
                                 "concat_ws('|', p[0], p[2], p[4], p[6]) product_type", 
                                 "concat_ws('|', p[1], p[3], p[5], p[7]) product_id", 
                                 "1 ab_flag", 
                                 "'%s' stat_date" %t)
df_buy_005 = df_buy_004.withColumn('rec_id', lit('CHP_FPM_005'))

df = df_look_002.union(df_look_003).union(df_buy_004).union(df_buy_005)
spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
df.write.insertInto('fbidm.tdm_firs_chp_rules', True)                            
#df_look.write.saveAsTable('', mode = 'overwrite', partitionBy = 'stat_date')
