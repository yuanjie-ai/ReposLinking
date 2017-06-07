# coding=utf-8
from me import *
t = time.strftime("%Y%m%d",time.localtime(time.time()))
t_0 = time.strftime("%Y-%m-%d",time.localtime(time.time()))
t_1 = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*1))
t_2 = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*2))
t_3 = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*3))
t_3M = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*3))

#------------------------------------------------------------------

df = spark.table('fbidm.yuanjie_rec_all')
df.cache()

#------------------------------------------------------------------
# n_3：1中组合 规则：最近或者众数
n_3 = df.groupBy('user_id').agg(countDistinct('cate').name('n')).filter('n=3').select('user_id')
n_3 = df.join(broadcast(n_3), 'user_id')

n_3 = n_3.groupBy('user_id', 'cate').agg(modeString(collect_list(concat_ws('-', 'item_id1', 'item_id2'))).name('item_id'))
n_3 = n_3.groupBy('user_id').agg(collect_list('item_id').name('product_list'))

#------------------------------------------------------------------
# n_2：3种组合 已经两种补全补理财（记录只有两条待考虑）
n_2 = df.groupBy('user_id').agg(countDistinct('cate').name('n')).filter('n=2').select('user_id')
n_2 = df.join(broadcast(n_2), 'user_id')
n_2.cache()
# n_2_last
n_2_last = n_2.orderBy('user_id', 'cate', 'item_id1', 'item_id2', desc('dt')).dropDuplicates(['user_id','cate'])
n_2_last = n_2_last.groupBy('user_id').agg(collect_list(concat_ws('-', 'item_id1', 'item_id2')).name('product_list_last'))
# n_2_mode
n_2_mode = n_2.groupBy('user_id', 'cate').agg(modeString(collect_list(concat_ws('-', 'item_id1', 'item_id2'))).name('item_id'))
n_2_mode = n_2_mode.groupBy('user_id').agg(collect_list('item_id').name('product_list_mode'))
n_2 = n_2_last.join(n_2_mode, 'user_id')

addList = udf(lambda x: (x[0] + x[1])[:3], ArrayType(StringType()))
n_2 = n_2.withColumn('product_list', addList(array('product_list_last', 'product_list_mode'))).select('user_id', 'product_list')

#------------------------------------------------------------------
# n_1：3种组合 理财+1
n_1 = df.groupBy('user_id').agg(countDistinct('cate').name('n')).filter('n=1').select('user_id')
n_1 = df.join(broadcast(n_1), 'user_id')
n_1.cache()

n_1_fin = n_1.filter("cate = 'fin'")
n_1_chp = n_1.filter("cate = 'chp'")
n_1_ins = n_1.filter("cate = 'ins'")
n_1_fin.cache()
n_1_chp.cache()
n_1_ins.cache()

# 近三天
fin_hot2 = [list(i)[0] for i in n_1_fin.filter(col('dt') > t_3).select(concat_ws('-', 'item_id1', 'item_id2').name('item_id')).groupBy('item_id').count().orderBy(desc('count')).head(2)]
chp_hot1 = [list(i)[0] for i in n_1_chp.filter(col('dt') > t_3).select(concat_ws('-', 'item_id1', 'item_id2').name('item_id')).groupBy('item_id').count().orderBy(desc('count')).head(1)]
ins_hot1 = [list(i)[0] for i in n_1_ins.filter(col('dt') > t_3).select(concat_ws('-', 'item_id1', 'item_id2').name('item_id')).groupBy('item_id').count().orderBy(desc('count')).head(1)]
# n_1_fin
n_1_fin_last = n_1_fin.orderBy('user_id', 'cate', 'item_id1', 'item_id2', desc('dt')).dropDuplicates(['user_id','cate'])
n_1_fin_last = n_1_fin_last.groupBy('user_id').agg(collect_list(concat_ws('-', 'item_id1', 'item_id2')).name('product_list'))
addList = udf(lambda x: list(set(x[0] + x[1] + x[2]))[:3], ArrayType(StringType()))
n_1_fin = n_1_fin_last.withColumn('product_list', addList(array('product_list', litArray(fin_hot2), litArray(chp_hot1))))
# n_1_chp
n_1_chp_last = n_1_chp.orderBy('user_id', 'cate', 'item_id1', 'item_id2', desc('dt')).dropDuplicates(['user_id','cate'])
n_1_chp_last = n_1_chp_last.groupBy('user_id').agg(collect_list(concat_ws('-', 'item_id1', 'item_id2')).name('product_list'))
addList = udf(lambda x: list(set(x[0] + x[1])), ArrayType(StringType()))
n_1_chp = n_1_chp_last.withColumn('product_list', addList(array('product_list', litArray(fin_hot2))))
# n_1_ins
n_1_ins_last = n_1_ins.orderBy('user_id', 'cate', 'item_id1', 'item_id2', desc('dt')).dropDuplicates(['user_id','cate'])
n_1_ins_last = n_1_ins_last.groupBy('user_id').agg(collect_list(concat_ws('-', 'item_id1', 'item_id2')).name('product_list'))
addList = udf(lambda x: list(set(x[0] + x[1])), ArrayType(StringType()))
n_1_ins = n_1_ins_last.withColumn('product_list', addList(array('product_list', litArray(fin_hot2))))

n_1 = n_1_fin.union(n_1_chp).union(n_1_ins)

df = n_1.union(n_2).union(n_3)
addList = udf(lambda x: (x[0] + x[1]), ArrayType(StringType()))
df = df.withColumn('product_list', addList(array('product_list', litArray(fin_hot2))))
df.cache()
#------------------------------------------------------------------
# res
f = udf(lambda x:'-'.join(x[:5]))
df = df.withColumn('itemid',split(f('product_list'),'-'))

df = creatCols(df,'itemid')

colnames = ['stat_date',
            'acct_no',
            'rec_id',
            'product_type1',
            'product_id1',
            'product_type2',
            'product_id2',
            'product_type3',
            'product_id3',
            'product_type4',
            'product_id4',
            'product_type5',
            'product_id5',
            'ab_flag']

df = df.select(lit(t),
               'user_id',
               lit('FNC_ALS_001'),
               'itemid0','itemid1','itemid2','itemid3','itemid4',
               'itemid5','itemid6','itemid7','itemid8','itemid9',
               lit(1)).toDF(*colnames)


DataFrameWriter(df).saveAsTable("fbidm.tdm_firs_fnc_als_001_d", mode='overwrite', partitionBy='stat_date')
#------------------------------------------------------------------