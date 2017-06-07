# rec_all
```
# coding=utf-8
from me import *
t_0 = time.strftime("%Y-%m-%d",time.localtime(time.time()))
t_1 = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*1))
t_2 = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*2))
t_3 = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*3))
t_3M = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*3))


# 基础信息
#------------------------------------------------------------------
old_name = ['acct_no', 'l2_prod_group_cd', 'l3_prod_group_cd', 'entry_time']
new_name = ['user_id', 'cate', 'item_id1', 'item_id2', 'dt']
fin = tuple([str(i) for i in [1001, 1002, 1004, 1005]])
chp = tuple([str(i) for i in range(1018, 1025)])
ins = tuple([str(i) for i in range(1031, 1041)])
fin_cond = "l2_prod_group_cd in {}".format(fin)
chp_cond = "l2_prod_group_cd in {}".format(chp)
ins_cond = "l2_prod_group_cd in {}".format(ins)

df = spark.table('fdm_dpa.mls_recommender_base_info_detail')
df = df.select(old_name) \
       .dropna() \
       .filter(" l3_prod_group_cd not in ('', '1', '2') ") \
       .filter(df.stat_date >= t_3M)													##########
df.cache()
df1 = df.filter(fin_cond).withColumn('cate', lit('fin')).select(['acct_no', 'cate', 'l2_prod_group_cd', 'l3_prod_group_cd', 'entry_time'])
df2 = df.filter(chp_cond).withColumn('cate', lit('chp')).select(['acct_no', 'cate', 'l2_prod_group_cd', 'l3_prod_group_cd', 'entry_time'])
df3 = df.filter(ins_cond).withColumn('cate', lit('ins')).select(['acct_no', 'cate', 'l2_prod_group_cd', 'l3_prod_group_cd', 'entry_time'])


# 订单表
#------------------------------------------------------------------
data = spark.table('finance.sor_bil_bill_order').filter('pay_time is not null')
fin_1001 = data.select('account_no',
					 lit('fin'),
                     lit('1001'), 
					 'bill_product_id',
                     'pay_time').dropna()
					 
data = spark.table('fdm_sor.sor_txn_fnd_p2p_order').filter('pay_time is not null')
fin_1002 = data.select('account_no',
					 lit('fin'),
                     lit('1002'), 
                     'product_id',
                     'pay_time').dropna()

fin_1004 = spark.table('fdm_sor.sor_txn_fnd_prch_order')
fin_1004 = fin_1004.filter("fund_cat = '2' and sys_st = '14' and pay_time is not null") \
                   .select('acct_no', col('fund_id').name('id'), 'pay_time')
				 
fin_1004_p = spark.table('fdm_sor.sor_prd_fnd_product')
fin_1004_p = fin_1004_p.filter("fund_cat = '2' and type in (2, 3, 5)").select('id')

fin_1004 = fin_1004.join(broadcast(fin_1004_p), 'id').dropna() \
				   .select('acct_no', lit('fin'), lit('1004'), 'id', 'pay_time')

# ins
to_replace = ['R8004001','R8004002','R8004003','R8004004','R8004005','R8004006','R8004007','R8004008','R8004009','R8004010']
value = [str(i) for i in range(1031,1041)]


ins_product = spark.table('fdm_sor.sor_prd_ins_product').filter("channel_code = 'SC1001'").select('product_code', 'cate_code') \
														.replace(to_replace,value, subset='cate_code') \
														.distinct()
											  
ins_1031 = spark.table('fdm_sor.SOR_TXN_INS_ORDER_MAIN_ORDER').select('account_no', 'product_code', 'issued_time') \
															  .filter("issued_time is not null")
ins_1031 = ins_1031.join(broadcast(ins_product),'product_code') \
				   .toDF( 'l3_prod_group_cd', 'userid', 'time', 'l2_prod_group_cd') \
				   .select('userid', lit('ins'), 'l2_prod_group_cd', 'l3_prod_group_cd', 'time') \
				   .dropna()			   
#------------------------------------------------------------------
# 大表
df = df1.union(df2).union(df3).union(fin_1001).union(fin_1002).union(fin_1004).union(ins_1031).dropna().toDF(*new_name).withColumn('stat_date', lit(t_0))
df.write.saveAsTable('fbidm.yuanjie_rec_all', mode = 'overwrite', partitionBy = 'stat_date')
#------------------------------------------------------------------
```









