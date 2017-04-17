from me import *

#-----------------------------------
df = spark.sql('select * from fbidm.cfs_recommender_feature')
#-----------------------------------

feature_string = ['is_inr_emp','vst_staytm_lvl','epp_byscan_actv_lvl','is_mbl_fill','mbl_fill_lvl','is_ele_chrg','is_gas_chrg',
                 'income','mem_sts','value_lvl','vst_pg_lvl','vst_cnt_lvl','epp_lifecycle']

		 
feature_string,feature_number = Feature_names(df,feature_string=feature_string)
df = Preprocessing(df,feature_string=feature_string,feature_number=feature_number,isTrainData=False,indexed_Data="fbidm.test_indexed_Data",scaled_Data="fbidm.test_scaled_Data")
			 
df.show(5)

rf = RandomForestClassificationModel.load('/user/fbidm/modelresult/recmodel/cfs_rdf_001/yuanjie_cfs_01.model')
pred = rf.transform(df).select('_id','probability')


#----------------------------------------------------------
r = pred.rdd
r.cache()
pred = spark.createDataFrame(r.map(lambda x:(x[0],zip(list(x[1]),[1025,1026,1027,1028,1029]))) \
							  .map(lambda x:(x[0],[i[1] for i in sorted(x[1],reverse=True)])) \
							  .map(lambda x:[x[0]]+x[1]),['acct_no','product_type1','product_type2','product_type3','product_type4','product_type5']) 


pred = pred.select(lit(time.strftime('%Y%m%d',time.localtime(time.time()-86400))).name('stat_date'),
'acct_no',\
lit('CFS_RDF_001').alias('rec_id'),\
'product_type1',col('product_type1').name('product_id1'),
'product_type2',col('product_type2').name('product_id2'),
'product_type3',col('product_type3').name('product_id3'),
'product_type4',col('product_type4').name('product_id4'),
'product_type5',col('product_type5').name('product_id5'),
lit(1).name('ab_flag'))
#----------------------------------------------------------

#----------------------------------------------------------
DataFrameWriter(pred).saveAsTable("fbidm.TDM_FIRS_CFS_RDF_001_d",mode='overwrite')
spark.stop()
#----------------------------------------------------------
