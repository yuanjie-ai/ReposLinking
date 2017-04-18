from me import *


#-----------------------------------
df = spark.table('fbidm.yuanjie_cfs_train')
df = df.withColumn('label',col('label').astype(IntegerType()))
#-----------------------------------

feature_string = ['is_inr_emp','vst_staytm_lvl','epp_byscan_actv_lvl','is_mbl_fill','mbl_fill_lvl','is_ele_chrg','is_gas_chrg',
                 'income','mem_sts','value_lvl','vst_pg_lvl','vst_cnt_lvl','epp_lifecycle']

feature_string,feature_number = Feature_names(df,feature_string=feature_string)
traindata = Preprocessing(df,_id='acct_no',feature_string=feature_string,feature_number=feature_number)
traindata.show(5)
#----------------------------------
p1 = traindata.filter('label<0')
p2 = traindata.filter('label<0')
for i,j in [(1,1),(4,10),(5,10)]:
    p3 = traindata.filter(col('label')==i)
    for i in range(j):
        p1 = p3.union(p1)
p2 = p1.union(p2)
traindata = p2.union(traindata)
traindata.cache()
#-----------------------------------

params = dict(featuresCol="features",
              labelCol="label", 
              predictionCol="prediction",
              probabilityCol="probability", 
              rawPredictionCol="rawPrediction", 
              maxDepth=8,
              numTrees=100,
              maxBins=10,
              impurity="gini",
              maxMemoryInMB=2560, 
              cacheNodeIds=True,
              seed=0)
rf = RandomForestClassifier(**params).fit(traindata)

pred = rf.transform(traindata)
pred.crosstab('label','prediction').sort('label_prediction').show()
#----------------------------------
rf.write().overwrite().save('/user/fbidm/modelresult/recmodel/cfs_rdf_001/yuanjie_cfs_01.model') 
spark.stop()
#----------------------------------
