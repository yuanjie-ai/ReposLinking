```
# -*- coding: utf-8 -*-

import re
import time
import itertools
import numpy as np
import pandas as pd
from scipy import stats
import pyspark.sql.functions as F

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import *
from pyspark.ml.classification import *
from pyspark.ml.evaluation import *
from pyspark.ml.tuning import *
# from pyspark.ml.recommendation import *
from pyspark.mllib.recommendation import *
from pyspark.ml.linalg import Vectors,VectorUDT
# f = udf(lambda x:Vectors.dense([i[1] for i in x]),VectorUDT())
# f = udf(lambda x:[i[1] for i in x],ArrayType(StringType()))
#http://stackoverflow.com/questions/42216891/pyspark-use-one-column-to-index-another-udf-of-two-columns

spark = SparkSession.builder \
    .appName("Yuanjie_Test") \
    .config('log4j.rootCategory',"WARN") \
    .enableHiveSupport() \
    .getOrCreate()
spark.conf.set("spark.executor.memory", '16g')
spark.conf.set('spark.executor.cores', '50')
spark.conf.set('spark.cores.max', '5')

sc = spark.sparkContext

# a-b
# f = udf(lambda x:[x[0][i] for i in np.where([i not in x[1] for i in x[0]])[0]],ArrayType(StringType()))
# f = udf(lambda x: diffList(x[0], x[1]),ArrayType(StringType()))
#which:np.where([True,False,True])[0]
def diffList(a=[], b=[]):
    ls = [a[i] for i in np.where([i not in b for i in a])[0]]
    return(ls)
# df1.id不在df2.id 子查询：【在与不在】[df1.select('id').subtract(df2.select('id')).]join(df1,'id')

# F.collect_list
# pandas: 
# p = lambda x:list(x)
# gr.agg(dict(字段名=p)) 

# F.litList：df.select(array([lit(i) for i in range(5)]).name('a')).show()
def litArray(ls=[]):
    return(array([lit(i) for i in ls]))

# ALS推荐列表
def resALS(df,params,topk=5,implicit=True):
    '''
    params = dict(rank=10,
              iterations=20,
              lambda_=0.01,
              alpha=0.01,
              blocks=-1,
              nonnegative=False,
              seed=0)
    '''
    colname = ['user','product','rating']
    df = df.toDF(*colname)
    '''
    encoding
    '''
    df = stringIndexed(df,stringcol=['user','product'])
    indexed_user = df.select('user','indexed_user').distinct()
    indexed_item = df.select('product','indexed_product').distinct()
    traindata = df.select('indexed_user','indexed_product','rating')
    traindata.cache()

    if implicit:
        model = ALS.trainImplicit(traindata,**params)
    else:
        params.pop('alpha')
        model = ALS.train(traindata,**params)

    toList = udf(lambda x:[i[1] for i in x],ArrayType(StringType()))
    rec = model.recommendProductsForUsers(topk).toDF(['indexed_user','indexed_product'])
    rec = rec.withColumn('indexed_product',explode(toList('indexed_product'))) \
             .withColumn('indexed_product',col('indexed_product').astype(LongType()))
    res = rec.join(broadcast(indexed_user),'indexed_user') \
             .join(broadcast(indexed_item),'indexed_product') \
             .select('user','product') \
             .groupby('user') \
             .agg(collect_list('product').name('product'))
    return(res)

# 字符串列数值化
def stringIndexed(df,stringcol=[] ):
    for i in stringcol:
        df = StringIndexer(inputCol=i,outputCol='indexed_'+i).fit(df).transform(df).withColumn('indexed_'+i,col('indexed_'+i).astype(LongType()))
    return(df)



# 列裂
def creatCols(df,colname='name',strType=True):
    n = len(df.select(colname).first()[0])
    '''
    VectorUDT()
    Array()
    '''
    if strType:
        for i in range(n):
            f = udf(lambda x:x[i])
            df = df.withColumn(colname+str(i),f(colname))
    else:
        for i in range(n):
            f = udf(lambda x:float(x[i]),FloatType())
            df = df.withColumn(colname+str(i),f(colname))        
    return(df)

# emptyDF：初始化数据类型
def emptyDF(Stringcol=[],Floatcol=[]):
    ls = zip(Floatcol, [FloatType()]*len(Floatcol)) + zip(Stringcol, [StringType()]*len(Stringcol))
    schema = StructType([StructField(*i) for i in ls])
    df = spark.createDataFrame([], schema)
    return df

# freqItem
def freqItem(df,user_id=[],product_catalog=[]):
    '''
    推荐规则：商品目录主次顺序取众数
    '''
    p = product_catalog
    for i in range(len(p)):
        df = df.join(df.groupBy(user_id+p[:i]).agg(modeString(collect_list(p[i])).name(p[i])),user_id+p[:(i+1)])
    df = df.select(user_id+product_catalog)
    return(df.distinct())

# head
def head(df,n=5):
    sql = 'select * from df limit ' + str(n)
    df.registerTempTable('tab')
    df = spark.sql(sql)
    spark.catalog.dropTempView('tab')
    return(df)

#时间序列：pd.date_range(start='2016-06-01',end='2017-01-01',freq='D').map(lambda x:str(x)[:10])
# slidingWindow：尽量转换成时间戳
# 观察变量时间窗口宽度：observation_window = [start_time,start_time + 86400*m]
# 响应变量时间窗口宽度：response_window = [observation_window[1],observation_window[1] + 86400*n]
def slidingWindow(df,_id='id',time_col='time',label='label',observation_window=[],response_window=[]):
    observation = df.filter(col(time_col) >= observation_window[0]).filter(col(time_col) < observation_window[1]).drop(label)
    response = df.filter(col(time_col) >= response_window[0]).filter(col(time_col) < response_window[1]).select(_id,label)
    df = observation.join(response,_id).distinct()
    return(df)

# n_na
def n_na(df,isNull = True,axis = 0):
    '''
    axis:0按行统计,每列缺失值数
    只有axis=0才有null与nan
    '''
    if axis == 0:
        SUM = udf(lambda x:float(np.sum(x)),FloatType())
        if isNull:
            df = df.agg(*[SUM(collect_list(isnull(i))).name(i) for i in df.columns])
            return df
        else:
            df = df.agg(*[SUM(collect_list(isnan(i))).name(i) for i in df.columns])
            return df
    else:
        SUM = udf(lambda x:float(np.sum([i is None for i in x])),FloatType())
        if isNull:
            df = df.withColumn('n_null',SUM(array(df.columns)))
            return df
        else:
            df = df.withColumn('n_nan',SUM(array(df.columns)))
            return df
        
# mode
modeNumber = udf(lambda x:float(stats.mode(x)[0]),FloatType())
modeString = udf(lambda x:str(stats.mode(x)[0][0]),StringType())

# median
median = udf(lambda x:float(np.median(x)),FloatType())

# 组内众数（sql方式）
def spark_mode(df,colnames):
    gr_count = pp.groupBy(colnames).count()
    mode = gr_count.join(gr_count.groupBy(colnames[0]).agg(F.max(F.col('count')).alias('count')),on=[colnames[0],'count'])#.drop('count')
    mode.show(5)
    return mode

# Shape
def dim(df):
    shape = (df.count(),len(df.columns))
    print shape
    return(shape)

# 加索引
def addIndex(df,gr = lit(0)):
    w = F.row_number().over(Window.partitionBy(gr).orderBy(F.lit(0)))
    return(df.withColumn('_index', w))

# Cbind
def cbind(df1,df2,how='inner',_index=False):
    """
    One of `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`
    """
    if _index:
        df = addIndex(df1).join(addIndex(df2),on='_index',how=how)
    else:
        df = addIndex(df1).join(addIndex(df2),on='_index',how=how).drop('_index')
    return(df)

# metric
def confusionMatrix(pred):
    return(pred.crosstab('label','prediction').sort(F.col('label_prediction')))
    
# 特征名
def featureDate(df):
    feature_date = list((set(df.columns) & set(spark.table('fbidm.yuanjie_feature_date').select(F.collect_list('col_name')).first()[0])))
    return feature_date
def featureString(df):
    feature_string = list(set(df.columns) & set(spark.table('fbidm.yuanjie_feature_string').select(F.collect_list('col_name')).first()[0]))
    return feature_string
def featureNames(df,feature_string=[],feature_number=[],other=['acct_no','label']):
    if feature_string:
        feature_number = list(set(df.columns)- set(feature_string+other))
        return([feature_string,feature_number])
    if feature_number:
        feature_string = list(set(df.columns)- set(feature_number+other))
        return([feature_string,feature_number])

# 缺失值
def imputer(df,feature_string=[],feature_number=[]):
    for i in [i[0] for i in df.select(feature_string).dtypes if i[1] !='string']:
        df = df.withColumn(i,df[i].astype(LongType()))
    for i in feature_string:
        df = df.withColumn(i,df[i].astype(StringType()))  
    for i in feature_number:
        df = df.withColumn(i,df[i].astype(FloatType()))
    df = df.fillna(dict(zip(feature_string,['88888888']*len(feature_string)) + zip(feature_number,[0]*len(feature_number))))
    return(df)

# OneHot
## label为数值型，id为字符型
def preprocessing(df,_id='acct_no',feature_string=[],feature_number=[],model_Data='fbidm.df',isTrainData=True):
    indexed_Data = model_Data+'_indexed_Data'
    scaled_Data = model_Data+'_scaled_Data'
    df = df.withColumnRenamed(_id,'_id').withColumn('_id',col('_id').astype(StringType()))
    if 'label' in df.columns:
        df = df.withColumn('label',col('label').astype(IntegerType()))
    for i in [i[0] for i in df.select(feature_string).dtypes if i[1] !='string']:
        df = df.withColumn(i,df[i].astype(LongType()))    
    for i in feature_number:
        df = df.withColumn(i,col(i).astype(FloatType()))
    for i in feature_string:
        df = df.withColumn(i,col(i).astype(StringType()))
    print "Sucessful Data Astype:===================>10%"
    
    df = df.fillna(dict(zip(feature_string,['88888888']*len(feature_string)) + zip(feature_number,[0]*len(feature_number))))
    print "Sucessful Data Imputer:==================>20%"
    
    if feature_string:
        if isTrainData:
            DataFrameWriter(df.select(feature_string)).saveAsTable(indexed_Data,mode='overwrite')
            indexed_Data = spark.table(indexed_Data)
        else:
            indexed_Data = spark.table(indexed_Data)
            """
            在新数据集df上每类加上类别值，避免新数据各类别值变少
            """
            nrow = np.max([len(indexed_Data.select(i).rdd.countByKey()) for i in feature_string])
#           nrow = np.max([indexed_Data.select(i).drop_duplicates().count() for i in feature_string])
            df0 = spark.range(1,nrow)
            for i in feature_string:
                df0 = cbind(df0,indexed_Data.select(i).distinct(),how='outer')
            df0 = df0.drop('id')
            df0 = df0.fillna(dict(zip(feature_string,list(df0.first()))))
            for i in df.drop(*(feature_string)).columns:
                df0 = df0.withColumn(i,lit(0))
            df0 = df0.withColumn('_id',lit('new_id')).select(df.columns)
            df = df.union(df0)
            print "Sucessful Data kinds:====================>30%"

    print "Sucessful Data Indexed:==================>40%"

    for i in feature_string:
        inputCol=i
        outputCol='indexed_'+i
        indexer = StringIndexer(inputCol=inputCol,outputCol=outputCol,handleInvalid='skip').fit(indexed_Data)###model
        df = indexer.transform(df).withColumn(i,col(outputCol)).drop(outputCol)
    print "Sucessful Data Indexing:=================>50%" 

    for i in feature_string:
        inputCol=i
        outputCol='encoded_'+i
        encoder =  OneHotEncoder(dropLast=False,inputCol=inputCol,outputCol=outputCol)
        df = encoder.transform(df).withColumn(i,col(outputCol)).drop(outputCol)
    print "Sucessful Data OneHot:===================>60%"

    vecAssembler = VectorAssembler(inputCols=feature_string+feature_number,outputCol='features')
    df = vecAssembler.transform(df)
    print "Sucessful Data VectorAssembler:==========>70%"

    if 'label' in df.columns:
        df = df.select('_id','label','features')
    else:
        df = df.select('_id','features')
    if isTrainData:
        DataFrameWriter(df).saveAsTable(scaled_Data,mode='overwrite')
        scaled_Data = spark.table(scaled_Data)
    else:
        df = df.filter(col('_id') != 'new_id')
        scaled_Data = spark.table(scaled_Data)
    print "Sucessful Data Scaled:===================>80%"

    standardScaler = StandardScaler(withMean=True, withStd=True,inputCol='features', outputCol='scaled').fit(scaled_Data)###model
    df = standardScaler.transform(df).withColumn('features',col('scaled')).drop('scaled')
    print "Sucessful Data Scaling:==================>99%"
    return(df)


if __name__ == '__main__':
    print('I IS MAIN!!!!!')
else:
    print('Sucessful Import Me!!!!!')
```