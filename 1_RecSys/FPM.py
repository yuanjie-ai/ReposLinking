# coding = utf-8
__title__ = 'FPM'
__author__ = 'JieYuan'
__mtime__ = '2017/8/16'
from me import *
from  pyspark.mllib.fpm import FPGrowth

class FPM(object):
    def __init__(self, df, support=0.001, confidence=0.1):
        self.df = df.select(collect_list('items').name('items'))
        self.support = support
        self.confidence = confidence

    def getConfident(self):
        f = udf(lambda x: float(len(x)), FloatType())
        rdd = self.df.rdd.flatMap(lambda x: x[0])
        model = FPGrowth.train(rdd, self.support, 2)
        rules = model._java_model.generateAssociationRules(self.confidence).collect()
        ls = [[i.javaAntecedent()[0],
               i.javaConsequent()[0],
               i.confidence()] for i in rules if len(i.javaAntecedent()) == 1]
        return spark.createDataFrame(ls, ['l', 'r', 'confidencePositive'])

    @classmethod
    def getLastRules(cls, df):
        instance = cls(df)
        df1 = instance.getConfident()
        df2 = df1.toDF('r', 'rr', 'confidenceNegative')
        df = df1.join(df2, 'r', 'left_outer').select('l', 'r', 'rr', 'confidencePositive', 'confidenceNegative').filter("l = rr")
        df = df.selectExpr('l', 'r', 'confidencePositive confidence', "confidencePositive/confidenceNegative ir", "0.5*(confidencePositive+confidenceNegative) kulc")
        return df

#-----------------------------------------------------------------------------
t_3M = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*3))
df = spark.table("fbidm.yuanjie_rec_all") \
          .filter("cate = 'chp'") \
          .filter(col('dt') > t_3M)
df = df.select('user_id', concat_ws('-', 'item_id1', 'item_id2').name('items'))
df.cache()
df = df.groupBy('user_id').agg(collect_set('items').name('items'))
# 多项
f = udf(lambda x: float(len(x)), FloatType())
df = df.filter(f('items')>1)

df = FPM.getLastRules(df = df)
df.write.saveAsTable('fbidm.yuanjie_last', mode='overwrite') 
