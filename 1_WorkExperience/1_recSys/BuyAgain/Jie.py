# -*- coding: utf-8 -*-
import os
import sys
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
from pyspark.mllib.fpm import FPGrowth

# Init
spark = SparkSession.builder \
                    .appName("Yuanjie_Test") \
                    .config('log4j.rootCategory',"WARN") \
                    .enableHiveSupport() \
                    .getOrCreate()
spark.conf.set("spark.executor.memory", '16g')
spark.conf.set('spark.executor.cores', '50')
spark.conf.set('spark.cores.max', '5')

sc = spark.sparkContext

# 动态分区
spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")

# List(spark array)
class SparkArray(object):
    def __init__(self):
        pass

    @staticmethod
    def listUnique(x):
        listUnique = lambda x: sorted(list(set(x)), key=x.index)
        return listUnique(x)

    @staticmethod
    def listSub(a, b):
        listSub = lambda a, b: [a[i] for i in np.where([i not in b for i in a])[0]]
        return listSub(a, b)

    @staticmethod
    def listExplode(df, colname='name'):
        n = len(df.select(colname).first()[0])
        names = df.columns + [col(colname)[i].name(colname + str(i)) for i in range(n)]
        return df.select(names)

    @staticmethod
    def litArray(ls=[]):
        return (array([lit(i) for i in ls]))

# colToList
class SparkCol(object):
    def __init__(self):
        pass

    @staticmethod
    def takeN(df, n=5):
        return np.array(df.take(n)).flatten().tolist()

    @classmethod
    def colToList(cls, df):
        return cls.takeN(df.select(collect_list(df.columns[0])), 1)


# DataFrame
class SparkDF(object):
    def __init__(self):
        pass   
    
    @staticmethod
    def createEmptyDF(floatcol=[], stringcol=[]):
        ls = zip(floatcol, [FloatType()] * len(floatcol)) + zip(stringcol, [StringType()] * len(stringcol))
        schema = StructType([StructField(*i) for i in ls])
        df = spark.createDataFrame([], schema=schema)
        return df



class SparkFPM(object):
    def __init__(self, df, support=0.0001, confidence=0.1):
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
    def getLastRules(cls, df, support=0.0001):
        instance = cls(df, support)
        df1 = instance.getConfident()
        df2 = df1.toDF('r', 'rr', 'confidenceNegative')
        df = df1.join(df2, 'r', 'left_outer').select('l', 'r', 'rr', 'confidencePositive', 'confidenceNegative').filter("l = rr")
        df = df.selectExpr('l', 'r', 'confidencePositive confidence', "confidencePositive/confidenceNegative ir", "0.5*(confidencePositive+confidenceNegative) kulc")
        return df   
    











SPARK = \
'''
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\ 
      /_/

'''    
if __name__ == '__main__':
    print('It is main!!!')
else:
    print(SPARK)
