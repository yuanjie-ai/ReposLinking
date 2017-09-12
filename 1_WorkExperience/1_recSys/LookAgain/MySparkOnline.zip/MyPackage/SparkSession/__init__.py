# coding: utf-8
__title__ = '__init__'
__author__ = 'JieYuan'
__mtime__ = '2017/9/9'
import os
import sys
import re
import time
import itertools
import zipfile
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

# Init
spark = SparkSession.builder \
                    .appName("Yuanjie") \
                    .config('log4j.rootCategory',"WARN") \
                    .enableHiveSupport() \
                    .getOrCreate()
sc = spark.sparkContext
spark.conf.set("spark.executor.memory", '16g')
spark.conf.set('spark.executor.cores', '50')
spark.conf.set('spark.cores.max', '5')

# 动态分区
# spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
















SPARK = \
'''
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\    >>>https://github.com/Jie-Yuan
      /_/

'''
if __name__ == '__main__':
    print('It is main!!!')
else:
    print(SPARK)
    print("Successfully import MyPackage.SparkSession !!!")
