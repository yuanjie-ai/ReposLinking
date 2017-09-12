# coding: utf-8
__title__ = '__init__.py'
__author__ = 'JieYuan'
__mtime__ = '2017/9/9'

"""
基础模块都在这
"""
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
        ls = zip(Floatcol, [FloatType()] * len(floatcol)) + zip(Stringcol, [StringType()] * len(stringcol))
        schema = StructType([StructField(*i) for i in ls])
        df = spark.createDataFrame([], schema=schema)
        return df

print("Successfully import MyPackage.UDF !!!")
