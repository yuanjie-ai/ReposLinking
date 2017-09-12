# encoding=utf-8
__title__ = '__main__.py'
__author__ = 'JieYuan'
__mtime__ = '2017/9/9'

from MyPackage.SparkSession import *
from MyPackage.UDF import *

t = time.strftime("%Y%m%d",time.localtime(time.time()))
t_0, t_1, t_2, t_3, t_4, t_5, t_6, t_7 = [time.strftime("%Y-%m-%d", time.localtime(time.time() - i)) for i in 60*60*24*np.arange(8)]
t_3M = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*3))
t_6M = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*6))
t_1Y = time.strftime("%Y-%m-%d",time.localtime(time.time()-60*60*24*30*12))

# Dependent Data
if os.path.isfile("Data.zip"):
        os.system("unzip Data.zip")
else:
    print("NO Data.zip")

# ------------------------------------------------------------------------------
    
# Main
import jieba
import jieba.posseg
import jieba.analyse

important_col = ["catalog", "project_id", "project_name", "tag"]
df = spark.table('finance.sor_cfs_project') \
          .filter("status in ('10','11','12') and type not in ('02','03') and encore_status = '0'") \
          .filter(col('end_date') > t_0) \
          .filter(col('begin_date') < t_0) \
          .replace(['01', '02', '03', '04', '05', '06', '07'], ['1018', '1021', '1020', '1019', '1022', '1023', '1024'], 'catalog') \
          .select(important_col)

# toPandas
data = df.toPandas()
allowPOS = ['n', 'nr', 'nr1', 'nr2', 'nrj', 'nrf', 'ns', 'nsf', 'nt', 'nz', 'nl', 'ng']
data['cate_1'] = data['project_name'].apply(lambda x: jieba.analyse.extract_tags(x, topK=10, allowPOS=allowPOS))
data['cate_2'] = data['project_name'].apply(lambda x: [i.word for i in jieba.posseg.cut(x) if i.flag.startswith('n')])
data['tag'] = data['tag'].apply(lambda x: filter(lambda x: x != '', jieba.cut(x, cut_all=True)) if x is not None else [])
listUnique = lambda x: sorted(list(set(x)), key=x.index)
data['cate'] = data.apply(lambda x: listUnique( x[3] + x[4] + x[5]), axis=1)
data['cate'] = data.apply(lambda x: [x[0]] + x[6], axis=1)

# toDF
t1 = spark.createDataFrame(data[['catalog', 'project_id', 'cate']]) \
          .withColumn('item', concat_ws('-', 'catalog', 'project_id'))
          
t2 = t1.crossJoin(t1.selectExpr("item item_temp", "cate cate_temp"))
t2.cache()

# cate: 关键词
rank_score = lambda x: float((1./(np.where([i in x[1] for i in x[0]])[0] + 1)/(1./(np.arange(len(x[0]))+1)).sum()).sum())
rank_score = udf(rank_score, FloatType())
t3 = t2.withColumn('score', rank_score(array('cate', 'cate_temp'))) \
       .withColumn('r', expr("row_number() over(partition by cate order by score desc)")) \
       .select('item', 'item_temp', 'score', 'r')

# Top 8
top8 = t3[t3.r.between(2, 9)].cache()
df = top8.groupBy('item').agg(collect_list('item_temp').name('similar_item'))

# saveAsTable
df.write.saveAsTable('fbidm.yuanjie_chp_similar_item', mode = 'overwrite')
