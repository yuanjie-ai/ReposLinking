

http://blog.csdn.net/wwwxxdddx/article/details/51261835
## sspark-submit [options] <app jar | python file> [app options]
- --py-files: 支持 .zip/.py
- 常用模式
```
# -*- coding: utf-8 -*-
import os
cmd = \
"source change_spark_version spark-2.1.0 && /home/bigdata/software/spark-2.1.0.7-bin-2.4.0.10/bin/spark-submit \
--master yarn-cluster \
--num-executors 40 \
--executor-memory 10G \
--executor-cores 2 \
--driver-memory 3G \
--py-files Test.zip,idf.txt \
__main__.py"                         # 注意空格
#os.system(cmd + 'idf.py')
os.system(cmd + 'PyMain.py')
```
