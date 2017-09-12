# :rocket: 看了又看 :facepunch:
---
## IDE spark任务结构
- [Data.zip][1]
- [MySparkOnline.zip][2]
---
## IDE spark-submit
- spark-submit [options] <app jar | python file> [app options]
> spark-submit --help
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
--py-files MySparkOnline.zip,Data.zip "                          # 注意空格

os.system(cmd + '__main__.py')
```



---
[1]: https://github.com/Jie-Yuan/1_SomeProjects/tree/master/1_WorkExperience/1_recSys/LookAgain/Data.zip
[2]: https://github.com/Jie-Yuan/1_SomeProjects/tree/master/1_WorkExperience/1_recSys/LookAgain/MySparkOnline.zip

