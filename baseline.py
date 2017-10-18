#coding:utf-8
# 引入需要的包
import pandas as pd
from sklearn.metrics import f1_score,auc
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
# 假设抽取的样本同分布 则 test sets 中含有 19493 正样本
import numpy as np

# 根目录
dir = '../public/'

print('reading train datas')
train = pd.read_csv(dir + 'train.csv')
# 训练集中标签为1的EID
pos_train = list(train[train['TARGET']==1]['EID'].unique())
print(train.head())
print('train shape',train.shape)
org_train_shape = train.shape
# 查看训练集中0 1 的占比
print('positive sample',train[train.TARGET == 1].__len__())
print('positive ration',train[train.TARGET == 1].__len__() * 1.0/ len(train))
print('reading test datas')
test = pd.read_csv(dir + 'evaluation_public.csv')
print(test.head())
print('test shape',test.shape)
org_test_shape = test.shape
# 提取全部的企业EID
all_eid_number = len(set(list(test['EID'].unique()) + list(train['EID'].unique())))
print('all EID number ',all_eid_number)
# 获取企业基本信息表
print('reading 1.entbase')
entbase = pd.read_csv(dir + '1entbase.csv')
# 题目要求是用0填充，因此对nan进行填充
print("entbase fill nan in 0")
entbase = entbase.fillna(0)
print('entbase shape',entbase.shape)
print(entbase.head())

print('merge train/test')
train = pd.merge(train,entbase,on=['EID'],how='left')
# nan填充为单独一类
train = train.fillna(-7)

test = pd.merge(test,entbase,on=['EID'],how='left')
# nan填充为单独一类
test = test.fillna(-7)

# 删除训练集的EID/
del train['EID']
# 提交数据的EID单独保存
test_index = test.pop('EID')
print(test.shape,org_test_shape)

import lightgbm as lgb
# 抽样选择数据 控制同分布
tmp1 = train[train.TARGET==1]
tmp0 = train[train.TARGET==0]
x_valid_1 = tmp1.sample(frac=0.3, random_state=70, axis=0)
x_train_1 = tmp1.drop(x_valid_1.index.tolist())
x_valid_2 = tmp0.sample(frac=0.1, random_state=70, axis=0)
x_train_2 = tmp0.drop(x_valid_2.index.tolist())
X_train = pd.concat([x_train_1,x_train_2],axis=0)
# 分割标签和训练数据
y_train = X_train.pop('TARGET')
X_test = pd.concat([x_valid_1,x_valid_2],axis=0)
y_test = X_test.pop('TARGET')

feature_len = X_train.shape[1]
print(feature_len)
print train.columns

X_train = X_train.values
X_test = X_test.values
y_train = y_train.values
y_test = y_test.values

lgb_train = lgb.Dataset(X_train, y_train)
lgb_eval = lgb.Dataset(X_test, y_test, reference=lgb_train)

params = {
    'boosting_type': 'gbdt',
    'objective': 'binary',
    'metric': {'auc'},
    'num_leaves': 128,
    'learning_rate': 0.08,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 10,
    'verbose': 0
}
evals_result = {}
print('Start training...')
# train
gbm = lgb.train(params,
                lgb_train,
                num_boost_round = 1000,
                valid_sets=lgb_eval,
                feature_name=['f' + str(i + 1) for i in range(feature_len)],
                early_stopping_rounds=15,
                evals_result=evals_result)

# print('Plot metrics during training...')
# ax = lgb.plot_metric(evals_result, metric='auc')
# plt.show()
# #
# print('Plot feature importances...')
# lgb.plot_importance(gbm,max_num_features=feature_len)
# plt.show()

print('Start predicting...')

y_pred = gbm.predict(test.values, num_iteration=gbm.best_iteration)
y_pred = np.round(y_pred,8)
result = pd.DataFrame({'PROB':list(y_pred),
                       })
# 选择阈值，控制 0 1 的比例
result['FORTARGET'] = result['PROB'] > 0.26
result['PROB'] = result['PROB'].astype('str')
result['FORTARGET'] = result['FORTARGET'].astype('int')
result = pd.concat([test_index,result],axis=1)

print('predict pos tation',sum(result['FORTARGET']))

result = pd.DataFrame(result).drop_duplicates(['EID'])
result[['EID','FORTARGET','PROB']].to_csv('./evaluation_public.csv',index=None)

print(len(result.EID.unique()))
