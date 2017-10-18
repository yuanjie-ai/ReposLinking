# encoding: utf-8
'''
Created on 2017年10月11日

@author: c
'''
import pandas as pd
import numpy as np

labelfeature=pd.read_csv('./data/train.csv')
test=pd.read_csv('./data/evaluation_public.csv')
EID=test.copy()
basicfeature=pd.read_csv('./data/1entbase.csv')
basicfeature=basicfeature.fillna(0)
print (labelfeature.head(5))
print (basicfeature.head(5))
basic=pd.merge(labelfeature,basicfeature,how='left',on='EID')
test=pd.merge(test,basicfeature,how='left',on='EID')
print(basic.columns)

branchfeature=pd.read_csv('./data/3branch.csv')

home_prob=branchfeature.groupby(by=['EID'])['IFHOME'].sum()/branchfeature.groupby(by=['EID'])['IFHOME'].count()
branch_sum=branchfeature.groupby(by=['EID'])['IFHOME'].count()
home_sum=branchfeature.groupby(by=['EID'])['IFHOME'].sum()
basic['home_prob']=basic['EID'].map(lambda x:home_prob.get(x,-1))
basic['branch_sum']=basic['EID'].map(lambda x:branch_sum.get(x,0))
basic['home_sum']=basic['EID'].map(lambda x:home_sum.get(x,0))
test['home_prob']=test['EID'].map(lambda x:home_prob.get(x,-1))
test['branch_sum']=test['EID'].map(lambda x:branch_sum.get(x,0))
test['home_sum']=test['EID'].map(lambda x:home_sum.get(x,0))
branchfeature=branchfeature[branchfeature.B_ENDYEAR>0]

survive_sum=branchfeature.groupby(by=['EID'])['IFHOME'].count()
basic['survive_sum']=basic['EID'].map(lambda x:survive_sum.get(x,0))
basic['survive_rate']=basic['survive_sum']/basic['branch_sum']
basic=basic.fillna(-1)

test['survive_sum']=test['EID'].map(lambda x:survive_sum.get(x,0))
test['survive_rate']=test['survive_sum']/test['branch_sum']
test=test.fillna(-1)
# print (home_prob[6])
branchfeature=pd.read_csv('./data/4invest.csv')

home_prob=branchfeature.groupby(by=['EID'])['IFHOME'].sum()/branchfeature.groupby(by=['EID'])['IFHOME'].count()
branch_sum=branchfeature.groupby(by=['EID'])['IFHOME'].count()
home_sum=branchfeature.groupby(by=['EID'])['IFHOME'].sum()
basic['inv_home_prob']=basic['EID'].map(lambda x:home_prob.get(x,-1))
basic['inv_branch_sum']=basic['EID'].map(lambda x:branch_sum.get(x,0))
basic['inv_home_sum']=basic['EID'].map(lambda x:home_sum.get(x,0))

test['inv_home_prob']=test['EID'].map(lambda x:home_prob.get(x,-1))
test['inv_branch_sum']=test['EID'].map(lambda x:branch_sum.get(x,0))
test['inv_home_sum']=test['EID'].map(lambda x:home_sum.get(x,0))

branchfeature=branchfeature[branchfeature.BTENDYEAR>0]

survive_sum=branchfeature.groupby(by=['EID'])['IFHOME'].count()
basic['inv_survive_sum']=basic['EID'].map(lambda x:survive_sum.get(x,0))

basic['inv_survive_rate']=basic['inv_survive_sum']/basic['inv_branch_sum']
basic=basic.fillna(-1)
 
test['inv_survive_sum']=test['EID'].map(lambda x:survive_sum.get(x,0))

test['inv_survive_rate']=test['inv_survive_sum']/test['inv_branch_sum']
test=test.fillna(-1)

rightsfeature=pd.read_csv('./data/5right.csv')
rightsfeature=rightsfeature.fillna(-1)


rights_sum=rightsfeature.groupby(by=['EID'])['RIGHTTYPE'].count()
basic['rights_sum']=basic['EID'].map(lambda x:rights_sum.get(x,0))
test['rights_sum']=test['EID'].map(lambda x:rights_sum.get(x,0))
rightsfeature=rightsfeature[rightsfeature.FBDATE!=-1]
rights_sum=rightsfeature.groupby(by=['EID'])['RIGHTTYPE'].count()
basic['rights_sum_get']=basic['EID'].map(lambda x:rights_sum.get(x,0))
test['rights_sum_get']=test['EID'].map(lambda x:rights_sum.get(x,0))


projectfeature=pd.read_csv('./data/6project.csv')

project_home_sum=projectfeature.groupby(by=['EID'])['IFHOME'].sum()
project_sum=projectfeature.groupby(by=['EID'])['IFHOME'].count()

basic['project_home_sum']=basic['EID'].map(lambda x:project_home_sum.get(x,0))
test['project_home_sum']=test['EID'].map(lambda x:project_home_sum.get(x,0))
basic['project_sum']=basic['EID'].map(lambda x:project_sum.get(x,0))
test['project_sum']=test['EID'].map(lambda x:project_sum.get(x,0))
basic['project_home_rate']=basic['project_home_sum']/basic['project_sum']
test['project_home_rate']=test['project_home_sum']/test['project_sum']


excutefeature=pd.read_csv('./data/7lawsuit.csv')

excute_sum=excutefeature.groupby(by=['EID'])['LAWAMOUNT'].count()
excute_LAWA_sum=excutefeature.groupby(by=['EID'])['LAWAMOUNT'].sum()
basic['excute_sum']=basic['EID'].map(lambda x:excute_sum.get(x,0))
test['excute_sum']=test['EID'].map(lambda x:excute_sum.get(x,0))
basic['excute_LAWA_sum']=basic['EID'].map(lambda x:excute_LAWA_sum.get(x,0))
test['excute_LAWA_sum']=test['EID'].map(lambda x:excute_LAWA_sum.get(x,0))
basic['excute_LAWA_ave']=basic['excute_LAWA_sum']/basic['excute_sum']
test['excute_LAWA_ave']=test['excute_LAWA_sum']/test['excute_sum']


losstrustfeature=pd.read_csv('./data/8breakfaith.csv')
losstrustfeature=losstrustfeature.fillna(-1)
losstrust_sum=losstrustfeature.groupby(by=['EID'])['FBDATE'].count()
losstrustfeature=losstrustfeature[losstrustfeature.SXENDDATE!=-1]
losstrust_end_sum=losstrustfeature.groupby(by=['EID'])['FBDATE'].count()
basic['losstrust_sum']=basic['EID'].map(lambda x:losstrust_sum.get(x,0))
test['losstrust_sum']=test['EID'].map(lambda x:losstrust_sum.get(x,0))
basic['losstrust_end_sum']=basic['EID'].map(lambda x:losstrust_end_sum.get(x,0))
test['losstrust_end_sum']=test['EID'].map(lambda x:losstrust_end_sum.get(x,0))
basic['losstrust_end_rate']=basic['losstrust_end_sum']/basic['losstrust_sum']
test['losstrust_end_rate']=test['losstrust_end_sum']/test['losstrust_sum']


offerfeature=pd.read_csv('./data/9recruit.csv')

offer_sum=offerfeature.groupby(by=['EID'])['RECRNUM'].count()
offer_RECRNUM_sum=offerfeature.groupby(by=['EID'])['RECRNUM'].sum()
basic['offer_sum']=basic['EID'].map(lambda x:offer_sum.get(x,0))
test['offer_sum']=test['EID'].map(lambda x:offer_sum.get(x,0))
basic['offer_RECRNUM_sum']=basic['EID'].map(lambda x:offer_RECRNUM_sum.get(x,0))
test['offer_RECRNUM_sum']=test['EID'].map(lambda x:offer_RECRNUM_sum.get(x,0))
basic['offer_RECRNUM_ave']=basic['offer_RECRNUM_sum']/basic['offer_sum']
test['offer_RECRNUM_ave']=test['offer_RECRNUM_sum']/test['offer_sum']

import lightgbm as lgb
from sklearn.model_selection import train_test_split
# 'EID','TARGET', 
predictor=[ 'RGYEAR', 'HY', 'ZCZB', 'ETYPE', 'MPNUM', 'INUM',
       'FINZB', 'FSTINUM', 'TZINUM','home_prob','branch_sum','home_sum','survive_sum','survive_rate',
       'inv_home_prob','inv_branch_sum','inv_home_sum','inv_survive_sum','inv_survive_rate','rights_sum'
       ,'rights_sum_get','project_home_sum','project_sum','project_home_rate','excute_sum',
       'excute_LAWA_sum','excute_LAWA_ave','losstrust_sum','losstrust_end_sum','losstrust_end_rate',
       'offer_sum','offer_RECRNUM_sum','offer_RECRNUM_ave']
label=basic.TARGET
train=basic[predictor]
test=test[predictor]
  
X_train,y_train,X_validation, y_validation = train_test_split(train,label,test_size=0.3,random_state=10000)
   
lgb_train = lgb.Dataset(X_train,  X_validation)
lgb_eval = lgb.Dataset(y_train, y_validation)
# specify your configurations as a dict
params = {
    'task': 'train',
    'boosting': 'gbdt',
    'objective': 'binary',
    'metric': {'auc',},#,binary_logloss'
    'num_leaves': 2**6,
#     'max_depth':7,
    'learning_rate': 0.05,
#     'feature_fraction': 0.3,
#     'bagging_fraction': 0.5,
#     'bagging_freq': 5,
    'verbose': 0,
}
print('Start training...')
# train
   
gbm = lgb.train(params,
                lgb_train,
                num_boost_round=1000,
                valid_sets=lgb_eval,
                early_stopping_rounds=30
                )
   
y_pred = gbm.predict(test, num_iteration=gbm.best_iteration)


EID['PROB']=y_pred
EID['FORTARGET']=EID['PROB'].apply(lambda x:int(x*2))
EID.to_csv('result.csv',index=False)

print('Calculate feature importances...')
print('Feature names:', gbm.feature_name())
# feature importances
print('Feature importances:', list(gbm.feature_importance()))
