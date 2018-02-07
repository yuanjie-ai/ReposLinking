from udfs import *

path = '/algor/yuanjie/Competitions/1_糖尿病/DataCache/'
train = pd.read_csv(path + 'f_train_20180204.csv', encoding='gbk')
test = pd.read_csv(path + 'f_test_a_20180204.csv', encoding='gbk')

train.columns = Str.convertToPinyin(*train.columns)
test.columns = Str.convertToPinyin(*test.columns)

data = pd.concat([train, test.assign(label=-1)])

# MissingFeatures
miss1 = ['SNP21', 'SNP22', 'SNP23', 'DMjiazushi', 'SNP54', 'SNP55', 'ACEID']
miss2 = ['SNP22', 'SNP23', 'DMjiazushi', 'SNP54', 'SNP55', 'ACEID']
miss3 = ['shousuoya', 'shuzhangya', 'tangshaiyunzhou', 'DMjiazushi'] # miss3 = ['shousuoya', 'tangshaiyunzhou', 'DMjiazushi']
miss4 = ['shuzhangya', 'tangshaiyunzhou', 'DMjiazushi']
miss5 = ['DMjiazushi', 'shousuoya', 'shuzhangya']
miss6 = ['AST', 'DMjiazushi']
miss7 = ['DMjiazushi', 'SNP54', 'SNP55', 'ACEID', 'SNP21', 'SNP22', 'SNP23', 'shousuoya', 'shuzhangya',
         'tangshaiyunzhou', 'AST']
miss8 = ['SNP54', 'SNP55', 'ACEID', 'SNP21', 'SNP22', 'SNP23', 'DMjiazushi']
miss9 = ['SNP55', 'ACEID', 'SNP21', 'SNP22', 'SNP23', 'DMjiazushi']
miss10 = ['ACEID', 'SNP21', 'SNP22', 'SNP23', 'DMjiazushi']

data1 = data.copy()
for i, miss in enumerate([miss1, miss2, miss3, miss4, miss5, miss6, miss7, miss8, miss9, miss10]):
    data1['miss_' + str(i)] = data1[miss].isnull().sum(1)

poly = PolynomialFeatures(degree=2, interaction_only=False, include_bias=True)
X = poly.fit_transform(data1[data1.label != -1].fillna('-999').drop(['label'], 1).values)
y = train.label.ravel()

clf = LGBMClassifier(
    boosting_type='gbdt',  # 'rf', 'dart', 'goss'
    objective='binary',  # objective='multiclass', num_class = 3
    max_depth=-1,
    num_leaves=2 ** 7 - 1,
    learning_rate=0.01,
    n_estimators=500,

    subsample=0.8,
    subsample_freq=1,
    colsample_bytree=0.8,

    reg_alpha=0.0,
    reg_lambda=0,

    scale_pos_weight=1, 

    random_state=888,
    n_jobs=30
)
scores = cross_val_score(clf, X, y, scoring='roc_auc', cv=StratifiedKFold(5, True, 42))

print(scores)
print('Auc: %s' % np.mean(scores), 'Std: %s' % np.std(scores))
