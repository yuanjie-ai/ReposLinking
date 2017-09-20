---
```
# coding = utf-8
__title__ = 'Deploy'
__author__ = 'JieYuan'
__mtime__ = '2017/7/18'
import time
import pandas as pd
import pymysql
import happybase
import redis

# Connect to the database
# url: jdbc:mysql://10.10.10.10:8888/firspre1
class MySql(object):
    def __init__(self):
        params = dict(host='10.10.10.10',
                      user='FIRSusr',
                      password='vr6UT5Y06PR8',
                      db='firspre1',
                      charset='utf8mb4',
                      # cursorclass=pymysql.cursors.DictCursor,
                      autocommit=True)
        self.connection = pymysql.connect(**params)
    def __sqlQuery(self, sql):
        df = pd.read_sql(sql, self.connection)
        df = df.replace(['R' + str(i) for i in range(8004001, 8004011)],
                        [str(i) for i in range(1031, 1041)]) \
               .apply(lambda x: '-'.join(x), 1) \
               .tolist()
        self.connection.close()
        return df
    def in_sale_list_pc(self):
        sql_pc = "SELECT CateCode, ProductCode FROM firspre1.bx_product WHERE ChannelCode = 'SC1001' and status = 8"
        return self.__sqlQuery(sql_pc)
    def in_sale_list_app(self):
        sql_app = "SELECT CateCode, ProductCode FROM firspre1.bx_product WHERE ChannelCode = 'SC1002' and status = 8"
        return self.__sqlQuery(sql_app)

class HBase(object):
    def __init__(self):
        self.connection = happybase.Connection('xxx.com', 9090)
    def get_rec_list(self, member_id='member_id', tablename='yuanjie_rec_list'):
        member_id = member_id[::-1] + ':' + 'ins'
        tab = self.connection.table(tablename)
        row = tab.row(row=member_id)
        self.connection.close()
        if row == {}:
            return 'newUser'
        else:
            return [i.decode('utf-8') for i in row.values()] # ['0000000000000020743', '[1035-P10071, 1036-P10009]']
    def backup_rec_list(self, member_id='member_id', rec_list=[], tablename='yuanjie_rec_list'):
        tab = self.connection.table(tablename)
        member_id = member_id[::-1] + ':' + 'ins'
        tab.put(row=member_id, data={'f: rec_list': str(rec_list)})
        self.connection.close()
    def exposure_n(self, member_id='member_id' , recing_product=[], tablename='ns_firs:tdm_firs_rec_agg_info'):
        member_id = ':'.join([member_id[::-1], time.strftime("%Y-%m-%d", time.localtime()), recing_product])
        tab = self.connection.table(tablename)
        row = tab.row(row=member_id)
        self.connection.close()
        if b'f:exposure_n' in row.keys():
            return int(row[b'f:exposure_n'].decode('utf-8'))
        else:
            return 0

class Update(object):
    def __init__(self, data):
        self.jsonData = eval(data)
        self.member_id = self.jsonData['acctNo']
        self.recing_product = self.jsonData['l2ProdGroupCd'] + '-' + self.jsonData['l3ProdGroupCd']
        self.operationType = self.jsonData['operationType']
    def __update_inSale_list(self, offline_rec_list=[], in_sale_list=[]):
        x = offline_rec_list + in_sale_list
        l = list(set(x))
        l.sort(key=x.index)
        ls = list(set(offline_rec_list).difference(in_sale_list))
        if ls == []:
            return l
        else:
            for i in ls:
                l.remove(i)
                return l
    def update_rec_list(self, recing_product=[], offline_rec_list=[], in_sale_list=[]):
        rec_list = self.__update_inSale_list(offline_rec_list, in_sale_list)
        if recing_product in rec_list:
            rec_list.remove(recing_product)
        return rec_list + [recing_product]

class Redis(object):
    def __init__(self):
        self.pool = redis.ConnectionPool(host='11.11.11.11', port=6379, db=0)
        self.r = redis.Redis(connection_pool = self.pool)
    def get_rec_list(self, acct_no='acct_no_1'):
        return self.r.get(acct_no)
    def backup_rec_list(self, acct_no='acct_no_1', rec_list=[]):
        return self.r.set(acct_no, str(rec_list))


# init
m = MySql()
h = HBase()

# online_rec_list
jd = '{"acctNo":"0201437284", \
       "cityCd":"025", \
       "countryCd":"-", \
       "entryTime":"2017-07-03 15:04:13,127", \
       "l1ProdGroupCd":"0", \
       "l2ProdGroupCd":"1035", \
       "l3ProdGroupCd":"P10071", \
       "operationType":"1", \
       "osType":"Win7", \
       "prodVisitDrtn":"9", \
       "prodVisitUrl":"https://dq.suning.com/bill/order/cashBuy.htm?productId=2302857&channelCode=PC_JRMH&idsTrustFrom=suning&idsFirstLogin=true","productCd":"", \
       "prvnceCd":"100", \
       "visitChannel":"WEB", \
       "visitId":"149906541806166592", \
       "visitNum":"5"}'

try:
    u = Update(jd)
    if u.operationType == '2':
        if h.exposure_n(u.member_id, u.recing_product) >= 5:
            acct_no_rec_list = h.get_rec_list(u.member_id)
            update_rec_list = u.update_rec_list(recing_product = u.recing_product,
                                                offline_rec_list = eval(acct_no_rec_list[1]),
                                                in_sale_list = m.in_sale_list_pc())
            Redis().backup_rec_list(acct_no_rec_list[0], update_rec_list[:4])
            print((acct_no_rec_list[0], update_rec_list[:4]))
    else:
        acct_no_rec_list = h.get_rec_list(u.member_id)
        update_rec_list = u.update_rec_list(recing_product = u.recing_product,
                                            offline_rec_list = eval(acct_no_rec_list[1]),
                                            in_sale_list = m.in_sale_list_pc())
        Redis().backup_rec_list(acct_no_rec_list[0], update_rec_list[:4])
        print((acct_no_rec_list[0], update_rec_list[:4]))
except BaseException:
    print(-1)
else:
    print(0)

```
