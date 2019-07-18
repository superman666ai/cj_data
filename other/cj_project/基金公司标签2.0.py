#coding:utf-8
import os
import cx_Oracle
import pandas as pd
import numpy as np
import time
from datetime import timedelta
from dateutil.parser import parse
import xlrd
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import math
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta


[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)

#投研平台库
[userNamepif, passwordpif, hostIPpif, dbNamepif] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
#[userNamepif, passwordpif, hostIPpif, dbNamepif] = ['pif', 'pif', '172.16.125.151', 'pif']
try:
	fund_dbpra = cx_Oracle.connect(user=userNamepif, password=passwordpif, dsn=hostIPpif + '/' + dbNamepif)
	cu_pra = fund_dbpra.cursor()
except cx_Oracle.DatabaseError as e:
	print('数据库链接失败')

path = os.path.dirname(os.path.realpath(__file__)) + '/'

rptdate = '20190331'

sql = '''
        SELECT 
        DISTINCT b.OB_OBJECT_NAME_1090,a.F4_1120
        FROM
        (SELECT 
        F1_1120,F4_1120
        FROM
        wind.TB_OBJECT_1120)a
        LEFT JOIN 
        (SELECT
        F2_1090,OB_OBJECT_NAME_1090
        FROM
        wind.TB_OBJECT_1090 )b
        ON a.F1_1120 = b.F2_1090   
        LEFT JOIN 
        (SELECT
        F2_1289
        FROM
        wind.TB_OBJECT_1289 )c
        ON c.F2_1289 = b.F2_1090                     

        '''

sql = '''
        SELECT 
        DISTINCT S_INFO_WINDCODE      
        FROM wind.chinamutualfundbenchmarkeod
        '''
#中证800指数：000906；中证国债：h11006；恒生指数：HSI；中证综合债指数：h11009；中证短债：h11015；中证可转债：930898；MSCI全球指数：892400

cu = fund_db.cursor()
trade_dates = pd.DataFrame(cu.execute(sql).fetchall(), columns=['a'])
trade_dates
trade_dates.to_excel(path+r'基金公司打标签156147.xlsx')
#####################################################################################################
#以下为产品概况（基金数量）及主打产品类型标签
#####################################################################################################
################################################1、基金公司基金总数标签#################################################################
#无需要设置的参数
#以下为取数逻辑，得到三列分别为基金公司名、基金公司成立日、截止目前存续基金产品总数
sql = '''

        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,COUNT(DISTINCT c.F1_1099)
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018

        '''

cu = fund_db.cursor()
sum_product_amount = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date','sum_product_amount'])

amount = sum_product_amount['sum_product_amount']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_product_amount.loc[sum_product_amount['sum_product_amount'] > high, 'temp'] = '高-'
sum_product_amount.loc[(sum_product_amount['sum_product_amount'] <= high) & (sum_product_amount['sum_product_amount'] > mid_high), 'temp'] = '中高-'
sum_product_amount.loc[(sum_product_amount['sum_product_amount'] <= mid_high) & (sum_product_amount['sum_product_amount'] > middle), 'temp'] = '中-'
sum_product_amount.loc[(sum_product_amount['sum_product_amount'] <= middle) & (sum_product_amount['sum_product_amount'] > mid_low), 'temp'] = '中低-'
sum_product_amount.loc[sum_product_amount['sum_product_amount'] <= mid_low, 'temp'] = '低-'

list_amount = []
for amount in sum_product_amount['sum_product_amount']:
    amount = str(amount)
    list_amount.append( amount)
sum_product_amount['sum_product_amount'] = list_amount

#在标签后加上基金产品数量
sum_product_amount.loc[: , '基金总数标签'] = sum_product_amount.loc[: , 'temp'] + sum_product_amount.loc[: , 'sum_product_amount']
sum_product_amount = sum_product_amount.drop(['temp','sum_product_amount'], axis = 1)
sum_product_amount.to_excel(path+r'基金公司基金总数标签.xlsx')
#################################################2、基金公司非货币基金产品总数标签####################################################
# 无需设置参数
##以下为取数逻辑，得到三列分别为基金公司名、基金公司成立日、截止目前存续非货币基金产品总数
sql = '''

        SELECT
        b.OB_OBJECT_NAME_1018,b.F35_1018,COUNT(DISTINCT a.F1_1099)
        FROM
        (SELECT
        F12_1099,F1_1099,F100_1099,F23_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F100_1099 != '货币市场型')a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018
        GROUP BY b.OB_OBJECT_NAME_1018,b.F35_1018

        '''

cu = fund_db.cursor()
sum_nonmonetary_product = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'sum_nonmonetary_product'])

amount = sum_nonmonetary_product['sum_nonmonetary_product']
amount = np.array(amount)

# 按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

# 根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_nonmonetary_product.loc[sum_nonmonetary_product['sum_nonmonetary_product'] > high, 'temp'] = '高-'
sum_nonmonetary_product.loc[(sum_nonmonetary_product['sum_nonmonetary_product'] <= high) & (sum_nonmonetary_product['sum_nonmonetary_product'] > mid_high), 'temp'] = '中高-'
sum_nonmonetary_product.loc[(sum_nonmonetary_product['sum_nonmonetary_product'] <= mid_high) & (sum_nonmonetary_product['sum_nonmonetary_product'] > middle), 'temp'] = '中-'
sum_nonmonetary_product.loc[(sum_nonmonetary_product['sum_nonmonetary_product'] <= middle) & (sum_nonmonetary_product['sum_nonmonetary_product'] > mid_low), 'temp'] = '中低-'
sum_nonmonetary_product.loc[sum_nonmonetary_product['sum_nonmonetary_product'] <= mid_low, 'temp'] = '低-'

list_amount = []
for amount in sum_nonmonetary_product['sum_nonmonetary_product']:
    amount = str(amount)
    list_amount.append(amount)
sum_nonmonetary_product['sum_nonmonetary_product'] = list_amount

# 在标签后加上非货币基金产品数量
sum_nonmonetary_product.loc[:, '非货币基金总数标签'] = sum_nonmonetary_product.loc[:, 'temp'] + sum_nonmonetary_product.loc[:,'sum_nonmonetary_product']
sum_nonmonetary_product = sum_nonmonetary_product.drop(['temp','sum_nonmonetary_product'], axis = 1)

sum_nonmonetary_product.to_excel(path+r'基金公司非货币基金产品总数标签.xlsx')



#########################################3、4、基金公司偏股类、偏债类基金总数标签########################################################
#无需设置参数
##以下为取数逻辑，得到三列分别为基金公司名、基金公司成立日、截止目前存续货币基金产品交易代码
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,d.F16_1090
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099,F23_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL ) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)d
        ON c.F1_1099 = d.F2_1090

        '''

cu = fund_db.cursor()
company_fund = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date','基金代码'])

###此处需要根据本地表格将基金分类！！
#path = 'C:/Users/wx/Desktop/分类-0213.xlsx'
#data = pd.read_excel(path)
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
data = data[['基金代码','六大类型']]
print('data len is:',len(data))
data = pd.merge( data, company_fund, on = '基金代码', how = 'right')

#继续按照不同类型分类统计各基金公司当前存续的偏股类或偏债类基金总数
stock = data[data['六大类型'] == '偏股类']
bond = data[data['六大类型'] == '偏债类']
print('bond len is:',len(bond))
stock = stock.groupby(by = ['company','start_up_date'])['基金代码'].count()
stock = pd.DataFrame(stock)
bond = bond.groupby(by = ['company','start_up_date'])['基金代码'].count()
bond = pd.DataFrame(bond)

#####下面打偏债类基金标签
amount = bond['基金代码']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
bond.loc[bond['基金代码'] > high, 'temp'] = '高-'
bond.loc[(bond['基金代码'] <= high) & (bond['基金代码'] > mid_high), 'temp'] = '中高-'
bond.loc[(bond['基金代码'] <= mid_high) & (bond['基金代码'] > middle), 'temp'] = '中-'
bond.loc[(bond['基金代码'] <= middle) & (bond['基金代码'] > mid_low), 'temp'] = '中低-'
bond.loc[bond['基金代码'] <= mid_low, 'temp'] = '低-'

list_amount = []
for amount in bond['基金代码']:
    amount = str(amount)
    list_amount.append( amount)
bond['基金代码'] = list_amount

#在标签后加上偏债基金产品数量
bond.loc[: , '偏债基金总数标签'] = bond.loc[: , 'temp'] + bond.loc[: , '基金代码']

#由于不是所有基金公司均有偏债基金，此处引用1中得到的标签，对没有偏债基金的公司打上‘无’的标签
bond = pd.merge( bond, sum_product_amount, on = [ 'company', 'start_up_date'], how = 'right', )
bond.loc[bond['偏债基金总数标签'].isnull(), '偏债基金总数标签'] = '无'
bond = bond.drop( [ '基金代码', 'temp', '基金总数标签'], axis = 1)
bond.to_excel(path+r'偏债基金产品数量.xlsx')
#####下面打偏股类基金标签
amount = stock['基金代码']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
stock.loc[stock['基金代码'] > high, 'temp'] = '高-'
stock.loc[(stock['基金代码'] <= high) & (stock['基金代码'] > mid_high), 'temp'] = '中高-'
stock.loc[(stock['基金代码'] <= mid_high) & (stock['基金代码'] > middle), 'temp'] = '中-'
stock.loc[(stock['基金代码'] <= middle) & (stock['基金代码'] > mid_low), 'temp'] = '中低-'
stock.loc[stock['基金代码'] <= mid_low, 'temp'] = '低-'

list_amount = []
for amount in stock['基金代码']:
    amount = str(amount)
    list_amount.append( amount)
stock['基金代码'] = list_amount

#在标签后加上偏股基金产品数量
stock.loc[: , '偏股基金总数标签'] = stock.loc[: , 'temp'] + stock.loc[: , '基金代码']

#由于不是所有基金公司均有偏股基金，此处引用1中得到的标签，对没有偏股基金的公司打上‘无’的标签
stock = pd.merge( stock, sum_product_amount, on = [ 'company', 'start_up_date'], how = 'right', )
stock.loc[stock['偏股基金总数标签'].isnull(), '偏股基金总数标签'] = '无'
stock = stock.drop( [ '基金代码', 'temp', '基金总数标签'], axis = 1)
stock.to_excel(path+r'偏股基金总数标签.xlsx')



#################################################5、基金公司货币基金产品总数标签####################################################
# 无需设置参数
##以下为取数逻辑，得到三列分别为基金公司名、基金公司成立日、截止目前存续货币基金产品总数
sql = '''

        SELECT
        b.OB_OBJECT_NAME_1018,b.F35_1018,COUNT(DISTINCT a.F1_1099)
        FROM
        (SELECT
        F12_1099,F1_1099,F100_1099,F23_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F100_1099 = '货币市场型')a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018
        GROUP BY b.OB_OBJECT_NAME_1018,b.F35_1018

        '''

cu = fund_db.cursor()
sum_monetary_product = pd.DataFrame(cu.execute(sql).fetchall(),columns=['company', 'start_up_date', 'sum_monetary_product'])

amount = sum_monetary_product['sum_monetary_product']
amount = np.array(amount)

# 按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

# 根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_monetary_product.loc[sum_monetary_product['sum_monetary_product'] > high, 'temp'] = '高-'
sum_monetary_product.loc[(sum_monetary_product['sum_monetary_product'] <= high) & (sum_monetary_product['sum_monetary_product'] > mid_high), 'temp'] = '中高-'
sum_monetary_product.loc[(sum_monetary_product['sum_monetary_product'] <= mid_high) & (sum_monetary_product['sum_monetary_product'] > middle), 'temp'] = '中-'
sum_monetary_product.loc[(sum_monetary_product['sum_monetary_product'] <= middle) & (sum_monetary_product['sum_monetary_product'] > mid_low), 'temp'] = '中低-'
sum_monetary_product.loc[sum_monetary_product['sum_monetary_product'] <= mid_low, 'temp'] = '低-'

list_amount = []
for amount in sum_monetary_product['sum_monetary_product']:
    amount = str(amount)
    list_amount.append(amount)
sum_monetary_product['sum_monetary_product'] = list_amount

# 在标签后加上货币基金产品数量
sum_monetary_product.loc[:, '货币基金总数标签'] = sum_monetary_product.loc[:, 'temp'] + sum_monetary_product.loc[:,'sum_monetary_product']

# 由于不是所有基金公司均有货币基金，此处引用1中得到的标签，对没有货币基金的公司打上‘无’的标签
sum_monetary_product = pd.merge(sum_monetary_product, sum_product_amount, on=['company', 'start_up_date'],how='right', )
sum_monetary_product = sum_monetary_product.drop(['temp', 'sum_monetary_product', '基金总数标签'], axis=1)
sum_monetary_product.loc[sum_monetary_product['货币基金总数标签'].isnull(), '货币基金总数标签'] = '无'

sum_monetary_product.to_excel(path+r'货币基金总数标签.xlsx')


##################################################6、基金公司QDII基金产品总数标签###############################################
# 无需设置参数
##以下为取数逻辑，得到三列分别为基金公司名、基金公司成立日、截止目前存续QDII基金产品总数
sql = '''

        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,COUNT(DISTINCT d.F1_1099)
        FROM
        (SELECT
        a.F15_1104,b.F1_1099,b.F12_1099
        FROM
        (SELECT
        F15_1104,F51_1104
        FROM  wind.TB_OBJECT_1104
        WHERE F51_1104 IS NOT NULL)a
        JOIN
        (SELECT
        F1_1099,F12_1099,F23_1099
        FROM wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL)b
        ON a.F15_1104 = b.F1_1099)d
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018)c
        ON d.F12_1099 = c.F34_1018
        GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018

        '''

cu = fund_db.cursor()
sum_QDII_product = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'sum_QDII_product'])

amount = sum_QDII_product['sum_QDII_product']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_QDII_product.loc[sum_QDII_product['sum_QDII_product'] > high, 'temp'] = '高-'
sum_QDII_product.loc[(sum_QDII_product['sum_QDII_product'] <= high) & (sum_QDII_product['sum_QDII_product'] > mid_high), 'temp'] = '中高-'
sum_QDII_product.loc[(sum_QDII_product['sum_QDII_product'] <= mid_high) & (sum_QDII_product['sum_QDII_product'] > middle), 'temp'] = '中-'
sum_QDII_product.loc[(sum_QDII_product['sum_QDII_product'] <= middle) & (sum_QDII_product['sum_QDII_product'] > mid_low), 'temp'] = '中低-'
sum_QDII_product.loc[sum_QDII_product['sum_QDII_product'] <= mid_low, 'temp'] = '低-'

list_amount = []
for amount in sum_QDII_product['sum_QDII_product']:
    amount = str(amount)
    list_amount.append( amount)
sum_QDII_product['sum_QDII_product'] = list_amount

#在标签后加上QDII基金产品数量
sum_QDII_product.loc[: , 'QDII基金总数标签'] = sum_QDII_product.loc[: , 'temp'] + sum_QDII_product.loc[: , 'sum_QDII_product']


#由于不是所有基金公司均有QDII基金，此处引用1中得到的标签，对没有QDII基金的公司打上‘无’的标签
sum_QDII_product = pd.merge( sum_QDII_product, sum_product_amount, on = [ 'company', 'start_up_date'], how = 'right', )
sum_QDII_product = sum_QDII_product.drop(['temp','sum_QDII_product','基金总数标签'], axis = 1)
sum_QDII_product.loc[sum_QDII_product['QDII基金总数标签'].isnull(), 'QDII基金总数标签'] = '无'
sum_QDII_product.to_excel(path+r'QDII基金总数标签.xlsx')

##################################################7、主打产品类型标签（数量维度）###############################################
#打产品线丰富或单一判断条件为：基金总数小于30或存在四中类型基金的其中任意两种单个个数均小于等于2
#主打产品类型（数量）标签（一级分类）
# 无需设置参数
##以下为取数逻辑，得到三列分别为基金公司名、基金公司成立日、基金产品交易代码
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090
        GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090

        '''

cu = fund_db.cursor()
fund_of_company = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date','基金代码'])

#此处需要本地地址！！！
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
data = data[['基金代码','一级分类','二级分类']]
data = pd.merge( data, fund_of_company, on = '基金代码', how = 'right')

sum_ = data.groupby(by = ['一级分类','company','start_up_date'])['基金代码'].count()
sum_ = pd.DataFrame(sum_)
sum_.columns = ['fund_amount']
sum_ = sum_.sort_values( by = ['一级分类', 'fund_amount'],ascending = False)
sum_ = sum_.reset_index([ '一级分类', 'company', 'start_up_date'])

category = sum_.drop_duplicates( ['一级分类'])
category = category['一级分类']
company_class1 = pd.DataFrame(columns = ('company','主打产品类型标签（数量维度一级分类）'))
firm = sum_.drop_duplicates(['company'])
company_class1['company'] = firm['company']
company_class1 = company_class1.reset_index( drop = True )
for i in category:
    sum_cat = sum_[sum_['一级分类'] == i]
    sum_cat = sum_cat.reset_index( drop = True)
    good_at = sum_cat.head(1)
    good_at['temp'] = '主打' + good_at['一级分类'] + '基金'
    selected = company_class1[company_class1['company'] == str(good_at['company'][0])]['主打产品类型标签（数量维度一级分类）']
    selected = selected.reset_index(drop = True)
    if type(selected[0]) != type(''):
        company_class1.loc[company_class1['company'] == str(good_at['company'][0]), '主打产品类型标签（数量维度一级分类）'] = str(good_at['temp'][0])
    else:
        company_class1.loc[company_class1['company'] == str(good_at['company'][0]), '主打产品类型标签（数量维度一级分类）'] = company_class1.loc[company_class1['company'] == str(good_at['company'][0]), '主打产品类型标签（数量维度一级分类）'] + ',' + str(good_at['temp'][0])
#company_class1 即为最终一级标签
res_company_class1 = company_class1
company_class1.to_excel(path+'company_class1.xlsx')
sum_ = data.groupby(by = ['二级分类','company','start_up_date'])['基金代码'].count()
sum_ = pd.DataFrame(sum_)
sum_.columns = ['fund_amount']
sum_ = sum_.sort_values( by = ['二级分类', 'fund_amount'],ascending = False)
sum_ = sum_.reset_index([ '二级分类', 'company', 'start_up_date'])

category = sum_.drop_duplicates( ['二级分类'])
category = category['二级分类']
company_class2 = pd.DataFrame(columns = ('company','主打产品类型标签（数量维度二级分类）'))
firm = sum_.drop_duplicates(['company'])
company_class2['company'] = firm['company']
company_class2 =  company_class2.reset_index( drop = True)
for i in category:
    sum_cat = sum_[sum_['二级分类'] == i]
    sum_cat = sum_cat.reset_index( drop = True)
    good_at = sum_cat.head(1)
    good_at['temp'] = '主打' + good_at['二级分类'] + '基金'
    selected = company_class2[company_class2['company'] == str(good_at['company'][0])]['主打产品类型标签（数量维度二级分类）']
    selected = selected.reset_index(drop = True)
    if type(selected[0]) != type(''):
        company_class2.loc[company_class2['company'] == str(good_at['company'][0]), '主打产品类型标签（数量维度二级分类）'] = str(good_at['temp'][0])
    else:
        company_class2.loc[company_class2['company'] == str(good_at['company'][0]), '主打产品类型标签（数量维度二级分类）'] = company_class2.loc[company_class2['company'] == str(good_at['company'][0]), '主打产品类型标签（数量维度二级分类）'] + ',' + str(good_at['temp'][0])
#company_class2即为最终二级标签
company_class2.to_excel(path+r'主打产品类型标签（数量维度二级分类）.xlsx')
res_company_class2_num = company_class2
#####################################################################################################
#以下为产品概况（基金规模）标签
#####################################################################################################
# 无需设置参数
##以下为取数逻辑，得到三列分别为基金公司名、基金公司成立日、基金产品交易代码、该基金净值
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090,SUM(d.F3_1104)
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F3_1104,F15_1104,MAX(F14_1104)
        FROM wind.TB_OBJECT_1104
        GROUP BY F3_1104,F15_1104)d
        ON c.F1_1099 = d.F15_1104
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090
        GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090

        '''

cu = fund_db.cursor()
fund_value = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date','基金代码','fund_value'])
##此处需要本地地址！！！
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
data = data[['基金代码','六大类型']]
data = pd.merge( data, fund_value, on = '基金代码', how = 'right')

################################################1、基金公司总规模行业排名标签#################################################################
sum_ = data.groupby(by = ['company','start_up_date'])['fund_value'].sum()
sum_ = pd.DataFrame(sum_)
amount = sum_['fund_value']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_.loc[sum_['fund_value'] > high, '基金总规模行业排名标签'] = '高-'
sum_.loc[(sum_['fund_value'] <= high) & (sum_['fund_value'] > mid_high), '基金总规模行业排名标签'] = '中高-'
sum_.loc[(sum_['fund_value'] <= mid_high) & (sum_['fund_value'] > middle), '基金总规模行业排名标签'] = '中-'
sum_.loc[(sum_['fund_value'] <= middle) & (sum_['fund_value'] > mid_low), '基金总规模行业排名标签'] = '中低-'
sum_.loc[sum_['fund_value'] <= mid_low, '基金总规模行业排名标签'] = '低-'

#给基金公司排名并加序号
rank = pd.DataFrame(sum_['fund_value']).rank()
rank.columns = ['temp']
sum_ = pd.merge( sum_, rank, on = ['company','start_up_date'])

#将序号全部变为int格式,再转为字符串
list_rank = []
for num in sum_['temp']:
    num = int(num)
    num = str(num)
    list_rank.append(num)
sum_['temp'] = list_rank

#在标签后加上基金规模行业排名
sum_.loc[: , '基金总规模行业排名标签'] = sum_.loc[: , '基金总规模行业排名标签'] + sum_.loc[: , 'temp']
sum_ = sum_.drop(['temp','fund_value'], axis = 1)
sum_ = sum_.reset_index(['company','start_up_date'])

sum_.to_excel(path+r'基金总规模行业排名标签.xlsx')
################################################2、基金公司非货币基金总规模行业排名标签#################################################################
non_monetary = data[data['六大类型'] != '货币类']
sum_non_monetary = non_monetary.groupby(by = ['company','start_up_date'])['fund_value'].sum()
sum_non_monetary = pd.DataFrame(sum_non_monetary)
amount = sum_non_monetary['fund_value']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_non_monetary.loc[sum_non_monetary['fund_value'] > high, '基金总规模行业排名标签'] = '高-'
sum_non_monetary.loc[(sum_non_monetary['fund_value'] <= high) & (sum_non_monetary['fund_value'] > mid_high), '基金总规模行业排名标签'] = '中高-'
sum_non_monetary.loc[(sum_non_monetary['fund_value'] <= mid_high) & (sum_non_monetary['fund_value'] > middle), '基金总规模行业排名标签'] = '中-'
sum_non_monetary.loc[(sum_non_monetary['fund_value'] <= middle) & (sum_non_monetary['fund_value'] > mid_low), '基金总规模行业排名标签'] = '中低-'
sum_non_monetary.loc[sum_non_monetary['fund_value'] <= mid_low, '基金总规模行业排名标签'] = '低-'

#给基金公司排名并加序号
rank = pd.DataFrame(sum_non_monetary['fund_value']).rank()
rank.columns = ['temp']
sum_non_monetary = pd.merge( sum_non_monetary, rank, on = ['company','start_up_date'])

#将序号全部变为int格式,再转为字符串
list_rank = []
for num in sum_non_monetary['temp']:
    num = int(num)
    num = str(num)
    list_rank.append(num)
sum_non_monetary['temp'] = list_rank

#在标签后加上基金规模行业排名
sum_non_monetary.loc[: , '基金总规模行业排名标签'] = sum_non_monetary.loc[: , '基金总规模行业排名标签'] + sum_non_monetary.loc[: , 'temp']
sum_non_monetary = sum_non_monetary.drop(['temp','fund_value'], axis = 1)
sum_non_monetary = sum_non_monetary.reset_index(['company','start_up_date'])

sum_non_monetary.to_excel(path+r'基金公司非货币基金总规模行业排名标签.xlsx')
################################################3、基金公司偏股类基金总规模行业排名标签#################################################################
stock = data[data['六大类型'] == '偏股类']
sum_stock = stock.groupby(by = ['company','start_up_date'])['fund_value'].sum()
sum_stock = pd.DataFrame(sum_stock)
amount = sum_stock['fund_value']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_stock.loc[sum_stock['fund_value'] > high, '基金总规模行业排名标签'] = '高-'
sum_stock.loc[(sum_stock['fund_value'] <= high) & (sum_stock['fund_value'] > mid_high), '基金总规模行业排名标签'] = '中高-'
sum_stock.loc[(sum_stock['fund_value'] <= mid_high) & (sum_stock['fund_value'] > middle), '基金总规模行业排名标签'] = '中-'
sum_stock.loc[(sum_stock['fund_value'] <= middle) & (sum_stock['fund_value'] > mid_low), '基金总规模行业排名标签'] = '中低-'
sum_stock.loc[sum_stock['fund_value'] <= mid_low, '基金总规模行业排名标签'] = '低-'

#给基金公司排名并加序号
rank = pd.DataFrame(sum_stock['fund_value']).rank()
rank.columns = ['temp']
sum_stock = pd.merge( sum_stock, rank, on = ['company','start_up_date'])

#将序号全部变为int格式,再转为字符串
list_rank = []
for num in sum_stock['temp']:
    num = int(num)
    num = str(num)
    list_rank.append(num)
sum_stock['temp'] = list_rank

#在标签后加上基金规模行业排名
sum_stock.loc[: , '基金总规模行业排名标签'] = sum_stock.loc[: , '基金总规模行业排名标签'] + sum_stock.loc[: , 'temp']
sum_stock = sum_stock.drop(['temp','fund_value'], axis = 1)
sum_stock = sum_stock.reset_index(['company','start_up_date'])

sum_stock.to_excel(path+r'基金公司偏股类基金总规模行业排名标签.xlsx')
res_sum_stock = sum_stock

################################################4、基金公司偏债类基金总规模行业排名标签#################################################################
bond = data[data['六大类型'] == '偏债类']
sum_bond = bond.groupby(by = ['company','start_up_date'])['fund_value'].sum()
sum_bond = pd.DataFrame(sum_bond)
amount = sum_bond['fund_value']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_bond.loc[sum_bond['fund_value'] > high, '基金总规模行业排名标签'] = '高-'
sum_bond.loc[(sum_bond['fund_value'] <= high) & (sum_bond['fund_value'] > mid_high), '基金总规模行业排名标签'] = '中高-'
sum_bond.loc[(sum_bond['fund_value'] <= mid_high) & (sum_bond['fund_value'] > middle), '基金总规模行业排名标签'] = '中-'
sum_bond.loc[(sum_bond['fund_value'] <= middle) & (sum_bond['fund_value'] > mid_low), '基金总规模行业排名标签'] = '中低-'
sum_bond.loc[sum_bond['fund_value'] <= mid_low, '基金总规模行业排名标签'] = '低-'

#给基金公司排名并加序号
rank = pd.DataFrame(sum_bond['fund_value']).rank()
rank.columns = ['temp']
sum_bond = pd.merge( sum_bond, rank, on = ['company','start_up_date'])

#将序号全部变为int格式,再转为字符串
list_rank = []
for num in sum_bond['temp']:
    num = int(num)
    num = str(num)
    list_rank.append(num)
sum_bond['temp'] = list_rank

#在标签后加上基金规模行业排名
sum_bond.loc[: , '基金总规模行业排名标签'] = sum_bond.loc[: , '基金总规模行业排名标签'] + sum_bond.loc[: , 'temp']
sum_bond = sum_bond.drop(['temp','fund_value'], axis = 1)
sum_bond = sum_bond.reset_index(['company','start_up_date'])
sum_bond = pd.DataFrame(sum_bond)

sum_bond.to_excel(path+r'基金公司偏债类基金总规模行业排名标签.xlsx')
res_sum_bond = sum_bond
################################################5、基金公司货币基金总规模行业排名标签#################################################################
monetary = data[data['六大类型'] == '货币类']
sum_monetary = monetary.groupby(by = ['company','start_up_date'])['fund_value'].sum()
sum_monetary = pd.DataFrame(sum_monetary)
amount = sum_monetary['fund_value']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_monetary.loc[sum_monetary['fund_value'] > high, '基金总规模行业排名标签'] = '高-'
sum_monetary.loc[(sum_monetary['fund_value'] <= high) & (sum_monetary['fund_value'] > mid_high), '基金总规模行业排名标签'] = '中高-'
sum_monetary.loc[(sum_monetary['fund_value'] <= mid_high) & (sum_monetary['fund_value'] > middle), '基金总规模行业排名标签'] = '中-'
sum_monetary.loc[(sum_monetary['fund_value'] <= middle) & (sum_monetary['fund_value'] > mid_low), '基金总规模行业排名标签'] = '中低-'
sum_monetary.loc[sum_monetary['fund_value'] <= mid_low, '基金总规模行业排名标签'] = '低-'

#给基金公司排名并加序号
rank = pd.DataFrame(sum_monetary['fund_value']).rank()
rank.columns = ['temp']
sum_monetary = pd.merge( sum_monetary, rank, on = ['company','start_up_date'])

#将序号全部变为int格式,再转为字符串
list_rank = []
for num in sum_monetary['temp']:
    num = int(num)
    num = str(num)
    list_rank.append(num)
sum_monetary['temp'] = list_rank

#在标签后加上基金规模行业排名
sum_monetary.loc[: , '基金总规模行业排名标签'] = sum_monetary.loc[: , '基金总规模行业排名标签'] + sum_monetary.loc[: , 'temp']
sum_monetary = sum_monetary.drop(['temp','fund_value'], axis = 1)
sum_monetary = sum_monetary.reset_index(['company','start_up_date'])
sum_monetary.to_excel(path+r'基金公司货币基金总规模行业排名标签.xlsx')
res_sum_monetary = sum_monetary

################################################6、基金公司QDII基金总规模行业排名标签#################################################################
QDII = data[data['六大类型'] == 'QDII']
sum_QDII = QDII.groupby(by = ['company','start_up_date'])['fund_value'].sum()
sum_QDII = pd.DataFrame(sum_QDII)
amount = sum_QDII['fund_value']
amount = np.array( amount)

#按照分类标准取分位数
high = np.percentile(amount, 90)
mid_high = np.percentile(amount, 65)
middle = np.percentile(amount, 35)
mid_low = np.percentile(amount, 10)

#根据分位数排名给基金公司打上‘高’、‘中高’、‘中’、‘中低’、‘低’的标签
sum_QDII.loc[sum_QDII['fund_value'] > high, '基金总规模行业排名标签'] = '高-'
sum_QDII.loc[(sum_QDII['fund_value'] <= high) & (sum_QDII['fund_value'] > mid_high), '基金总规模行业排名标签'] = '中高-'
sum_QDII.loc[(sum_QDII['fund_value'] <= mid_high) & (sum_QDII['fund_value'] > middle), '基金总规模行业排名标签'] = '中-'
sum_QDII.loc[(sum_QDII['fund_value'] <= middle) & (sum_QDII['fund_value'] > mid_low), '基金总规模行业排名标签'] = '中低-'
sum_QDII.loc[sum_QDII['fund_value'] <= mid_low, '基金总规模行业排名标签'] = '低-'

#给基金公司排名并加序号
rank = pd.DataFrame(sum_QDII['fund_value']).rank()
rank.columns = ['temp']
sum_QDII = pd.merge( sum_QDII, rank, on = ['company','start_up_date'])

#将序号全部变为int格式,再转为字符串
list_rank = []
for num in sum_QDII['temp']:
    num = int(num)
    num = str(num)
    list_rank.append(num)
sum_QDII['temp'] = list_rank

#在标签后加上基金规模行业排名
sum_QDII.loc[: , '基金总规模行业排名标签'] = sum_QDII.loc[: , '基金总规模行业排名标签'] + sum_QDII.loc[: , 'temp']
sum_QDII = sum_QDII.drop(['temp','fund_value'], axis = 1)
sum_QDII = sum_QDII.reset_index(['company','start_up_date'])
sum_QDII.to_excel(path+r'基金公司QDII基金总规模行业排名标签.xlsx')
res_sum_QDII = sum_QDII
##################################################7、主打产品类型标签（规模维度）###############################################
##此处需要本地地址！！！
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
data = data[['基金代码', '一级分类', '二级分类']]
data = pd.merge( data, fund_value, on = '基金代码', how = 'right')

sum_ = data.groupby(by = ['一级分类','company','start_up_date'])['fund_value'].sum()
sum_ = pd.DataFrame(sum_)
sum_.columns = ['sum_value']
sum_ = sum_.sort_values( by = ['一级分类', 'sum_value'],ascending = False)
sum_ = sum_.reset_index([ '一级分类', 'company', 'start_up_date'])

category = sum_.drop_duplicates( ['一级分类'])
category = category['一级分类']
company_class1 = pd.DataFrame(columns = ('company','主打产品类型标签（规模维度一级分类）'))
firm = sum_.drop_duplicates(['company'])
company_class1['company'] = firm['company']
company_class1 = company_class1.reset_index( drop = True )
for i in category:
    sum_cat = sum_[sum_['一级分类'] == i]
    sum_cat = sum_cat.reset_index( drop = True)
    good_at = sum_cat.head(1)
    good_at['temp'] = '主打' + good_at['一级分类'] + '基金'
    selected = company_class1[company_class1['company'] == str(good_at['company'][0])]['主打产品类型标签（规模维度一级分类）']
    selected = selected.reset_index(drop = True)
    if type(selected[0]) != type(''):
        company_class1.loc[company_class1['company'] == str(good_at['company'][0]), '主打产品类型标签（规模维度一级分类）'] = str(good_at['temp'][0])
    else:
        company_class1.loc[company_class1['company'] == str(good_at['company'][0]), '主打产品类型标签（规模维度一级分类）'] = company_class1.loc[company_class1['company'] == str(good_at['company'][0]), '主打产品类型标签（规模维度一级分类）'] + ',' + str(good_at['temp'][0])
#company_class1即为最终一级标签

sum_ = data.groupby(by = ['二级分类','company','start_up_date'])['fund_value'].sum()
sum_ = pd.DataFrame(sum_)
sum_.columns = ['sum_value']
sum_ = sum_.sort_values( by = ['二级分类', 'sum_value'],ascending = False)
sum_ = sum_.reset_index([ '二级分类', 'company', 'start_up_date'])

category = sum_.drop_duplicates( ['二级分类'])
category = category['二级分类']
company_class2 = pd.DataFrame(columns = ('company','主打产品类型标签（规模维度二级分类）'))
firm = sum_.drop_duplicates(['company'])
company_class2['company'] = firm['company']
company_class2 = company_class2.reset_index( drop = True )
for i in category:
    sum_cat = sum_[sum_['二级分类'] == i]
    sum_cat = sum_cat.reset_index( drop = True)
    good_at = sum_cat.head(1)
    good_at['temp'] = '主打' + good_at['二级分类'] + '基金'
    selected = company_class2[company_class2['company'] == str(good_at['company'][0])]['主打产品类型标签（规模维度二级分类）']
    selected = selected.reset_index(drop = True)
    if type(selected[0]) != type(''):
        company_class2.loc[company_class2['company'] == str(good_at['company'][0]), '主打产品类型标签（规模维度二级分类）'] = str(good_at['temp'][0])
    else:
        company_class2.loc[company_class2['company'] == str(good_at['company'][0]), '主打产品类型标签（规模维度二级分类）'] = company_class2.loc[company_class2['company'] == str(good_at['company'][0]), '主打产品类型标签（规模维度二级分类）'] + ',' + str(good_at['temp'][0])
#company_class2即为最终二级标签
company_class2.to_excel(path+r'主打产品类型标签（规模维度二级分类）.xlsx')
res_company_class2 = company_class2
#打产品线丰富或单一判断条件为：基金总数小于30或存在四中类型基金的其中任意两种单个个数均小于等于2







##############################################################擅长产品类型标签#########################################################################

#定义一个找到距离某日期最近的周五的日期,由于wind中计算周频的数据都用的是每周五的数据计算，于是需要根据指定日期往前寻找最近一个周五的日期
def get_friday(start_date):
    """

    参数
    --------
    start_date:日期,字符格式,如'20190110'

    返回值
    --------
    返回一个具体的日期，返回格式为字符格式，如'20190105'。

    示例
    --------
    >>>a = get_friday('20190110')
    >>>a
    '20190104'
     """
    while True:  # 这是一个死循环，只有当break的时候跳出循环，如果不对就会一直循环下去。在本例中，是周五时跳出循环，不是周五的时候会前寻找
        # 然后又进行判断是否是周五，直到是交易日跳出循环，返回start_date
        if pd.to_datetime(start_date).weekday() == 4:
            break
        else:
            # print(start_date, '日期非星期五，前推到最近一交易日')
            temp_date = pd.to_datetime(start_date).date()  # 转换成datetime里面的date格式，如果没有后面的.date那么就是包含具体小时分钟的
            start_date = (temp_date + relativedelta(days=-1)).strftime('%Y%m%d')  # 前一天的日期后又变为'年月日'的格式
    return start_date

#找出离start_date最近的交易日
def if_trade(start_date,trade_dates):
    """
     生成指定日期距离最近的交易日期
     功能        --------        生成指定日期距离最近的交易日期
     参数        --------        输入日期，格式为字符格式，如'20171220'
     返回值        --------        返回一个具体的日期，返回格式为字符格式，如'20181011'。如输入日期当天为交易日期，        则返回当天；否则往前遍历至最近交易日。
     参看        --------        无关联函数。        需要在函数外将交易日期列表存好，需要用trade_dates变量存交易日期，数据格式为'20180120',        只需要交易日一列即可，trade_dates为dataframe类型，列名需要为'交易日期'。
     示例        --------        >>>a = if_trade('20181229',trade_dates)        >>>a        '20181228'        """
    while True:  # 这是一个死循环，只有当break的时候跳出循环，如果不对就会一直循环下去。在本例中，是交易日时跳出循环，不是交易日的时候会出现indexerror
                # 然后又进行判断是否是交易日，直到是交易日跳出循环，返回start_date
        try:
            start_date = trade_dates.loc[trade_dates['announce_date'] == start_date].values[0][0]#这里的trade_dates指的是取出来的带昨日收盘价的数据中的日期数据
            break
        except IndexError:
            # print(start_date, '日期非交易日，前推到最近一交易日')
            temp_date = pd.to_datetime(start_date).date()  # 转换成datetime里面的date格式，如果没有后面的.date那么就是包含具体小时分钟的
            start_date = (temp_date + relativedelta(days=-1)).strftime('%Y%m%d')  # 前一天的日期后又变为'年月日'的格式
    return start_date

#trade_dates = {'交易日期':['20170414','20170417','20170418','20170419']}
#trade_dates = pd.DataFrame(trade_dates)

def if_trade_back(start_date, trade_dates):
    """
     生成指定日期距离最近的交易日期
     功能        --------        生成指定日期距离最近的交易日期
     参数        --------        输入日期，格式为字符格式，如'20171220'
     返回值        --------        返回一个具体的日期，返回格式为字符格式，如'20181011'。如输入日期当天为交易日期，        则返回当天；否则往后遍历至最近交易日。
     参看        --------        无关联函数。        需要在函数外将交易日期列表存好，需要用trade_dates变量存交易日期，数据格式为'20180120',        只需要交易日一列即可，trade_dates为dataframe类型，列名需要为'交易日期'。
     示例        --------        >>>a = if_trade('20181229',trade_dates)        >>>a        '20181228'        """
    while True:  # 这是一个死循环，只有当break的时候跳出循环，如果不对就会一直循环下去。在本例中，是交易日时跳出循环，不是交易日的时候会出现indexerror
                # 然后又进行判断是否是交易日，直到是交易日跳出循环，返回start_date
        try:
            start_date = trade_dates.loc[trade_dates['announce_date'] == start_date].values[0][0]#这里的trade_dates指的是取出来的带今日收盘价的数据中的日期数据
            break
        except IndexError:
            # print(start_date, '日期非交易日，后推到最近一交易日')
            temp_date = pd.to_datetime(start_date).date()  # 转换成datetime里面的date格式，如果没有后面的.date那么就是包含具体小时分钟的
            start_date = (temp_date + relativedelta(days = 1)).strftime('%Y%m%d')  # 后一天的日期后又变为'年月日'的格式
    return start_date

def date_gen( trade_dates, days=None, months=None, years=None, end=None):
    """生成指定日期距离最近的交易日期
        功能
        --------
        生成指定日期之前几年/月/日距离最近的交易日期

        参数
        --------
        trade_dates为一个DataFrame,其中包含announce_date列，这一列的日期全部为交易日
        days:需要往前多少个日历日，格式为int，可以为具体数字，也可为变量
        months:需要往前多少个月份，格式为int，可以为具体数字，也可为变量
        years:需要往前多少个年份，格式为int，可以为具体数字，也可为变量
        end:截止日期，格式为'20171220'，以该截止日期往前距离XX日、XX月、XX年后最近的交易日期
        参数需要写全，如days=3。days、months、years必须输入一个，end也是必要参数

        返回值
        --------
        返回一个具体的日期，返回格式为字符格式，如'20181011'。

        参看
        --------
        if_trade(start_date)：关联函数。

        示例
        --------
        >>>a = date_gen(days = 3,end = '20181220')
        >>>a
        '20181217'

        """
    def none(par):
        if not par:  # 意思是如果par为None时
            par = 0
        return par

    [days, months, years] = [none(days), none(months), none(years)]
    end = pd.to_datetime(end)
    start_date = (end - relativedelta(days=days, months=months, years=years)).strftime('%Y%m%d')
    start_date = if_trade_back(start_date, trade_dates)  # relativedelta表示时间的移动，之前是以移动一天，现在是移动一个月或者一年之类的
    return start_date

#today = time.strftime('%Y%m%d')
#year_lag = 2
#post = date_gen( years = year_lag, end = today)

def get_market( type, market_all):
    """
    功能
    --------
    提取某个二级分类的市场组合近三年的周收益率数据，

    参数
    --------
    type:二级分类，字符格式，如'股票型'
    market_all:DataFrame格式，其中index为日期，columns为各指数，value为指数的对应日期的收益率

    返回值
    --------
    返回一个pd.Series，name = '市场组合收益率',index为日期，为市场组合周收益率数据，按日期升序排列

    参看
    --------
    无关联函数

    示例
    --------
    #>>>get_market('股票型')
日期
20151030         NaN
20151106    0.066762
20151113   -0.004076
20151120    0.014694
20151127   -0.056840
20151204    0.029192
     """
    market_return = market_all
    if type =='股票型':
        market_type  = market_all['中证800指数']
    elif type == '激进配置型':
        market_type = market_all['中证800指数']*0.8+market_all['中证国债指数']*0.2
    elif type =='标准配置型':
        market_type = market_all['中证800指数']*0.6+market_all['中证国债指数']*0.4
    elif type == '保守配置型':
        market_type = market_all['中证800指数'] * 0.2 + market_all['中证国债指数'] * 0.8
    elif type == '灵活配置型':
        market_type = market_all['中证800指数'] * 0.5 + market_all['中证国债指数'] * 0.5
    elif type == '沪港深股票型':
        market_type = market_all['中证800指数'] * 0.45 + market_all['中证国债指数'] * 0.1+market_all['恒生指数']*0.45
    elif type == '沪港深配置型':
        market_type = market_all['中证800指数'] * 0.35 + market_all['中证国债指数'] * 0.3 + market_all['恒生指数'] * 0.35
    elif type =='纯债型':
        market_type = market_all['中证综合债指数']
    elif type == '普通债券型':
        market_type = market_all['中证综合债指数']*0.9+market_all['中证800指数'] * 0.1
    elif type == '激进债券型':
        market_type = market_all['中证综合债指数']*0.8+market_all['中证800指数'] * 0.2
    elif type =='短债型':
        market_type = market_all['中证短债指数']
    elif type =='可转债型':
        market_type = market_all['中证可转债债券型基金指数']
    elif type =='环球股票':
        market_type = market_all['MSCI收益率']
    #market_type.name = '市场组合收益率'
    market_return['市场组合收益率'] = market_type
    market_return = market_return[['announce_date','市场组合收益率']]
    return  market_return

def get_yeild_index(x):
    x = x.sort_values(by = ['index_code', 'index_name', 'announce_date'],ascending = True)
    x['收益率'] = x['收盘价'].pct_change()
    del x['收盘价']
    return x

def get_yeild_fund(x):
    x = x.sort_values(by = ['基金代码', 'announce_date'],ascending = True)
    x['收益率'] = x['收盘价'].pct_change()
    del x['收盘价']
    return x

#net_friday =  net_full[net_full.日期.isin(dates2)]
#net_friday = net_friday.groupby('基金代码').apply(lambda x : get_yeild(x))
#net_friday_group = net_friday.groupby('基金代码')

#取某基金的周收益率序列
def get_weekly_yeild(data,x):
    dates = data['announce_date'].drop_duplicates()
    data = data.groupby(by = '基金代码')
    try:
        df = data.get_group(x)
        df.index = df['announce_date']
        #del df['announce_date']
        #del df['基金代码']
        #df.columns = [x]
    except KeyError:
        df = pd.DataFrame(index=dates)
        df[x] = np.nan
    return df

#已结束IR的计算！！！！！！！！！！

def IR(net,year_lag,end_date,index_return,rank):
    """
    功能
    --------
    计算基金近几年的信息比率

    参数
    --------
    net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
    year_lag:近几年，格式为int
    end_date:观测期结束日（通常为今天或某报告期的最后一天），格式为str，例：'20170101'
    index_data:DataFrame格式，index为日期，columns为对应指数，values为对应日期对应指数的收益率

    返回值
    --------
    返回一个dataframe,是在net上添加计算结果信息比率列和信息比率标签列

    参看
    --------
    get_market():取某个二级分类的基准组合收益率数据

    示例
    --------
 #   >>>net = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
 #   >>>IR(net,1).head()
    基金代码      基金简称   二级分类       成立年限    1年信息比率  1年信息比率标签
0  000001      华夏成长  标准配置型  17.131507 -0.558956       NaN
1  000003  中海可转换债券A   可转债型   5.871233 -1.545706       NaN
2  000005    嘉实增强信用  普通债券型   5.904110  1.795728  高-1年信息比率
3  000011    华夏大盘精选    股票型  14.482192  0.783975  中-1年信息比率
4  000014      华夏聚利  普通债券型   5.873973 -1.432578       NaN
         """
    end = get_friday(end_date)
    start = date_gen( fund_data,years=year_lag, end=end_date)
    kind = net[rank +'级分类'].drop_duplicates()
    col = str(year_lag)+'年信息比率'
    col2 = net.columns
    net_return = pd.DataFrame(columns = col2)
    for i in kind:
        market = get_market(i,index_return)
        cate = net[net[rank + '级分类'] == i]
        net_kind = cate['基金代码']
        alpha = []
        for  j in net_kind:
            dates = cate[cate['基金代码'] == j]
            dates = dates.drop(dates.index[0])
            #if dates['distance'][0] < year_lag:
            #    IR = 1
            #    #IR=np.nan
            #    print(IR)
            #else:
            dates = pd.merge( dates, market, on = 'announce_date')
            #dates.columns = ['基金收益率', '市场组合收益率']
            dates['差额'] = dates['收益率'] - dates['市场组合收益率']
            tracking_error = dates['差额'].std()
            dates['基金净值'] = dates['收益率']+1
            dates['市场组合净值'] = dates['市场组合收益率']+1
            #product()为连乘，可以将一列数的值乘起来，当其中有空值时，默认该空值为1
            R_p = dates['基金净值'].product()
            #R_p = dates['基金净值'].product()/ dates.iloc[0,-1]
            R_m = dates['市场组合净值'].product()
            #R_m = dates['市场组合净值'].product() / dates.iloc[0,-1]
            IR = (R_p - R_m) / (tracking_error * np.sqrt(52))
            alpha.append(IR)
        cate[col] = alpha
        net_return = pd.concat( [net_return, cate], ignore_index = True)
        #alpha1 = pd.Series(alpha).dropna()
        #alpha1 = alpha1[alpha1>0]
        #try:
        #    high = np.percentile(alpha1, 80)
        #    low = np.percentile(alpha1, 20)
        #    net.loc[(net.二级分类 == i) & (net[col] > high), col +'标签'] = '高-' + str(year_lag) + '年信息比率'
        #    net.loc[(net.二级分类 == i) & (0 < net[col]) & (net[col] < low), col +'标签' ] = '低-' + str(year_lag) + '年信息比率'
        #    net.loc[(net.二级分类 == i) & (low <= net[col]) & (net[col] <= high), col +'标签'] = '中-' + str(
        #        year_lag) + '年信息比率'
        #except IndexError:
        #    pass
    net_return = net_return.drop_duplicates('基金代码')
    #下面进行计算按照基金规模的加权平均IR
    #首先按照基金公司及二级分类对基金净值进行加和
    #sum_value = net_return.groupby(by=['company', '二级分类'])['基金平均规模'].sum()
    #sum_value = pd.DataFrame(sum_value)
    #sum_value.columns = ['分类基金规模']
    #net_return = pd.merge( net_return, sum_value, on = ['company','二级分类'], how = 'left')
    #计算加权平均IR
    grouped = net_return.groupby( by = [ rank + '级分类', 'company', 'start_up_date'])
    weighted = lambda x: np.average(x[col], weights=x['基金平均规模'])
    weighted_IR = grouped.apply(weighted)
    weighted_IR = pd.DataFrame(weighted_IR)
    weighted_IR.columns = ['加权后IR']
    #weighted_IR = weighted_IR.sort_values(by=['start_up_date'], ascending=False)
    #weighted_IR = weighted_IR.drop_duplicates('加权后IR')
    weighted_IR = weighted_IR.sort_values( by=[ rank + '级分类', '加权后IR'], ascending= False)
    return weighted_IR



def tracking_error(net,year_lag,end_date,index_return, rank):
    """
    功能
    --------
    计算基金近几年的tracking_error

    参数
    --------
    net:需要进行计算的基金列表，dataframe格式，其中有成立年限列
    year_lag:近几年，格式为int
    end_date:观测期结束日（通常为今天或某报告期的最后一天），格式为str，例：'20170101'
    index_data:DataFrame格式，index为日期，columns为对应指数，values为对应日期对应指数的收益率

    返回值
    --------
    返回一个dataframe,是在net上添加计算结果信息比率列和信息比率标签列

    参看
    --------
    get_market():取某个二级分类的基准组合收益率数据

    示例
    --------
 #   >>>net = pd.merge(net_act,net0[['基金代码','成立年限']],on='基金代码',how = 'left').sort_values(by = '基金代码',ascending = True)
 #   >>>IR(net,1).head()
    基金代码      基金简称   二级分类       成立年限    1年信息比率  1年信息比率标签
0  000001      华夏成长  标准配置型  17.131507 -0.558956       NaN
1  000003  中海可转换债券A   可转债型   5.871233 -1.545706       NaN
2  000005    嘉实增强信用  普通债券型   5.904110  1.795728  高-1年信息比率
3  000011    华夏大盘精选    股票型  14.482192  0.783975  中-1年信息比率
4  000014      华夏聚利  普通债券型   5.873973 -1.432578       NaN
         """
    end = get_friday(end_date)
    start = date_gen( fund_data,years=year_lag, end=end_date)
    kind = net[rank + '级分类'].drop_duplicates()
    col = str(year_lag)+'年跟踪误差'
    col2 = net.columns
    net_return = pd.DataFrame(columns = col2)
    for i in kind:
        market = get_market(i,index_return)
        cate = net[net[rank + '级分类'] == i]
        net_kind = cate['基金代码']
        alpha = []
        for  j in net_kind:
            dates = cate[cate['基金代码'] == j]
            dates = dates.drop(dates.index[0])
            #if dates['distance'][0] < year_lag:
            #    IR = 1
            #    #IR=np.nan
            #    print(IR)
            #else:
            dates = pd.merge( dates, market, on = 'announce_date', how = 'left')
            #dates.columns = ['基金收益率', '市场组合收益率']
            dates['差额'] = dates['收益率'] - dates['市场组合收益率']
            tracking_error = dates['差额'].std()
            tracking_error = tracking_error * np.sqrt(52)
            alpha.append(tracking_error)
        cate[col] = alpha
        net_return = pd.concat( [net_return, cate], ignore_index = True)
        #alpha1 = pd.Series(alpha).dropna()
        #alpha1 = alpha1[alpha1>0]
        #try:
        #    high = np.percentile(alpha1, 80)
        #    low = np.percentile(alpha1, 20)
        #    net.loc[(net.二级分类 == i) & (net[col] > high), col +'标签'] = '高-' + str(year_lag) + '年信息比率'
        #    net.loc[(net.二级分类 == i) & (0 < net[col]) & (net[col] < low), col +'标签' ] = '低-' + str(year_lag) + '年信息比率'
        #    net.loc[(net.二级分类 == i) & (low <= net[col]) & (net[col] <= high), col +'标签'] = '中-' + str(
        #        year_lag) + '年信息比率'
        #except IndexError:
        #    pass
    net_return = net_return.drop_duplicates('基金代码')
    #下面进行计算按照基金规模的加权平均IR
    #首先按照基金公司及二级分类对基金净值进行加和
    #sum_value = net_return.groupby(by=['company', '二级分类'])['基金平均规模'].sum()
    #sum_value = pd.DataFrame(sum_value)
    #sum_value.columns = ['分类基金规模']
    #net_return = pd.merge( net_return, sum_value, on = ['company','二级分类'], how = 'left')
    #计算加权平均IR
    grouped = net_return.groupby( by = [ rank + '级分类', 'company', 'start_up_date'])
    weighted = lambda x: np.average(x[col], weights=x['基金平均规模'])
    weighted_tracking_error = grouped.apply(weighted)
    weighted_tracking_error = pd.DataFrame(weighted_tracking_error)
    weighted_tracking_error.columns = ['加权后tracking error']
    #weighted_tracking_error = weighted_IR.sort_values(by=['start_up_date'], ascending=False)
    #weighted_tracking_error = weighted_IR.drop_duplicates('company')
    return weighted_tracking_error


#首先获取所需指数近三年数据
#中证800指数：000906；中证国债：h11006；恒生指数：HSI；中证综合债指数：h11009；中证短债：h11015；中证可转债：930898；MSCI全球指数：892400(由于数据库中暂无MSCI全球指数，因此暂不涉及到与全球配置相关的基金)
#下表为沪深交易所指数数据（不含MSCI及恒生）
#首先取出今日日期
now = datetime.today()
now = now.strftime('%Y%m%d')
now = pd.to_datetime(now).date()
#转变为三年前日期（若要获取前两年数据则设置years = 2）
date_3years_before = (now - relativedelta(years = 3)).strftime('%Y%m%d')
now = now.strftime('%Y%m%d')
sql = '''
        SELECT
        DISTINCT c.F1_1289,b.F16_1090,a.F2_1120,a.F4_1120
        FROM
        (SELECT
        F1_1120,F2_1120,F4_1120
        FROM
        wind.TB_OBJECT_1120
        WHERE F2_1120 > '%(date_3years_before)s' AND F2_1120 < '%(now)s')a
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM
        wind.TB_OBJECT_1090
        WHERE F16_1090 = '000906' OR F16_1090 = 'h11006' OR F16_1090 = 'HSI' OR F16_1090 = 'h11009' OR F16_1090 = 'h11015' OR F16_1090 = '930898' OR F16_1090 = '892400')b
        ON a.F1_1120 = b.F2_1090
        JOIN
        (SELECT
        F2_1289,F1_1289
        FROM
        wind.TB_OBJECT_1289 )c
        ON c.F2_1289 = b.F2_1090

        '''%{'now':now,'date_3years_before':date_3years_before}
cu = fund_db.cursor()
index_data1 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['index_name', 'index_code','announce_date','收盘价'])
index_data1 = index_data1.sort_values(by = ['index_name','index_code','announce_date'])
index_data1 = index_data1.reset_index(drop = True)

#下面将数据转换为周频率数据
m = index_data1['announce_date']
n = pd.DataFrame(m)
n.columns = ['announce_date']
end_date = sorted(m)[-1]
end = get_friday(end_date)
dates2 = []
length = int(170)
for i in range(length):
    temp = date_gen(n, days=7 * i, end=end)
    dates2.append(temp)
c = index_data1[index_data1['announce_date'].isin(dates2)]

#将收盘价数据转变为收益率数据
index_code = index_data1['index_code']
index_code = index_code.drop_duplicates()

dd = pd.DataFrame(columns = ('index_name', 'index_code','announce_date','收益率'))
for i in index_code:
    a = c[c['index_code'] == i]
    a = get_yeild_index(a)
    dd = pd.concat([dd,a], ignore_index = True)

index_data1 = dd

#下表可取出恒生指数数据
sql = '''
        SELECT
        DISTINCT c.F1_1289,b.F16_1090,a.F3_1288,a.F2_1288
        FROM
        (SELECT
        F1_1288,F2_1288,F3_1288
        FROM
        wind.TB_OBJECT_1288
        WHERE F3_1288 > '%(date_3years_before)s' AND F3_1288 < '%(now)s')a
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM
        wind.TB_OBJECT_1090
        WHERE F16_1090 = 'HSI')b
        ON a.F1_1288 = b.F2_1090
        JOIN
        (SELECT
        F2_1289,F1_1289
        FROM
        wind.TB_OBJECT_1289 )c
        ON c.F2_1289 = b.F2_1090

        '''%{'now':now,'date_3years_before':date_3years_before}
cu = fund_db.cursor()
index_data2 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['index_name', 'index_code','announce_date','收盘价'])
index_data2 = index_data2.sort_values(by = ['index_name','index_code','announce_date'])
index_data2 = index_data2.reset_index(drop = True)

#由于除此表外，其他表的收盘价均为昨收盘价，因此需将此表日期整体向后推一个交易日
dd = pd.DataFrame(columns = ('index_name', 'index_code','announce_date','收益率'))
index_code = index_data2['index_code']
index_code = index_code.drop_duplicates()

#推后时间外，改变为周频率数据（只留每周周五的数据），并将收盘价数据均转换为收益率数据
for i in index_code:
    a = index_data2[index_data2['index_code'] == i]
    a = a.drop(0)
    m = []
    b = a['announce_date']
    b = pd.DataFrame(b)
    for j in a['announce_date']:
        j = if_trade_back(j,b)
        m.append(j)
    a['announce_date'] = m
    n = pd.DataFrame(m)
    n.columns = ['announce_date']
    end_date = sorted(m)[-1]
    end = get_friday(end_date)
    dates2 = []
    length = int(170)
    for i in range(length):
        temp = date_gen(n, days = 7*i,end = end)
        dates2.append(temp)
    c = a[a['announce_date'].isin(dates2)]
    c = get_yeild_index(c)
    dd = pd.concat([dd,c], ignore_index = True)

dd.columns = ['index_name', 'index_code','announce_date','收益率']
index_data2 = dd
date1 = index_data1['announce_date']
index_data2 = index_data2[index_data2['announce_date'].isin(date1)]
index_data2 = index_data2.reset_index(drop = True)

#将沪深交易所指数数据与恒生指数数据合并为全部所需指数的总表
index_data = pd.concat([index_data1,index_data2], ignore_index = True)

##下面转换格式，使index_data转换为index为时间，columns为指数标签，value为收益率
#转换之后直接取列标签得到的就是该指数的收益率数值
index_return = index_data[['index_name','announce_date','收益率']]
index_return = index_return.pivot_table(index = 'announce_date', columns = 'index_name', values = '收益率')
index_return = index_return.reset_index('announce_date')

#year_lag = 2
#today = time.strftime('%Y%m%d')


#下面需要得到基金公司的每只现有基金的成立年限以及基金净值收盘价，以计算收益率
#！！！！！！！周一寻找基金净值以计算基金净值
#筛选出基金公司名、公司成立日、基金产品代码、产品开始时间、产品结束时间
#需要计算多个截止日期的平均规模，但因为memory error，只取最近一个报告期的规模作为平均规模，此处需要优化！！！！！
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,c.F16_1090,c.F21_1099,c.F3_1104,c.F2_1120,c.F4_1120
        FROM
        (SELECT
        b.OB_OBJECT_NAME_1018,b.F35_1018,a.F21_1099,f.F2_1120,f.F4_1120,e.F16_1090,e.F2_1090,g.F3_1104
        FROM
        (SELECT
        F12_1099,F1_1099,F21_1099,F23_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F21_1099 < '%(date_3years_before)s' AND F23_1099 IS NULL) a
        JOIN
        (SELECT
        F15_1104,F14_1104,F3_1104
        FROM wind.TB_OBJECT_1104
        WHERE F14_1104 = '20181231')g
        ON g.F15_1104 = a.F1_1099
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON a.F1_1099 = e.F2_1090
        JOIN
        (SELECT F1_1120,F2_1120,F4_1120
        FROM wind.TB_OBJECT_1120
        WHERE F2_1120 > '%(date_3years_before)s' AND F2_1120 < '%(now)s')f
        ON f.F1_1120 = e.F2_1090
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c

        '''%{'now':now,'date_3years_before':date_3years_before}

cu = fund_db.cursor()
fund_data = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date','基金代码','start_date','基金平均规模','announce_date','收盘价'])
#guotai.to_excel(r'C:/Users/wx/Desktop/基金公司打标签156148.xlsx')
#guotai = fund_data[fund_data['start_up_date'] == '19980305']
#下一步计算每一只基金的成立年限
#将成立年限的end_date都设置为20190325（与取数逻辑相对应）
fund_data['end_date'] = now

#转化为时间格式并相减计算中间天数（即基金经理平均管理时长）
start = pd.to_datetime(fund_data['start_date'])
end = pd.to_datetime(fund_data['end_date'])
distance = (end - start).values/np.timedelta64(1,'D')
#将天数变为年数
distance = distance/365
fund_data['distance'] = distance
#trade_dates.to_excel(r'C:/Users/wx/Desktop/基金公司打标签156146.xlsx')
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
category = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
category = category[['基金代码','一级分类','二级分类']]

fund_data = pd.merge( fund_data, category, on = ['基金代码'], how = 'left')

#下面将数据转换为周频率数据
fund_code = fund_data['基金代码']
fund_code = fund_code.drop_duplicates()

temp_D = pd.DataFrame( columns = fund_data.columns)
for i in fund_code:
    n = fund_data[fund_data['基金代码'] == i]
    n = n.sort_values( by = 'announce_date', ascending = False)
    n = pd.DataFrame(n)
    dates1 = n['announce_date']
    dates1_list = list(dates1)
    dates1 = pd.DataFrame(dates1)
    end_date = dates1_list[0]
    end = get_friday(end_date)
    dates2 = []
    length = int(170)
    for i in range(length):
        temp = date_gen(dates1, days=7 * i, end = end)
        dates2.append(temp)
    c = n[n['announce_date'].isin(dates2)]
    #将收盘价转化为收益率数据
    g_yield = get_yeild_fund(c)
    temp_D = pd.concat( [ temp_D, g_yield], ignore_index = True)

fund_data = temp_D
fund_data.to_excel(path+r'擅长产品类型标签.xlsx')
res_fund_data = fund_data
#下面两个函数中IR()与tracking_error()中，第一个参数为基金公司的周收益率数据，第二个数据可设置为2或3（注意若该参数改变，则sql前的日期设置中的years参数也需改变）
#第三个参数通常为今日日期（也可设置为最近一个报告期的结束日期），第四个参数为需要用到的几种市场指数的收益率数据，第五个参数为二级分类或一级分类，只能输入‘二’或‘一’（目前仅支持‘二’）


##############################################################一级分类和二级分类产品表现标签#########################################################################
#由于已知算法中，一级分类的基准未知，目前仅可打出二级分类产品表现标签
#一级分类基准确定后，只需修改函数get_market()即可打出一级分类产品标签
second_ind = ['股票型','激进配置型','标准配置型','保守配置型','灵活配置型','沪港深配置型','沪港深股票型','纯债型','普通债券型','激进债券型','可转债型']
fund_data_valid = fund_data[fund_data['二级分类'].isin(second_ind)]
fund_data_valid = fund_data_valid.drop(['收盘价'],axis = 1)

SHI =  IR( fund_data_valid, 3, '20190520', index_return, rank = '二')
SHI = SHI.reset_index(['二级分类', 'company', 'start_up_date'])
SHI_top = SHI.drop(['加权后IR'],axis = 1)
#SHI_top中每种分类下，按照从前往后的顺序，前10%的公司即为连续3年该类基金表现优秀的公司（若要得到连续两年的结果，注意同时修改sql前的参数！！）
SHI_top.to_excel(path+r'一级分类和二级分类产品表现标签.xlsx')
res_SHI_tops = SHI_top
##############################################################一级分类和二级分类产品表现标签#########################################################################
#由于已知算法中，指数型及ETF分类基准未知，目前该标签暂时不能打出
#指数型及ETF分类及基准确定后，只需修改函数get_market()即可打出特色类产品表现标签
SHI_special =  tracking_error( fund_data_valid, 3, '20190520', index_return, rank = '二')


#####################################################################################################
#以下为机构持有标签
#####################################################################################################
#半年度更新标签，需要设置最新一期的半年报报告日期，通常为 20XX0630 或 20XX1231
#输出结果分别为基金公司名，基金公司成立日，基金交易代码，该基金机构持有比例，该基金规模
end_date = 20181231
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090,f.F7_1533,SUM(d.F3_1104)
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F3_1104,F15_1104,MAX(F14_1104)
        FROM wind.TB_OBJECT_1104
        GROUP BY F3_1104,F15_1104)d
        ON c.F1_1099 = d.F15_1104
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090
        JOIN
        (SELECT
        F1_1533,F7_1533,F3_1533
        FROM wind.TB_OBJECT_1533
        WHERE F3_1533 = '%(end_date)s')f
        ON e.F2_1090 = f.F1_1533
        GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090,f.F7_1533

        '''%{'end_date':end_date}

cu = fund_db.cursor()
institution_hold_rate = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date','基金代码','机构持有比例','基金规模'])
##此处需要使用外部表格！！！
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
data = data[['基金代码','六大类型']]
data = pd.merge( data, institution_hold_rate, on = '基金代码', how = 'right')
# data.to_excel(r'C:/Users/wx/Desktop/基金公司打标签156147.xlsx')
data.to_excel(path+r'机构持有标签.xlsx')
res_institution_hold = data
##################################################1、总基金资产机构持有比例 ###############################################
#对不同基金公司进行机构持有比例打分
whole = data.groupby(by = ['company','start_up_date'])['基金规模'].sum()
whole = pd.DataFrame(whole)
whole.columns = ['sum']
whole = pd.merge( data, whole, on = 'company', how = 'left')
whole['weighted_sum'] = whole.apply(lambda x:x['机构持有比例']*x['基金规模']/100,axis = 1)
whole['weighted_percent'] = whole.apply(lambda x:x['weighted_sum']/x['sum'],axis =1)
whole_sum = whole.groupby(by = ['company','start_up_date'])['weighted_percent'].sum()
whole_sum = pd.DataFrame(whole_sum)

###对基金公司打标签
#先打高，中，低
whole_sum.loc[whole_sum['weighted_percent'] > 0.8, '总基金资产机构持有比例'] = '高-'
whole_sum.loc[(whole_sum['weighted_percent'] <= 0.8) & (whole_sum['weighted_percent'] > 0.3), '总基金资产机构持有比例'] = '中-'
whole_sum.loc[whole_sum['weighted_percent'] <= 0.3, '总基金资产机构持有比例'] = '低-'

#再将比例数据转化为百分数（保留一位小数）
temp = []
weighted_percent = whole_sum['weighted_percent']
for i in weighted_percent:
    i = round(i,3)
    i = str(100*i) + '%'
    if len(i) >= 6:
        i = i[0:4] + i[-1]
    temp.append(i)
whole_sum['weighted_percent'] = temp
whole_sum['总基金资产机构持有比例'] = whole_sum['总基金资产机构持有比例'] + whole_sum['weighted_percent']
whole_sum = whole_sum.drop('weighted_percent',axis = 1)
whole_sum = whole_sum.reset_index(['company','start_up_date'])
#whole_sum即为最终所打标签

whole_sum.to_excel(path+r'总基金资产机构持有比例.xlsx')
res_whole_sum = whole_sum

##################################################2、非货币基金基金资产机构持有比例 ###############################################
non_monetary = data[data['六大类型'] != '货币类']
_non_monetary = non_monetary.groupby(by = ['company','start_up_date'])['基金规模'].sum()
_non_monetary = pd.DataFrame(_non_monetary)
_non_monetary.columns = ['sum']
_non_monetary =pd.merge( non_monetary, _non_monetary, on = 'company', how = 'left')
_non_monetary['weighted_sum'] = _non_monetary.apply(lambda x:x['机构持有比例']*x['基金规模']/100,axis = 1)
_non_monetary['weighted_percent'] = _non_monetary.apply(lambda x:x['weighted_sum']/x['sum'],axis =1)
sum_non_monetary = _non_monetary.groupby(by = ['company','start_up_date'])['weighted_percent'].sum()
sum_non_monetary = pd.DataFrame(sum_non_monetary)

sum_non_monetary.loc[sum_non_monetary['weighted_percent'] > 0.8, '总基金资产机构持有比例'] = '高-'
sum_non_monetary.loc[(sum_non_monetary['weighted_percent'] <= 0.8) & (sum_non_monetary['weighted_percent'] > 0.3), '总基金资产机构持有比例'] = '中-'
sum_non_monetary.loc[sum_non_monetary['weighted_percent'] <= 0.3, '总基金资产机构持有比例'] = '低-'

#再将比例数据转化为百分数（保留一位小数）
temp = []
weighted_percent = sum_non_monetary['weighted_percent']
for i in weighted_percent:
    i = round(i,3)
    i = str(100*i) + '%'
    if len(i) >= 6:
        i = i[0:4] + i[-1]
    temp.append(i)
sum_non_monetary['weighted_percent'] = temp
sum_non_monetary['总基金资产机构持有比例'] = sum_non_monetary['总基金资产机构持有比例'] + sum_non_monetary['weighted_percent']
sum_non_monetary = sum_non_monetary.drop('weighted_percent',axis = 1)
sum_non_monetary = sum_non_monetary.reset_index(['company','start_up_date'])
#sum_non_monetary即为最终所打标签
sum_non_monetary.to_excel(path+r'非货币基金基金资产机构持有比例.xlsx')
res_sum_non_monetary = sum_non_monetary


##################################################3、偏股类基金资产机构持有比例 ###############################################

stock = data[data['六大类型'] == '偏股类']
_stock = stock.groupby(by = ['company','start_up_date'])['基金规模'].sum()
_stock = pd.DataFrame(_stock)
_stock.columns = ['sum']
_stock =pd.merge( stock, _stock, on = 'company', how = 'left')
_stock['weighted_sum'] = _stock.apply(lambda x:x['机构持有比例']*x['基金规模']/100,axis = 1)
_stock['weighted_percent'] = _stock.apply(lambda x:x['weighted_sum']/x['sum'],axis =1)
sum_stock = _stock.groupby(by = ['company','start_up_date'])['weighted_percent'].sum()
sum_stock = pd.DataFrame(sum_stock)

sum_stock.loc[sum_stock['weighted_percent'] > 0.8, '总基金资产机构持有比例'] = '高-'
sum_stock.loc[(sum_stock['weighted_percent'] <= 0.8) & (sum_stock['weighted_percent'] > 0.3), '总基金资产机构持有比例'] = '中-'
sum_stock.loc[sum_stock['weighted_percent'] <= 0.3, '总基金资产机构持有比例'] = '低-'

#再将比例数据转化为百分数（保留一位小数）
temp = []
weighted_percent = sum_stock['weighted_percent']
for i in weighted_percent:
    i = round(i,3)
    i = str(100*i) + '%'
    if len(i) >= 6:
        i = i[0:4] + i[-1]
    temp.append(i)
sum_stock['weighted_percent'] = temp
sum_stock['总基金资产机构持有比例'] = sum_stock['总基金资产机构持有比例'] + sum_stock['weighted_percent']
sum_stock = sum_stock.drop('weighted_percent',axis = 1)
sum_stock = sum_stock.reset_index(['company','start_up_date'])
#sum_stock即为最终所打标签
sum_stock.to_excel(path+r'偏股类基金资产机构持有比例.xlsx')
res_sum_stock_inst_hold = sum_stock

##################################################4、偏债类基金资产机构持有比例 ###############################################

bond = data[data['六大类型'] == '偏债类']
_bond = bond.groupby(by = ['company','start_up_date'])['基金规模'].sum()
_bond = pd.DataFrame(_bond)
_bond.columns = ['sum']
_bond =pd.merge( bond, _bond, on = 'company', how = 'left')
_bond['weighted_sum'] = _bond.apply(lambda x:x['机构持有比例']*x['基金规模']/100,axis = 1)
_bond['weighted_percent'] = _bond.apply(lambda x:x['weighted_sum']/x['sum'],axis =1)
sum_bond = _bond.groupby(by = ['company','start_up_date'])['weighted_percent'].sum()
sum_bond = pd.DataFrame(sum_bond)

sum_bond.loc[sum_bond['weighted_percent'] > 0.8, '总基金资产机构持有比例'] = '高-'
sum_bond.loc[(sum_bond['weighted_percent'] <= 0.8) & (sum_bond['weighted_percent'] > 0.3), '总基金资产机构持有比例'] = '中-'
sum_bond.loc[sum_bond['weighted_percent'] <= 0.3, '总基金资产机构持有比例'] = '低-'

#再将比例数据转化为百分数（保留一位小数）
temp = []
weighted_percent = sum_bond['weighted_percent']
for i in weighted_percent:
    i = round(i,3)
    i = str(100*i) + '%'
    if len(i) >= 6:
        i = i[0:4] + i[-1]
    temp.append(i)
sum_bond['weighted_percent'] = temp
sum_bond['总基金资产机构持有比例'] = sum_bond['总基金资产机构持有比例'] + sum_bond['weighted_percent']
sum_bond = sum_bond.drop('weighted_percent',axis = 1)
sum_bond = sum_bond.reset_index(['company','start_up_date'])
#sum_bond即为最终所打标签

sum_bond.to_excel(path+r'偏债类基金资产机构持有比例.xlsx')
res_sum_bond_inst_hold = sum_bond

##################################################5、货币基金基金资产机构持有比例 ###############################################
monetary = data[data['六大类型'] == '货币类']
_monetary = monetary.groupby(by = ['company','start_up_date'])['基金规模'].sum()
_monetary = pd.DataFrame(_monetary)
_monetary.columns = ['sum']
_monetary =pd.merge( monetary, _monetary, on = 'company', how = 'left')
_monetary['weighted_sum'] = _monetary.apply(lambda x:x['机构持有比例']*x['基金规模']/100,axis = 1)
_monetary['weighted_percent'] = _monetary.apply(lambda x:x['weighted_sum']/x['sum'],axis =1)
sum_monetary = _monetary.groupby(by = ['company','start_up_date'])['weighted_percent'].sum()
sum_monetary = pd.DataFrame(sum_monetary)

sum_monetary.loc[sum_monetary['weighted_percent'] > 0.8, '总基金资产机构持有比例'] = '高-'
sum_monetary.loc[(sum_monetary['weighted_percent'] <= 0.8) & (sum_monetary['weighted_percent'] > 0.3), '总基金资产机构持有比例'] = '中-'
sum_monetary.loc[sum_monetary['weighted_percent'] <= 0.3, '总基金资产机构持有比例'] = '低-'

#再将比例数据转化为百分数（保留一位小数）
temp = []
weighted_percent = sum_monetary['weighted_percent']
for i in weighted_percent:
    i = round(i,3)
    i = str(100*i) + '%'
    if len(i) >= 6:
        i = i[0:4] + i[-1]
    temp.append(i)
sum_monetary['weighted_percent'] = temp
sum_monetary['总基金资产机构持有比例'] = sum_monetary['总基金资产机构持有比例'] + sum_monetary['weighted_percent']
sum_monetary = sum_monetary.drop('weighted_percent',axis = 1)
sum_monetary = sum_monetary.reset_index(['company','start_up_date'])
#sum_monetary即为最终所打标签

sum_monetary.to_excel(path+r'货币基金基金资产机构持有比例.xlsx')
res_sum_monetary_inst_hold = sum_monetary
##################################################6、QDII基金基金资产机构持有比例 ###############################################

QDII = data[data['六大类型'] == 'QDII']
_QDII = QDII.groupby(by = ['company','start_up_date'])['基金规模'].sum()
_QDII = pd.DataFrame(_QDII)
_QDII.columns = ['sum']
_QDII =pd.merge( QDII, _QDII, on = 'company', how = 'left')
_QDII['weighted_sum'] = _QDII.apply(lambda x:x['机构持有比例']*x['基金规模']/100,axis = 1)
_QDII['weighted_percent'] = _QDII.apply(lambda x:x['weighted_sum']/x['sum'],axis =1)
sum_QDII = _QDII.groupby(by = ['company','start_up_date'])['weighted_percent'].sum()
sum_QDII = pd.DataFrame(sum_QDII)

sum_QDII.loc[sum_QDII['weighted_percent'] > 0.8, '总基金资产机构持有比例'] = '高-'
sum_QDII.loc[(sum_QDII['weighted_percent'] <= 0.8) & (sum_QDII['weighted_percent'] > 0.3), '总基金资产机构持有比例'] = '中-'
sum_QDII.loc[sum_QDII['weighted_percent'] <= 0.3, '总基金资产机构持有比例'] = '低-'

#再将比例数据转化为百分数（保留一位小数）
temp = []
weighted_percent = sum_QDII['weighted_percent']
for i in weighted_percent:
    i = round(i,3)
    i = str(100*i) + '%'
    if len(i) >= 6:
        i = i[0:4] + i[-1]
    temp.append(i)
sum_QDII['weighted_percent'] = temp
sum_QDII['总基金资产机构持有比例'] = sum_QDII['总基金资产机构持有比例'] + sum_QDII['weighted_percent']
sum_QDII = sum_QDII.drop('weighted_percent',axis = 1)
sum_QDII = sum_QDII.reset_index(['company','start_up_date'])
#sum_QDII即为最终所打标签
sum_QDII.to_excel(path+r'QDII基金基金资产机构持有比例.xlsx')
res_sum_QDII_inst_hold = sum_QDII



######################################################################################################
#以下为投研团队部分标签
######################################################################################################


#################################################基金公司基金经理总人数标签####################################################################
##无需设置参数，输出结果分别为基金公司、基金公司成立日、基金公司基金经理总人数
sql = '''
        SELECT
        e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
        FROM
        (SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
        FROM wind.TB_OBJECT_1272
        WHERE F3_1272 IS NOT NULL AND F4_1272 IS NULL)d
        ON c.F1_1099 = d.F1_1272
        GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272)e
        GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018

        '''

cu = fund_db.cursor()
sum_manager = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'sum_manager'])

managers = np.array(sum_manager['sum_manager'])

#按照分类标准取分位数
high = np.percentile(managers, 80)
low = np.percentile(managers, 20)

#根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
sum_manager.loc[sum_manager['sum_manager'] > high, '基金经理总人数标签'] = '高-'
sum_manager.loc[(sum_manager['sum_manager'] <= high) & (sum_manager['sum_manager'] > low), '基金经理总人数标签'] = '中-'
sum_manager.loc[sum_manager['sum_manager'] <= low, '基金经理总人数标签'] = '低-'

#将序号全部变为int格式,再转为字符串
list_manager = []
for num in sum_manager['sum_manager']:
    num = str(num)
    list_manager.append(num)
sum_manager['temp'] = list_manager

#在标签后加上基金经理总人数
sum_manager.loc[: , '基金经理总人数标签'] = sum_manager.loc[: , '基金经理总人数标签'] + sum_manager.loc[: , 'temp']
sum_manager = sum_manager.drop(['temp','sum_manager'], axis = 1)
#sum_manager即为最终标签
sum_manager.to_excel(path+r'基金公司基金经理总人数标签.xlsx')
res_sum_manager = sum_manager
#################################################基金公司基金经理管理年数标签####################################################################
#取数时无需设置参数
#筛选出基金公司名、公司成立日、基金经理名字、该基金经理在此基金公司管理第一支基金的开始时间
sql = '''
        SELECT
        b.OB_OBJECT_NAME_1018,b.F35_1018,c.F2_1272,MIN(c.F3_1272)
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018
        JOIN
        (SELECT
        F1_1272,F2_1272,F3_1272
        FROM wind.TB_OBJECT_1272
        WHERE F3_1272 IS NOT NULL)c
        ON a.F1_1099 = c.F1_1272
        GROUP BY b.OB_OBJECT_NAME_1018,b.F35_1018,c.F2_1272

        '''

cu = fund_db.cursor()
manage_date_start = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'manager','start_date'])

#取数时无需设置参数
#筛选出基金公司名、公司成立日、基金经理名字、该基金经理目前在此基金公司管理最后一支基金的截止时间（none表示截止目前基金仍在存续）
sql = '''
        SELECT
        b.OB_OBJECT_NAME_1018,b.F35_1018,c.F2_1272,MAX(c.F4_1272)
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018
        JOIN
        (SELECT
        F1_1272,F2_1272,F3_1272,F4_1272
        FROM wind.TB_OBJECT_1272
        WHERE F3_1272 IS NOT NULL)c
        ON a.F1_1099 = c.F1_1272
        GROUP BY b.OB_OBJECT_NAME_1018,b.F35_1018,c.F2_1272

        '''

cu = fund_db.cursor()
manage_date_end = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'manager','end_date'])

now = []
###today为更新标签日当日日期
today = '20190514'
for i in manage_date_end['end_date']:
    if i == None:
        i = today
    now.append(i)
manage_date_end['end_date'] = pd.DataFrame(now)

manage_date = pd.merge( manage_date_start, manage_date_end, on = ['company', 'start_up_date', 'manager'])

#转化为时间格式并相减计算中间天数（即基金经理平均管理时长）
start = pd.to_datetime(manage_date['start_date'])
end = pd.to_datetime(manage_date['end_date'])
distance = pd.DataFrame(end - start)
distance.columns = ['distance']
day = []
for i in distance['distance']:
    i = i.days
    i = round(i/365, 2)
    day.append(i)
manage_date['distance'] = day
manage_date.to_excel(path+r'基金公司基金经理管理年数标签.xlsx')
res_manage_date = manage_date
#################################################1、基金公司基金经理平均管理年限标签####################################################################
#计算每个公司的基金经理平均管理年限（不管该基金经理是否现就职于该公司）
mean_manage_years = manage_date.groupby(by = ['company', 'start_up_date'])['distance'].mean()
mean_manage_years = pd.DataFrame(mean_manage_years)
mean_manage_years.columns = ['mean_manage_years']

mean_ = mean_manage_years['mean_manage_years']
high = np.percentile(mean_, 80)
low = np.percentile(mean_, 20)

#根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
mean_manage_years.loc[mean_manage_years['mean_manage_years'] > high, '平均管理年限标签'] = '高-'
mean_manage_years.loc[(mean_manage_years['mean_manage_years'] <= high) & (mean_manage_years['mean_manage_years'] > low), '平均管理年限标签'] = '中-'
mean_manage_years.loc[mean_manage_years['mean_manage_years'] <= low, '平均管理年限标签'] = '低-'

#将序号全部变为int格式,再转为字符串
list_manage_years = []
for num in mean_manage_years['mean_manage_years']:
    num = str(num)
    list_manage_years.append(num)
mean_manage_years['temp'] = list_manage_years

#在标签后加上平均管理年限
mean_manage_years.loc[: , '平均管理年限标签'] = mean_manage_years.loc[: , '平均管理年限标签'] + mean_manage_years.loc[: , 'temp']
mean_manage_years = mean_manage_years.drop(['temp','mean_manage_years'], axis = 1)
mean_manage_years = mean_manage_years.reset_index(['company','start_up_date'])
#mean_manage_years即为所得平均管理年限标签
mean_manage_years.to_excel(path+r'基金公司基金经理平均管理年限标签.xlsx')
res_mean_manage_years = mean_manage_years

#################################################2、基金公司基金经理最大管理年限标签####################################################################
max_manage_years = manage_date.groupby(by = ['company', 'start_up_date'])['distance'].max()
max_manage_years = pd.DataFrame(max_manage_years)
max_manage_years.columns = ['max_manage_years']

max_ = max_manage_years['max_manage_years']
high = np.percentile(max_, 80)
low = np.percentile(max_, 20)

#根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
max_manage_years.loc[max_manage_years['max_manage_years'] > high, '最大管理年限标签'] = '高-'
max_manage_years.loc[(max_manage_years['max_manage_years'] <= high) & (max_manage_years['max_manage_years'] > low), '最大管理年限标签'] = '中-'
max_manage_years.loc[max_manage_years['max_manage_years'] <= low, '最大管理年限标签'] = '低-'

#将序号全部变为int格式,再转为字符串
list_manage_years = []
for num in max_manage_years['max_manage_years']:
    num = str(num)
    list_manage_years.append(num)
max_manage_years['temp'] = list_manage_years

#在标签后加上平均管理年限
max_manage_years.loc[: , '最大管理年限标签'] = max_manage_years.loc[: , '最大管理年限标签'] + max_manage_years.loc[: , 'temp']
max_manage_years = max_manage_years.drop(['temp','max_manage_years'], axis = 1)
max_manage_years = max_manage_years.reset_index(['company','start_up_date'])
#max_manage_years即为所得最大管理年限标签

max_manage_years.to_excel(path+r'基金公司基金经理最大管理年限标签.xlsx')
res_max_manage_years = max_manage_years
#################################################基金经理本公司任职年数标签####################################################################

#################################################1、本公司任职三年以内的基金经理比例####################################################################

#首先取出今日日期
now = datetime.today()
now = now.strftime('%Y%m%d')
now = pd.to_datetime(now).date()
#转变为三年前日期
date_3years_before = (now - relativedelta(years = 3)).strftime('%Y%m%d')
#取出任职三年以内的基金经理人数数据
sql = '''
        SELECT
        e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
        FROM
        (SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
        FROM wind.TB_OBJECT_1272
        WHERE F3_1272 IS NOT NULL AND F4_1272 IS NULL)d
        ON c.F1_1099 = d.F1_1272
        GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272
        HAVING MIN(d.F3_1272) >= '%(date_3years_before)s')e
        GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018

        '''%{'date_3years_before':date_3years_before}

cu = fund_db.cursor()
within_3years = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'within_3years'])

#取出基金公司基金经理总人数标签
##无需设置参数，输出结果分别为基金公司、基金公司成立日、基金公司基金经理总人数
sql = '''
        SELECT
        e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
        FROM
        (SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099) a
        JOIN
        (SELECT
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
        FROM wind.TB_OBJECT_1272
        WHERE F3_1272 IS NOT NULL AND F4_1272 IS NULL)d
        ON c.F1_1099 = d.F1_1272
        GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272)e
        GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018

        '''

cu = fund_db.cursor()
sum_manager = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'sum_manager'])

within_3years = pd.merge(within_3years, sum_manager, on = ['company', 'start_up_date'])
within_3years['percent'] = within_3years['within_3years']/within_3years['sum_manager']

percent = within_3years['percent']
high = np.percentile(percent, 80)
low = np.percentile(percent, 20)

#根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
within_3years.loc[within_3years['percent'] > high, '任职三年以内基金经理比例标签'] = '高-'
within_3years.loc[(within_3years['percent'] <= high) & (within_3years['within_3years'] > low), '任职三年以内基金经理比例标签'] = '中-'
within_3years.loc[within_3years['percent'] <= low, '任职三年以内基金经理比例标签'] = '低-'

#将序号全部变为int格式,再转为字符串
list_percent = []
for num in within_3years['percent']:
    num = round(num,2)
    num = str(num)
    list_percent.append(num)
within_3years['temp'] = list_percent

#在标签后加上任职符合要求的基金经理人数占比
within_3years.loc[: , '任职三年以内基金经理比例标签'] = within_3years.loc[: , '任职三年以内基金经理比例标签'] + within_3years.loc[: , 'temp']
within_3years = within_3years.drop(['temp','within_3years','sum_manager','temp','percent'], axis = 1)
#within_3years即为所求标签
within_3years.to_excel(path+r'本公司任职三年以内的基金经理比例.xlsx')
res_within_3years = within_3years

#################################################2、本公司任职三年以上、五年以上、十年以上的基金经理比例####################################################################
#只需设置lag = 3 或 5 或 10即可
def OVER_YEARS(lag = 5):
    #首先取出今日日期
    now = datetime.today()
    now = now.strftime('%Y%m%d')
    now = pd.to_datetime(now).date()
    #转变为三年前日期
    date_years_before = (now - relativedelta(years = lag)).strftime('%Y%m%d')
    #取出任职X年以上的基金经理人数数据
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL AND F4_1272 IS NULL)d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272
            HAVING MIN(d.F3_1272) <= '%(date_years_before)s')e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018
    
            '''%{'date_years_before':date_years_before}

    cu = fund_db.cursor()
    over_years = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'over_years'])

    #取出基金公司基金经理总人数标签
    ##无需设置参数，输出结果分别为基金公司、基金公司成立日、基金公司基金经理总人数
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL AND F4_1272 IS NULL)d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272)e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018
    
            '''

    cu = fund_db.cursor()
    sum_manager = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'sum_manager'])

    over_years = pd.merge(over_years, sum_manager, on = ['company', 'start_up_date'])
    over_years['percent'] = over_years['over_years']/over_years['sum_manager']

    percent = over_years['percent']
    high = np.percentile(percent, 80)
    low = np.percentile(percent, 20)

    #根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
    lag = str(lag)
    over_years.loc[over_years['percent'] > high, '任职' + lag + '年以内基金经理比例标签'] = '高-'
    over_years.loc[(over_years['percent'] <= high) & (over_years['percent'] > low), '任职' + lag + '年以内基金经理比例标签'] = '中-'
    over_years.loc[over_years['percent'] <= low, '任职' + lag + '年以内基金经理比例标签'] = '低-'

    #将序号全部变为int格式,再转为字符串
    list_percent = []
    for num in over_years['percent']:
        num = round(num,2)
        num = str(num)
        list_percent.append(num)
    over_years['temp'] = list_percent

    #在标签后加上符合要求的基金经理人数占比
    over_years.loc[: , '任职' + lag + '年以内基金经理比例标签'] = over_years.loc[: , '任职' + lag + '年以内基金经理比例标签'] + over_years.loc[: , 'temp']
    over_years = over_years.drop(['sum_manager','temp','percent'], axis = 1)
    #over_years即为所求标签
    return(over_years)


#################################################基金经理新增、离职、变动标签####################################################################

#################################################1、近一年、三年新增基金经理人数标签####################################################################
# 只需设置lag = 3 或 1
def NEW_MANAGERS(lag=3):
    # 首先取出今日日期
    now = datetime.today()
    now = now.strftime('%Y%m%d')
    now = pd.to_datetime(now).date()
    # 转变为三年前日期
    date_years_before = (now - relativedelta(years=lag)).strftime('%Y%m%d')
    # 取出近X年新增的基金经理人数数据
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL AND F4_1272 IS NULL)d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272
            HAVING MIN(d.F3_1272) >= '%(date_years_before)s')e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018

            ''' % {'date_years_before': date_years_before}

    cu = fund_db.cursor()
    new_managers = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'new_managers'])

    managers = new_managers['new_managers']
    high = np.percentile(managers, 80)
    low = np.percentile(managers, 20)

    # 根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
    lag = str(lag)
    new_managers.loc[new_managers['new_managers'] > high, '近' + lag + '年新增基金经理人数标签'] = '高-'
    new_managers.loc[(new_managers['new_managers'] <= high) & (new_managers['new_managers'] > low), '近' + lag + '年新增基金经理人数标签'] = '中-'
    new_managers.loc[new_managers['new_managers'] <= low, '近' + lag + '年新增基金经理人数标签'] = '低-'

    # 将序号全部变为int格式,再转为字符串
    list_percent = []
    for num in new_managers['new_managers']:
        num = round(num, 2)
        num = str(num)
        list_percent.append(num)
    new_managers['temp'] = list_percent

    # 在标签后加上符合要求的基金经理人数占比
    new_managers.loc[:, '近' + lag + '年新增基金经理人数标签'] = new_managers.loc[:, '近' + lag + '年新增基金经理人数标签'] + new_managers.loc[:,'temp']
    new_managers = new_managers.drop(['new_managers', 'temp'], axis=1)
    # new_managers即为所求标签
    return (new_managers)



#################################################2、近一年、三年离职基金经理人数标签####################################################################
# 只需设置lag = 3 或 1
def OFF_MANAGERS(lag=3):
    # 首先取出今日日期
    now = datetime.today()
    now = now.strftime('%Y%m%d')
    now = pd.to_datetime(now).date()
    # 转变为三年前日期
    date_years_before = (now - relativedelta(years=lag)).strftime('%Y%m%d')
    # 取出近X年新增的基金经理人数数据
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL )d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272
            HAVING (MAX(d.F4_1272) >= '%(date_years_before)s') AND (MIN(d.F4_1272) IS NOT NULL))e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018

            ''' % {'date_years_before': date_years_before}

    cu = fund_db.cursor()
    off_managers = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'off_managers'])

    managers = off_managers['off_managers']
    high = np.percentile(managers, 80)
    low = np.percentile(managers, 20)

    # 根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
    lag = str(lag)
    off_managers.loc[off_managers['off_managers'] > high, '近' + lag + '年离职基金经理人数标签'] = '高-'
    off_managers.loc[(off_managers['off_managers'] <= high) & (off_managers['off_managers'] > low), '近' + lag + '年离职基金经理人数标签'] = '中-'
    off_managers.loc[off_managers['off_managers'] <= low, '近' + lag + '年离职基金经理人数标签'] = '低-'

    # 将序号全部变为int格式,再转为字符串
    list_percent = []
    for num in off_managers['off_managers']:
        num = round(num, 2)
        num = str(num)
        list_percent.append(num)
    off_managers['temp'] = list_percent

    # 在标签后加上符合要求的基金经理人数占比
    off_managers.loc[:, '近' + lag + '年离职基金经理人数标签'] = off_managers.loc[:, '近' + lag + '年离职基金经理人数标签'] + off_managers.loc[:,'temp']
    off_managers = off_managers.drop(['off_managers', 'temp'], axis=1)
    # new_managers即为所求标签
    return (off_managers)



#################################################3、近一年、三年基金经理离职率标签####################################################################
# 只需设置lag = 3 或 1
def OFF_MANAGERS_PER(lag=3):
    # 首先取出今日日期
    now = datetime.today()
    now = now.strftime('%Y%m%d')
    now = pd.to_datetime(now).date()
    # 转变为三年前日期
    date_years_before = (now - relativedelta(years=lag)).strftime('%Y%m%d')
    # 取出X年以内离职的基金经理人数数据
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL)d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272
            HAVING MAX(d.F4_1272) >= '%(date_years_before)s' AND MIN(d.F4_1272) IS NOT NULL)e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018

            ''' % {'date_years_before': date_years_before}

    cu = fund_db.cursor()
    off_managers = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'off_managers'])

    # 取出基金公司基金经理总人数标签
    ##无需设置参数，输出结果分别为基金公司、基金公司成立日、基金公司基金经理总人数
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F2_1272)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272,MAX(d.F4_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F3_1272 IS NOT NULL)d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272
            HAVING (MAX(d.F4_1272) >= '%(date_years_before)s' OR MIN(d.F4_1272) IS NULL) AND MIN(d.F3_1272) < '%(date_years_before)s' )e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018

            '''% {'date_years_before': date_years_before}

    cu = fund_db.cursor()
    sum_manager = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'sum_manager'])

    off_managers = pd.merge(off_managers, sum_manager, on=['company', 'start_up_date'])
    off_managers['percent'] = off_managers['off_managers'] / sum_manager['sum_manager']

    percent = off_managers['percent']
    high = np.percentile(percent, 80)
    low = np.percentile(percent, 20)

    # 根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
    lag = str(lag)
    off_managers.loc[off_managers['percent'] > high, '近' + lag + '年基金经理离职率标签'] = '高-'
    off_managers.loc[(off_managers['percent'] <= high) & (off_managers['percent'] > low), '近' + lag + '年基金经理离职率标签'] = '中-'
    off_managers.loc[off_managers['percent'] <= low, '近' + lag + '年基金经理离职率标签'] = '低-'

    # 将序号全部变为int格式,再转为字符串
    list_percent = []
    for num in off_managers['percent']:
        num = round(num, 2)
        num = str(num)
        list_percent.append(num)
    off_managers['temp'] = list_percent

    # 在标签后加上符合要求的基金经理人数占比
    off_managers.loc[:, '近' + lag + '年基金经理离职率标签'] = off_managers.loc[:, '近' + lag + '年基金经理离职率标签'] + off_managers.loc[:,'temp']
    off_managers = off_managers.drop(['sum_manager', 'temp', 'percent'], axis=1)
    # off_managers即为所求标签
    return (off_managers)



#################################################4、近一年、三年基金经理发生更换的产品比率标签####################################################################
# 只需设置lag = 3 或 1
# 首先取出今日日期
def SUM_PRODUCT(lag = 1):
    lag = 1
    now = datetime.today()
    now = now.strftime('%Y%m%d')
    now = pd.to_datetime(now).date()
    # 转变为三年前日期
    date_years_before = (now - relativedelta(years=lag)).strftime('%Y%m%d')
    #按照产品筛选
    #先计算每个公司每个时间段内存续的产品总数
    sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,COUNT(DISTINCT c.F1_1099)
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099) a
        JOIN
        (SELECT 
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
        FROM wind.TB_OBJECT_1272
        WHERE F4_1272 IS NULL AND F3_1272 <= '%(date_years_before)s')d
        ON c.F1_1099 = d.F1_1272
        GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018
    
        '''% {'date_years_before': date_years_before}

    cu = fund_db.cursor()
    whole_product = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date','whole_product'])

    #再计算存续期内发生产品经理信息变更的产品总数
    #首先选定存续期，count每个产品经手经理的总数（e内的取数逻辑）
    #然后针对e内count值大于2的产品，按基金公司分组COUNT产品总数
    sql = '''
            SELECT
            e.OB_OBJECT_NAME_1018,e.F35_1018,COUNT(DISTINCT e.F1_1099)
            FROM
            (SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,c.F1_1099,COUNT(DISTINCT d.F2_1272)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F1_1272,F2_1272,F6_1272,F3_1272,F4_1272
            FROM wind.TB_OBJECT_1272
            WHERE F4_1272 IS NULL AND F3_1272 <= '%(date_years_before)s')d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,c.F1_1099
            HAVING COUNT(DISTINCT d.F2_1272) > 1) e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018
            
    
            '''% {'date_years_before': date_years_before}

    cu = fund_db.cursor()
    sum_product = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date','sum_product'])

    sum_product = pd.merge( sum_product, whole_product, on = ['company', 'start_up_date'] )
    sum_product['percent'] = sum_product['sum_product']/sum_product['whole_product']

    percent = sum_product['percent']
    high = np.percentile(percent, 80)
    low = np.percentile(percent, 20)

    # 根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
    lag = str(lag)
    sum_product.loc[sum_product['percent'] > high, '近' + lag + '年基金经理发生更换的产品比率标签'] = '高-'
    sum_product.loc[(sum_product['percent'] <= high) & (sum_product['percent'] > low), '近' + lag + '年基金经理发生更换的产品比率标签'] = '中-'
    sum_product.loc[sum_product['percent'] <= low, '近' + lag + '年基金经理发生更换的产品比率标签'] = '低-'

    # 将序号全部变为int格式,再转为字符串
    list_percent = []
    for num in sum_product['percent']:
        num = round(num, 2)
        num = str(num)
        list_percent.append(num)
    sum_product['temp'] = list_percent

    # 在标签后加上符合要求的基金经理人数占比
    sum_product.loc[:, '近' + lag + '年基金经理发生更换的产品比率标签'] = sum_product.loc[:, '近' + lag + '年基金经理发生更换的产品比率标签'] + sum_product.loc[:,'temp']
    sum_product = sum_product.drop(['sum_product', 'temp', 'percent'], axis=1)
    # sum_product即为所求标签
    return(sum_product)



"""
######################################################################################################
#以下为投资风格部分标签
######################################################################################################

########################################偏股类产品偏好主题标签#######################################################
# 首先计算单支基金的股票投资市值
#首先选取六个最近的半年报报告期(后面代码需要用到这些日期，因此日期大小的顺序必须由近及远)
date1 = '20181231'
date2 = '20180630'
date3 = '20171231'
date4 = '20170630'
date5 = '20161231'
date6 = '20160630'
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090,d.F4_1104,d.F14_1104
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT 
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F4_1104,F15_1104,F14_1104
        FROM wind.TB_OBJECT_1104
        WHERE F14_1104 = '%(date1)s' OR F14_1104 = '%(date2)s' OR F14_1104 = '%(date3)s' 
        OR F14_1104 = '%(date4)s' OR F14_1104 = '%(date5)s' OR F14_1104 = '%(date6)s')d
        ON c.F1_1099 = d.F15_1104
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090

        '''% {'date1':date1,'date2':date2,'date3':date3,'date4':date4,'date5':date5,'date6':date6}

cu = fund_db.cursor()
trade_dates1 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_date', '基金代码', 'stock_value','announce_date'])

#########此处需要使用本地文件
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
data = data[['基金代码', '六大类型']]
data = data[data['六大类型'] == '偏股类']
trade_dates1 = pd.merge(trade_dates1, data, on='基金代码', how='right')
trade_dates1 = trade_dates1[['company', 'start_date', '基金代码', 'stock_value','announce_date']]
trade_dates1 = trade_dates1[trade_dates1['stock_value'] != 0]

# 然后按公司对上一步加总，计算每个基金公司偏股类产品投资于股票部分的市值之和
trade_dates2 = trade_dates1.groupby(by = ['company', 'start_date','announce_date'])
fx = lambda x: np.sum(x['stock_value'])
trade_dates2 = trade_dates2.apply(fx)
trade_dates2 = pd.DataFrame(trade_dates2)
trade_dates2.columns = ['sum_stock_value']
trade_dates2 = trade_dates2[trade_dates2['sum_stock_value'] != 0]

# 对1、2两部分数据进行汇合加总
trade_dates_s = pd.merge(trade_dates1, trade_dates2, on=['company','start_date','announce_date'], how='left')
trade_dates_s['A'] = trade_dates_s.apply(lambda x: x['stock_value'] / x['sum_stock_value'], axis=1)

#计算各支基金投资于各股票的市值
sql = '''
        SELECT
        e.F16_1090,f.OB_OBJECT_NAME_1090,f.F16_1090,f.F4_1102,f.F6_1102
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT 
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        m.F7_1102,m.F4_1102,n.OB_OBJECT_NAME_1090,m.F3_1102,n.F16_1090,m.F6_1102
        FROM
        (SELECT 
        F7_1102,F3_1102,F4_1102,F6_1102
        FROM  wind.TB_OBJECT_1102 
        WHERE F6_1102 = '%(date1)s' OR F6_1102 = '%(date2)s' OR F6_1102 = '%(date3)s' 
        OR F6_1102 = '%(date4)s' OR F6_1102 = '%(date5)s' OR F6_1102 = '%(date6)s')m
        JOIN
        (SELECT
        F2_1090,OB_OBJECT_NAME_1090,F16_1090
        FROM
        wind.TB_OBJECT_1090
        )n
        ON m.F3_1102 = n.F2_1090 
        )f
        ON f.F7_1102 = c.F1_1099
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090

        '''% {'date1':date1,'date2':date2,'date3':date3,'date4':date4,'date5':date5,'date6':date6}

cu = fund_db.cursor()
trade_dates3 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['基金代码','inv_stock_name','inv_stock_code','inv_stock_value','announce_date'])

trade_dates = pd.merge( trade_dates_s, trade_dates3, on = ['基金代码','announce_date'], how = 'left')

##此处需要使用本地文件
path2 = path+'申万一级行业分类.xlsx'
shenwan_industry_1 = pd.read_excel(path2)
trade_dates = pd.merge( trade_dates, shenwan_industry_1, on = 'inv_stock_name', how = 'right')
trade_dates['B'] = trade_dates.apply(lambda x:x['inv_stock_value']/x['stock_value'], axis = 1)

###此处需要使用本地文件
path3 = path+'申万一级行业指数.xlsx'
theme = pd.read_excel(path3)
theme.rename(columns={'证券代码':'六大主题'},inplace=True)
theme = theme[['申万一级行业分类','六大主题']]
trade_dates = pd.merge( trade_dates, theme, on = '申万一级行业分类', how = 'left')

############################################################1、偏股类基金近半年偏好主题标签###################################################
trade_dates_x = trade_dates[trade_dates['announce_date'] == date1]
trade_dates_x = trade_dates_x[['company','start_date','基金代码','A','inv_stock_name','inv_stock_code_y','六大主题','B']]
    #trade_dates_x =trade_dates_x[trade_dates_x['company'] == '易方达基金管理有限公司']

grouped = trade_dates_x.groupby(by = ['company','start_date','基金代码','六大主题'])
get_sum = lambda x:np.sum(x['B'])
step_1 = grouped.apply(get_sum)
step_1 = pd.DataFrame(step_1)
middle = trade_dates_x[['company','start_date','A','六大主题']]
step_1 = pd.merge( step_1, middle, on = ['company','start_date','六大主题'] )
step_1 = step_1.drop_duplicates(subset = ['company','六大主题'],keep = 'first')
step_1.columns = ['company','start_date','六大主题','B','A']
grouped = step_1.groupby(by = ['company','start_date','六大主题'])
get_favor = lambda x: np.average( x['B'], weights = x['A'])
favor_unique = grouped.apply(get_favor)
favor_unique = pd.DataFrame(favor_unique)
favor_unique.columns = ['C']
favor_unique = favor_unique.sort_values(by = ['company','C'], ascending= False)
favor_unique = pd.DataFrame(favor_unique)
favor_unique = favor_unique[favor_unique['C'] > 0.5]
favor_unique = favor_unique.reset_index(['company','start_date','六大主题'])

favor_unique['偏股类产品近半年偏好主题标签'] = favor_unique['六大主题']
favor_unique = favor_unique.drop(['六大主题','C'],axis = 1)
#favor_unique即为最终标签




############################################################2、偏股类基金近一年偏好主题标签###################################################
trade_dates_x = trade_dates[trade_dates['announce_date'] == date1]
trade_dates_y = trade_dates[trade_dates['announce_date'] == date2]

trade_dates_x = trade_dates_x[['company','start_date','基金代码','A','inv_stock_name','inv_stock_code_y','六大主题','B']]
trade_dates_y = trade_dates_y[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_y.columns = ['company','start_date','基金代码','A_1','inv_stock_name','B_1']

trade_dates_x = pd.merge( trade_dates_x, trade_dates_y, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x['A_mean'] = trade_dates_x.apply(lambda x: (x['A']+x['A_1'])/2, axis = 1)
trade_dates_x['B_mean'] = trade_dates_x.apply(lambda x: (x['B']+x['B_1'])/2, axis = 1)
grouped = trade_dates_x.groupby(by = ['company','start_date','基金代码','六大主题'])
get_sum = lambda x:np.sum(x['B_mean'])
step_1 = grouped.apply(get_sum)
step_1 = pd.DataFrame(step_1)
middle = trade_dates_x[['company','start_date','A_mean','六大主题']]
step_1 = pd.merge( step_1, middle, on = ['company','start_date','六大主题'] )
step_1 = step_1.drop_duplicates(subset = ['company','六大主题'],keep = 'first')
step_1.columns = ['company','start_date','六大主题','B_mean','A_mean']
grouped = step_1.groupby(by = ['company','start_date','六大主题'])
get_favor = lambda x: np.average( x['B_mean'], weights = x['A_mean'])
favor_unique = grouped.apply(get_favor)
favor_unique = pd.DataFrame(favor_unique)
favor_unique.columns = ['C']
favor_unique = favor_unique.sort_values(by = ['company','C'], ascending= False)
favor_unique = pd.DataFrame(favor_unique)
favor_unique = favor_unique[favor_unique['C'] > 0.5]
favor_unique = favor_unique.reset_index(['company','start_date','六大主题'])

favor_unique['偏股类产品近一年偏好主题标签'] = favor_unique['六大主题']
favor_unique = favor_unique.drop(['六大主题','C'],axis = 1)
#favor_unique即为最终标签


############################################################3、偏股类基金近两年偏好主题标签###################################################
trade_dates_x = trade_dates[trade_dates['announce_date'] == date1]
trade_dates_y = trade_dates[trade_dates['announce_date'] == date2]
trade_dates_m = trade_dates[trade_dates['announce_date'] == date3]
trade_dates_n = trade_dates[trade_dates['announce_date'] == date4]

trade_dates_x = trade_dates_x[['company','start_date','基金代码','A','inv_stock_name','inv_stock_code_y','六大主题','B']]
trade_dates_y = trade_dates_y[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_y.columns = ['company','start_date','基金代码','A_1','inv_stock_name','B_1']
trade_dates_m = trade_dates_m[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_m.columns = ['company','start_date','基金代码','A_2','inv_stock_name','B_2']
trade_dates_n = trade_dates_n[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_n.columns = ['company','start_date','基金代码','A_3','inv_stock_name','B_3']

trade_dates_x = pd.merge( trade_dates_x, trade_dates_y, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_m, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_n, on = ['company','start_date','基金代码','inv_stock_name'])

trade_dates_x['A_mean'] = trade_dates_x.apply(lambda x: (x['A']+x['A_1']+x['A_2']+x['A_3'])/4, axis = 1)
trade_dates_x['B_mean'] = trade_dates_x.apply(lambda x: (x['B']+x['B_1']+x['B_2']+x['B_3'])/4, axis = 1)

grouped = trade_dates_x.groupby(by = ['company','start_date','基金代码','六大主题'])
get_sum = lambda x:np.sum(x['B_mean'])
step_1 = grouped.apply(get_sum)
step_1 = pd.DataFrame(step_1)
middle = trade_dates_x[['company','start_date','A_mean','六大主题']]
step_1 = pd.merge( step_1, middle, on = ['company','start_date','六大主题'] )
step_1 = step_1.drop_duplicates(subset = ['company','六大主题'],keep = 'first')
step_1.columns = ['company','start_date','六大主题','B_mean','A_mean']
grouped = step_1.groupby(by = ['company','start_date','六大主题'])
get_favor = lambda x: np.average( x['B_mean'], weights = x['A_mean'])
favor_unique = grouped.apply(get_favor)
favor_unique = pd.DataFrame(favor_unique)
favor_unique.columns = ['C']
favor_unique = favor_unique.sort_values(by = ['company','C'], ascending= False)

favor_unique = pd.DataFrame(favor_unique)
favor_unique = favor_unique[favor_unique['C'] > 0.5]
favor_unique = favor_unique.reset_index(['company','start_date','六大主题'])

favor_unique['偏股类产品近两年偏好主题标签'] = favor_unique['六大主题']
favor_unique = favor_unique.drop(['六大主题','C'],axis = 1)
#favor_unique即为最终标签


############################################################4、偏股类基金近三年偏好主题标签###################################################
trade_dates_x = trade_dates[trade_dates['announce_date'] == date1]
trade_dates_y = trade_dates[trade_dates['announce_date'] == date2]
trade_dates_m = trade_dates[trade_dates['announce_date'] == date3]
trade_dates_n = trade_dates[trade_dates['announce_date'] == date4]
trade_dates_p = trade_dates[trade_dates['announce_date'] == date5]
trade_dates_q = trade_dates[trade_dates['announce_date'] == date6]

trade_dates_x = trade_dates_x[['company','start_date','基金代码','A','inv_stock_name','inv_stock_code_y','六大主题','B']]
trade_dates_y = trade_dates_y[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_y.columns = ['company','start_date','基金代码','A_1','inv_stock_name','B_1']
trade_dates_m = trade_dates_m[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_m.columns = ['company','start_date','基金代码','A_2','inv_stock_name','B_2']
trade_dates_n = trade_dates_n[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_n.columns = ['company','start_date','基金代码','A_3','inv_stock_name','B_3']
trade_dates_p = trade_dates_p[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_p.columns = ['company','start_date','基金代码','A_4','inv_stock_name','B_4']
trade_dates_q = trade_dates_q[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_q.columns = ['company','start_date','基金代码','A_5','inv_stock_name','B_5']

trade_dates_x = pd.merge( trade_dates_x, trade_dates_y, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_m, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_n, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_p, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_q, on = ['company','start_date','基金代码','inv_stock_name'])

trade_dates_x['A_mean'] = trade_dates_x.apply(lambda x: (x['A']+x['A_1']+x['A_2']+x['A_3']+x['A_4']+x['A_5'])/6, axis = 1)
trade_dates_x['B_mean'] = trade_dates_x.apply(lambda x: (x['B']+x['B_1']+x['B_2']+x['B_3']+x['B_4']+x['B_5'])/6, axis = 1)

grouped = trade_dates_x.groupby(by = ['company','start_date','基金代码','六大主题'])
get_sum = lambda x:np.sum(x['B_mean'])
step_1 = grouped.apply(get_sum)
step_1 = pd.DataFrame(step_1)
middle = trade_dates_x[['company','start_date','A_mean','六大主题']]
step_1 = pd.merge( step_1, middle, on = ['company','start_date','六大主题'] )
step_1 = step_1.drop_duplicates(subset = ['company','六大主题'],keep = 'first')
step_1.columns = ['company','start_date','六大主题','B_mean','A_mean']
grouped = step_1.groupby(by = ['company','start_date','六大主题'])
get_favor = lambda x: np.average( x['B_mean'], weights = x['A_mean'])
favor_unique = grouped.apply(get_favor)
favor_unique = pd.DataFrame(favor_unique)
favor_unique.columns = ['C']
favor_unique = favor_unique.sort_values(by = ['company','C'], ascending= False)

favor_unique = pd.DataFrame(favor_unique)
favor_unique = favor_unique[favor_unique['C'] > 0.5]
favor_unique = favor_unique.reset_index(['company','start_date','六大主题'])

favor_unique['偏股类产品近三年偏好主题标签'] = favor_unique['六大主题']
favor_unique = favor_unique.drop(['六大主题','C'],axis = 1)
#favor_unique即为最终标签



#####################################################偏股类产品偏好行业标签############################################################
# 首先计算单支基金的股票投资市值
#首先选取六个最近的半年报报告期(后面代码需要用到这些日期，因此日期大小的顺序必须由近及远)
date1 = '20181231'
date2 = '20180630'
date3 = '20171231'
date4 = '20170630'
date5 = '20161231'
date6 = '20160630'
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090,d.F4_1104,d.F14_1104
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT 
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F4_1104,F15_1104,F14_1104
        FROM wind.TB_OBJECT_1104
        WHERE F14_1104 = '%(date1)s' OR F14_1104 = '%(date2)s' OR F14_1104 = '%(date3)s' 
        OR F14_1104 = '%(date4)s' OR F14_1104 = '%(date5)s' OR F14_1104 = '%(date6)s')d
        ON c.F1_1099 = d.F15_1104
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090

        '''% {'date1':date1,'date2':date2,'date3':date3,'date4':date4,'date5':date5,'date6':date6}

cu = fund_db.cursor()
trade_dates1 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_date', '基金代码', 'stock_value','announce_date'])

######需要读取外部数据
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
data = data[['基金代码', '六大类型']]
data = data[data['六大类型'] == '偏股类']
trade_dates1 = pd.merge(trade_dates1, data, on='基金代码', how='right')
trade_dates1 = trade_dates1[['company', 'start_date', '基金代码', 'stock_value','announce_date']]
trade_dates1 = trade_dates1[trade_dates1['stock_value'] != 0]

# 然后按公司对上一步加总，计算每个基金公司偏股类产品投资于股票部分的市值之和
trade_dates2 = trade_dates1.groupby(by = ['company', 'start_date','announce_date'])
fx = lambda x: np.sum(x['stock_value'])
trade_dates2 = trade_dates2.apply(fx)
trade_dates2 = pd.DataFrame(trade_dates2)
trade_dates2.columns = ['sum_stock_value']
trade_dates2 = trade_dates2[trade_dates2['sum_stock_value'] != 0]

# 对1、2两部分数据进行汇合加总
trade_dates_s = pd.merge(trade_dates1, trade_dates2, on=['company','start_date','announce_date'], how='left')
trade_dates_s['A'] = trade_dates_s.apply(lambda x: x['stock_value'] / x['sum_stock_value'], axis=1)

#计算各支基金投资于各股票的市值
sql = '''
        SELECT
        e.F16_1090,f.OB_OBJECT_NAME_1090,f.F16_1090,f.F4_1102,f.F6_1102
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT 
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        m.F7_1102,m.F4_1102,n.OB_OBJECT_NAME_1090,m.F3_1102,n.F16_1090,m.F6_1102
        FROM
        (SELECT 
        F7_1102,F3_1102,F4_1102,F6_1102
        FROM  wind.TB_OBJECT_1102 
        WHERE F6_1102 = '%(date1)s' OR F6_1102 = '%(date2)s' OR F6_1102 = '%(date3)s' 
        OR F6_1102 = '%(date4)s' OR F6_1102 = '%(date5)s' OR F6_1102 = '%(date6)s')m
        JOIN
        (SELECT
        F2_1090,OB_OBJECT_NAME_1090,F16_1090
        FROM
        wind.TB_OBJECT_1090
        )n
        ON m.F3_1102 = n.F2_1090 
        )f
        ON f.F7_1102 = c.F1_1099
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090

        '''% {'date1':date1,'date2':date2,'date3':date3,'date4':date4,'date5':date5,'date6':date6}

cu = fund_db.cursor()
trade_dates3 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['基金代码','inv_stock_name','inv_stock_code','inv_stock_value','announce_date'])

trade_dates = pd.merge( trade_dates_s, trade_dates3, on = ['基金代码','announce_date'], how = 'left')

#此处需要引用外部数据
path2 = 'C:/Users/wx/Desktop/模板/申万一级行业分类.xlsx'
shenwan_industry_1 = pd.read_excel(path2)
trade_dates = pd.merge( trade_dates, shenwan_industry_1, on = 'inv_stock_name', how = 'right')
trade_dates['B'] = trade_dates.apply(lambda x:x['inv_stock_value']/x['stock_value'], axis = 1)





######################################################1、偏股类基金近半年偏好行业标签###########################################################
trade_dates_x = trade_dates[trade_dates['announce_date'] == date1]
trade_dates_x = trade_dates_x[['company','start_date','基金代码','A','inv_stock_name','inv_stock_code_y','申万一级行业分类','B']]
    #trade_dates_x =trade_dates_x[trade_dates_x['company'] == '易方达基金管理有限公司']

grouped = trade_dates_x.groupby(by = ['company','start_date','基金代码','申万一级行业分类'])
get_sum = lambda x:np.sum(x['B'])
step_1 = grouped.apply(get_sum)
step_1 = pd.DataFrame(step_1)
middle = trade_dates_x[['company','start_date','A','申万一级行业分类']]
step_1 = pd.merge( step_1, middle, on = ['company','start_date','申万一级行业分类'] )
step_1 = step_1.drop_duplicates(subset = ['company','申万一级行业分类'],keep = 'first')
step_1.columns = ['company','start_date','申万一级行业分类','B','A']
grouped = step_1.groupby(by = ['company','start_date','申万一级行业分类'])
get_favor = lambda x: np.average( x['B'], weights = x['A'])
favor_unique = grouped.apply(get_favor)
favor_unique = pd.DataFrame(favor_unique)
favor_unique.columns = ['C']
favor_unique = favor_unique.sort_values(by = ['company','C'], ascending= False)

favor_unique = pd.DataFrame(favor_unique)
favor_unique = favor_unique[favor_unique['C'] > 0.25]
favor_unique = favor_unique.reset_index(['company','start_date','申万一级行业分类'])

favor_unique['偏股类基金近半年偏好行业标签'] = favor_unique['申万一级行业分类']
favor_unique = favor_unique.drop(['申万一级行业分类','C'],axis = 1)
#favor_unique即为最终标签





######################################################2、偏股类基金近一年偏好行业标签###########################################################
trade_dates_x = trade_dates[trade_dates['announce_date'] == date1]
trade_dates_y = trade_dates[trade_dates['announce_date'] == date2]

trade_dates_x = trade_dates_x[['company','start_date','基金代码','A','inv_stock_name','inv_stock_code_y','申万一级行业分类','B']]
trade_dates_y = trade_dates_y[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_y.columns = ['company','start_date','基金代码','A_1','inv_stock_name','B_1']

trade_dates_x = pd.merge( trade_dates_x, trade_dates_y, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x['A_mean'] = trade_dates_x.apply(lambda x: (x['A']+x['A_1'])/2, axis = 1)
trade_dates_x['B_mean'] = trade_dates_x.apply(lambda x: (x['B']+x['B_1'])/2, axis = 1)
grouped = trade_dates_x.groupby(by = ['company','start_date','基金代码','申万一级行业分类'])
get_sum = lambda x:np.sum(x['B_mean'])
step_1 = grouped.apply(get_sum)
step_1 = pd.DataFrame(step_1)
middle = trade_dates_x[['company','start_date','A_mean','申万一级行业分类']]
step_1 = pd.merge( step_1, middle, on = ['company','start_date','申万一级行业分类'] )
step_1 = step_1.drop_duplicates(subset = ['company','申万一级行业分类'],keep = 'first')
step_1.columns = ['company','start_date','申万一级行业分类','B_mean','A_mean']
grouped = step_1.groupby(by = ['company','start_date','申万一级行业分类'])
get_favor = lambda x: np.average( x['B_mean'], weights = x['A_mean'])
favor_unique = grouped.apply(get_favor)
favor_unique = pd.DataFrame(favor_unique)
favor_unique.columns = ['C']
favor_unique = favor_unique.sort_values(by = ['company','C'], ascending= False)

favor_unique = pd.DataFrame(favor_unique)
favor_unique = favor_unique[favor_unique['C'] > 0.25]
favor_unique = favor_unique.reset_index(['company','start_date','申万一级行业分类'])

favor_unique['偏股类基金近一年偏好行业标签'] = favor_unique['申万一级行业分类']
favor_unique = favor_unique.drop(['申万一级行业分类','C'],axis = 1)
#favor_unique即为最终标签




######################################################3、偏股类基金近两年偏好行业标签###########################################################
trade_dates_x = trade_dates[trade_dates['announce_date'] == date1]
trade_dates_y = trade_dates[trade_dates['announce_date'] == date2]
trade_dates_m = trade_dates[trade_dates['announce_date'] == date3]
trade_dates_n = trade_dates[trade_dates['announce_date'] == date4]

trade_dates_x = trade_dates_x[['company','start_date','基金代码','A','inv_stock_name','inv_stock_code_y','申万一级行业分类','B']]
trade_dates_y = trade_dates_y[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_y.columns = ['company','start_date','基金代码','A_1','inv_stock_name','B_1']
trade_dates_m = trade_dates_m[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_m.columns = ['company','start_date','基金代码','A_2','inv_stock_name','B_2']
trade_dates_n = trade_dates_n[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_n.columns = ['company','start_date','基金代码','A_3','inv_stock_name','B_3']

trade_dates_x = pd.merge( trade_dates_x, trade_dates_y, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_m, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_n, on = ['company','start_date','基金代码','inv_stock_name'])

trade_dates_x['A_mean'] = trade_dates_x.apply(lambda x: (x['A']+x['A_1']+x['A_2']+x['A_3'])/4, axis = 1)
trade_dates_x['B_mean'] = trade_dates_x.apply(lambda x: (x['B']+x['B_1']+x['B_2']+x['B_3'])/4, axis = 1)

grouped = trade_dates_x.groupby(by = ['company','start_date','基金代码','申万一级行业分类'])
get_sum = lambda x:np.sum(x['B_mean'])
step_1 = grouped.apply(get_sum)
step_1 = pd.DataFrame(step_1)
middle = trade_dates_x[['company','start_date','A_mean','申万一级行业分类']]
step_1 = pd.merge( step_1, middle, on = ['company','start_date','申万一级行业分类'] )
step_1 = step_1.drop_duplicates(subset = ['company','申万一级行业分类'],keep = 'first')
step_1.columns = ['company','start_date','申万一级行业分类','B_mean','A_mean']
grouped = step_1.groupby(by = ['company','start_date','申万一级行业分类'])
get_favor = lambda x: np.average( x['B_mean'], weights = x['A_mean'])
favor_unique = grouped.apply(get_favor)
favor_unique = pd.DataFrame(favor_unique)
favor_unique.columns = ['C']
favor_unique = favor_unique.sort_values(by = ['company','C'], ascending= False)

favor_unique = pd.DataFrame(favor_unique)
favor_unique = favor_unique[favor_unique['C'] > 0.25]
favor_unique = favor_unique.reset_index(['company','start_date','申万一级行业分类'])

favor_unique['偏股类基金近两年偏好行业标签'] = favor_unique['申万一级行业分类']
favor_unique = favor_unique.drop(['申万一级行业分类','C'],axis = 1)
#favor_unique即为最终标签



######################################################4、偏股类基金近三年偏好行业标签###########################################################
trade_dates_x = trade_dates[trade_dates['announce_date'] == date1]
trade_dates_y = trade_dates[trade_dates['announce_date'] == date2]
trade_dates_m = trade_dates[trade_dates['announce_date'] == date3]
trade_dates_n = trade_dates[trade_dates['announce_date'] == date4]
trade_dates_p = trade_dates[trade_dates['announce_date'] == date5]
trade_dates_q = trade_dates[trade_dates['announce_date'] == date6]

trade_dates_x = trade_dates_x[['company','start_date','基金代码','A','inv_stock_name','inv_stock_code_y','申万一级行业分类','B']]
trade_dates_y = trade_dates_y[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_y.columns = ['company','start_date','基金代码','A_1','inv_stock_name','B_1']
trade_dates_m = trade_dates_m[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_m.columns = ['company','start_date','基金代码','A_2','inv_stock_name','B_2']
trade_dates_n = trade_dates_n[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_n.columns = ['company','start_date','基金代码','A_3','inv_stock_name','B_3']
trade_dates_p = trade_dates_p[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_p.columns = ['company','start_date','基金代码','A_4','inv_stock_name','B_4']
trade_dates_q = trade_dates_q[['company','start_date','基金代码','A','inv_stock_name','B']]
trade_dates_q.columns = ['company','start_date','基金代码','A_5','inv_stock_name','B_5']

trade_dates_x = pd.merge( trade_dates_x, trade_dates_y, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_m, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_n, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_p, on = ['company','start_date','基金代码','inv_stock_name'])
trade_dates_x = pd.merge( trade_dates_x, trade_dates_q, on = ['company','start_date','基金代码','inv_stock_name'])

trade_dates_x['A_mean'] = trade_dates_x.apply(lambda x: (x['A']+x['A_1']+x['A_2']+x['A_3']+x['A_4']+x['A_5'])/6, axis = 1)
trade_dates_x['B_mean'] = trade_dates_x.apply(lambda x: (x['B']+x['B_1']+x['B_2']+x['B_3']+x['B_4']+x['B_5'])/6, axis = 1)

grouped = trade_dates_x.groupby(by = ['company','start_date','基金代码','申万一级行业分类'])
get_sum = lambda x:np.sum(x['B_mean'])
step_1 = grouped.apply(get_sum)
step_1 = pd.DataFrame(step_1)
middle = trade_dates_x[['company','start_date','A_mean','申万一级行业分类']]
step_1 = pd.merge( step_1, middle, on = ['company','start_date','申万一级行业分类'] )
step_1 = step_1.drop_duplicates(subset = ['company','申万一级行业分类'],keep = 'first')
step_1.columns = ['company','start_date','申万一级行业分类','B_mean','A_mean']
grouped = step_1.groupby(by = ['company','start_date','申万一级行业分类'])
get_favor = lambda x: np.average( x['B_mean'], weights = x['A_mean'])
favor_unique = grouped.apply(get_favor)
favor_unique = pd.DataFrame(favor_unique)
favor_unique.columns = ['C']
favor_unique = favor_unique.sort_values(by = ['company','C'], ascending= False)

favor_unique = pd.DataFrame(favor_unique)
favor_unique = favor_unique[favor_unique['C'] > 0.25]
favor_unique = favor_unique.reset_index(['company','start_date','申万一级行业分类'])

favor_unique['偏股类基金近三年偏好行业标签'] = favor_unique['申万一级行业分类']
favor_unique = favor_unique.drop(['申万一级行业分类','C'],axis = 1)
#favor_unique即为最终标签




#######################################################偏股类产品持股集中度###############################################################################################
#首先计算单支基金的股票投资市值
#季度更新
#日期设置为最近一个季报报告日期
date = '20181231'
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090,d.F4_1104
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT 
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F4_1104,F15_1104,F14_1104
        FROM wind.TB_OBJECT_1104
        WHERE F14_1104 = '%(date)s')d
        ON c.F1_1099 = d.F15_1104
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090

        '''% {'date':date}

cu = fund_db.cursor()
trade_dates1 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_date','基金代码','stock_value'])
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
data = data[['基金代码','六大类型']]
data = data[data['六大类型'] == '偏股类']
trade_dates1 = pd.merge(trade_dates1,data,on = '基金代码',how = 'right')
trade_dates1 = trade_dates1[trade_dates1['stock_value'] != 0]

#然后按公司对上一步加总，计算每个基金公司投资于股票部分的市值之和
trade_dates2 = trade_dates1.groupby(by = ['company', 'start_date'])
fx = lambda x: np.sum(x['stock_value'])
trade_dates2 = trade_dates2.apply(fx)
trade_dates2 = pd.DataFrame(trade_dates2)
trade_dates2.columns = ['sum_stock_value']
trade_dates2 = trade_dates2[trade_dates2['sum_stock_value'] != 0]

#对1、2两部分数据进行汇合加总
trade_dates = pd.merge( trade_dates1, trade_dates2, on = 'company',how = 'left')
trade_dates['A'] = trade_dates.apply(lambda x:x['stock_value']/x['sum_stock_value'], axis =1)

#计算各支基金的前十大重仓股
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090,f.OB_OBJECT_NAME_1090,f.F16_1090,f.F4_1102
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT 
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        m.F7_1102,m.F4_1102,n.OB_OBJECT_NAME_1090,m.F3_1102,n.F16_1090
        FROM
        (SELECT 
        g.F7_1102,g.F3_1102,g.F4_1102,g.F6_1102
        FROM 
        (SELECT ROW_NUMBER() OVER(PARTITION BY F7_1102 ORDER BY F4_1102 DESC)r, 
        p.F7_1102,p.F3_1102,p.F4_1102,p.F6_1102
        FROM  wind.TB_OBJECT_1102 p)g
        WHERE g.r<= 1000 AND g.F6_1102 = '%(date)s')m
        JOIN
        (SELECT
        F2_1090,OB_OBJECT_NAME_1090,F16_1090
        FROM
        wind.TB_OBJECT_1090
        )n
        ON m.F3_1102 = n.F2_1090 
        )f
        ON f.F7_1102 = c.F1_1099
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090

        '''% {'date':date}

cu = fund_db.cursor()
trade_dates3 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'date','基金代码','inv_stock_name','inv_stock_code','inv_stock_value'])

trade_dates = pd.merge( trade_dates, trade_dates3, on = '基金代码', how = 'left')
trade_dates['B'] = trade_dates.apply(lambda x:x['inv_stock_value']/x['stock_value'], axis = 1)
trade_dates['C'] = trade_dates.apply(lambda x:x['A']*x['B'], axis = 1)

path2 = 'C:/Users/wx/Desktop/模板/申万一级行业分类.xlsx'
catagory = pd.read_excel(path2)
category = catagory[['inv_stock_code']]

code_s = []
for i in category['inv_stock_code']:
    i = str(i)
    if len(i) < 6:
        i = '0' * (6 - len(i)) + i
    code_s.append(i)
category['inv_stock_code'] = code_s

trade_dates = pd.merge( trade_dates, category, on = 'inv_stock_code', how = 'right')

#trade_dates.to_excel(r'C:/Users/wx/Desktop/基金公司打标签1561478.xlsx')
#下面计算D(按照基金公司、TOP10中的股票进行分组并对C加和)
D = trade_dates.groupby(by = ['company_x','start_date','inv_stock_name','inv_stock_code'])['C'].sum()
#sum_C.to_excel(r'C:/Users/wx/Desktop/基金公司打标签1561478.xlsx')
#将上述D按照基金公司分组并排序选前三
D = pd.DataFrame(D)
D = D.sort_values(by = ['company_x','start_date','C'],ascending = False)
D = D.groupby(by = ['company_x','start_date']).head(10)
D = D.groupby(by = ['company_x','start_date'])['C'].sum()
D = pd.DataFrame(D)
D.columns = ['D']
D = D.reset_index(['company_x', 'start_date'])

# 根据D给基金公司打上‘持股集中’、‘持股平均’、‘持股分散’的标签
D.loc[D['D'] >= 0.7, '偏股类产品集中度标签'] = '持股集中'
D.loc[(D['D'] < 0.7) & (D['D'] > 0.3), '偏股类产品集中度标签'] = '持股平均'
D.loc[D['D'] <= 0.3, '偏股类产品集中度标签'] = '持股分散'

D = D.drop('D',axis = 1)
D.columns = ['company', 'start_date','偏股类产品集中度标签']
#D即为最终标签




########################################################偏股类产品前三重仓股######################################################
#首先计算单支基金的股票投资市值
#季度更新
#日期设置为最近一个季报报告日期
date = '20181231'
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090,d.F4_1104
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT 
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        F4_1104,F15_1104,F14_1104
        FROM wind.TB_OBJECT_1104
        WHERE F14_1104 = '%(date)s')d
        ON c.F1_1099 = d.F15_1104
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090

        '''% {'date':date}

cu = fund_db.cursor()
trade_dates1 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_date','基金代码','stock_value'])
sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '20181231'
'''
data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
data = data[['基金代码','六大类型']]
data = data[data['六大类型'] == '偏股类']
trade_dates1 = pd.merge(trade_dates1,data,on = '基金代码',how = 'right')
trade_dates1 = trade_dates1[trade_dates1['stock_value'] != 0]

#然后按公司对上一步加总，计算每个基金公司投资于股票部分的市值之和
trade_dates2 = trade_dates1.groupby(by = ['company', 'start_date'])
fx = lambda x: np.sum(x['stock_value'])
trade_dates2 = trade_dates2.apply(fx)
trade_dates2 = pd.DataFrame(trade_dates2)
trade_dates2.columns = ['sum_stock_value']
trade_dates2 = trade_dates2[trade_dates2['sum_stock_value'] != 0]

#对1、2两部分数据进行汇合加总
trade_dates = pd.merge( trade_dates1, trade_dates2, on = 'company',how = 'left')
trade_dates['A'] = trade_dates.apply(lambda x:x['stock_value']/x['sum_stock_value'], axis =1)

#计算各支基金的前十大重仓股
sql = '''
        SELECT
        c.OB_OBJECT_NAME_1018,c.F35_1018,e.F16_1090,f.OB_OBJECT_NAME_1090,f.F16_1090,f.F4_1102
        FROM
        (SELECT
        a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
        FROM
        (SELECT
        F12_1099,F1_1099
        FROM  wind.TB_OBJECT_1099
        WHERE F23_1099 IS NULL AND F22_1099 IS NOT NULL) a
        JOIN
        (SELECT 
        F34_1018,OB_OBJECT_NAME_1018,F35_1018
        FROM wind.TB_OBJECT_1018
        ORDER BY F35_1018 )b
        ON a.F12_1099 = b.F34_1018)c
        JOIN
        (SELECT
        m.F7_1102,m.F4_1102,n.OB_OBJECT_NAME_1090,m.F3_1102,n.F16_1090
        FROM
        (SELECT 
        g.F7_1102,g.F3_1102,g.F4_1102,g.F6_1102
        FROM 
        (SELECT ROW_NUMBER() OVER(PARTITION BY F7_1102 ORDER BY F4_1102 DESC)r, 
        p.F7_1102,p.F3_1102,p.F4_1102,p.F6_1102
        FROM  wind.TB_OBJECT_1102 p)g
        WHERE g.r<= 100 AND g.F6_1102 = '%(date)s')m
        JOIN
        (SELECT
        F2_1090,OB_OBJECT_NAME_1090,F16_1090
        FROM
        wind.TB_OBJECT_1090
        )n
        ON m.F3_1102 = n.F2_1090 
        )f
        ON f.F7_1102 = c.F1_1099
        JOIN
        (SELECT
        F2_1090,F16_1090
        FROM wind.TB_OBJECT_1090)e
        ON c.F1_1099 = e.F2_1090

        '''% {'date':date}

cu = fund_db.cursor()
trade_dates3 = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'date','基金代码','inv_stock_name','inv_stock_code','inv_stock_value'])

trade_dates = pd.merge( trade_dates, trade_dates3, on = '基金代码', how = 'left')
trade_dates['B'] = trade_dates.apply(lambda x:x['inv_stock_value']/x['stock_value'], axis = 1)
trade_dates['C'] = trade_dates.apply(lambda x:x['A']*x['B'], axis = 1)

path2 = 'C:/Users/wx/Desktop/模板/申万一级行业分类.xlsx'
catagory = pd.read_excel(path2)
category = catagory[['inv_stock_code']]

code_s = []
for i in category['inv_stock_code']:
    i = str(i)
    if len(i) < 6:
        i = '0' * (6 - len(i)) + i
    code_s.append(i)
category['inv_stock_code'] = code_s

trade_dates = pd.merge( trade_dates, category, on = 'inv_stock_code', how = 'right')

#trade_dates.to_excel(r'C:/Users/wx/Desktop/基金公司打标签1561478.xlsx')
#下面计算D(按照基金公司、TOP10中的股票进行分组并对C加和)
D = trade_dates.groupby(by = ['company_x','start_date','inv_stock_name','inv_stock_code'])['C'].sum()
#sum_C.to_excel(r'C:/Users/wx/Desktop/基金公司打标签1561478.xlsx')
#将上述D按照基金公司分组并排序选前三
D = pd.DataFrame(D)
D = D.sort_values(by = ['company_x','start_date','C'],ascending = False)
D = D.groupby(by = ['company_x','start_date']).head(3)
D = pd.DataFrame(D)
D = D.reset_index(['company_x', 'start_date', 'inv_stock_name', 'inv_stock_code'])

#下面进行打标签
label = pd.DataFrame({'company':[],'偏股类基金第一重仓股':[],'偏股类基金第二重仓股':[],'偏股类基金第三重仓股':[]})
company = D['company_x'].drop_duplicates()
label['company'] = company
label = label.reset_index(drop = True)
for i in company:
    D_subset = D[D['company_x'] == i]
    D_subset = D_subset.reset_index(drop = True)
    m,n = D_subset.shape
    if m == 3:
        label.loc[label['company'] == i, '偏股类基金第一重仓股'] = D_subset['inv_stock_name'][0]
        label.loc[label['company'] == i, '偏股类基金第二重仓股'] = D_subset['inv_stock_name'][1]
        label.loc[label['company'] == i, '偏股类基金第三重仓股'] = D_subset['inv_stock_name'][2]
label = label.dropna()
label = label.reset_index(drop = True)
#label即为最终结果
"""








######################################################################################################
#以下为财务概况部分标签
######################################################################################################

#####################################################################净资产排名标签################################################################################
#只需改动F14_1104（截止日期）为'20161231','20171231','20181231'即可得到年报数据
def value_rank(str1 = '2018'):
#str1可设置为不同年份，如：‘2017’、‘2016’
#利用str1获得不同年份年报数据
    str2 = '1231'
    date = str1 + str2
    sql = '''
    
            SELECT
            c.OB_OBJECT_NAME_1018,c.F35_1018,SUM(d.F3_1104)
            FROM
            (SELECT
            a.F1_1099,b.OB_OBJECT_NAME_1018,b.F35_1018,a.F12_1099
            FROM
            (SELECT
            F12_1099,F1_1099
            FROM  wind.TB_OBJECT_1099) a
            JOIN
            (SELECT 
            F34_1018,OB_OBJECT_NAME_1018,F35_1018
            FROM wind.TB_OBJECT_1018
            ORDER BY F35_1018 )b
            ON a.F12_1099 = b.F34_1018)c
            JOIN
            (SELECT
            F3_1104,F15_1104,F14_1104
            FROM wind.TB_OBJECT_1104
            WHERE F14_1104 = '%(date)s')d
            ON c.F1_1099 = d.F15_1104
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018
    
            '''% {'date':date}

    cu = fund_db.cursor()
    value = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date','sum_value'])

    sum_value = value['sum_value']
    high = np.percentile(sum_value, 90)
    mid_high = np.percentile(sum_value, 70)
    mid_low = np.percentile(sum_value, 30)
    low = np.percentile(sum_value, 10)

    # 给基金公司排名并加序号
    rank = pd.DataFrame(sum_value).rank()
    rank.columns = ['temp']
    value = pd.concat([value, rank], axis = 1)
    # 将序号全部变为int格式,再转为字符串
    list_rank = []
    for num in value['temp']:
        num = int(num)
        num = str(num)
        list_rank.append(num)
    value['temp'] = list_rank

    # 根据分位数排名给基金公司打上‘高’、‘中’、‘低’的标签
    value.loc[value['sum_value'] >= high, str1 + '年度净资产行业排名标签'] = '高-'
    value.loc[(value['sum_value'] < high) & (value['sum_value'] >= mid_high), str1 + '年度净资产行业排名标签'] = '中高-'
    value.loc[(value['sum_value'] < mid_high) & (value['sum_value'] >= mid_low), str1 + '年度净资产行业排名标签'] = '中-'
    value.loc[(value['sum_value'] < mid_low) & (value['sum_value'] >= low), str1 + '年度净资产行业排名标签'] = '中低-'
    value.loc[value['sum_value'] < low, str1 + '年度净资产行业排名标签'] = '低-'

    # 标签合并
    value[str1 + '年度净资产行业排名标签'] = value[str1 + '年度净资产行业排名标签'] + value['temp']
    value = value.drop([ 'temp', 'sum_value'], axis = 1)
    return(value)


#################################################2、本公司任职三年以上、五年以上、十年以上的基金经理比例####################################################################
rec = []
sql_cor = '''select distinct f34_1018, OB_OBJECT_NAME_1018 
from wind.tb_object_1018 x inner join wind.tb_object_1099 on f12_1099 = f34_1018 
'''
cor_name = pd.DataFrame(cu.execute(sql_cor).fetchall(),columns=['公司id','基金公司名称'])
sql_in = '''insert into t_fof_tag_info(fundid,tagid,tagvalue,batchno) values(:1,:2,:3,:4)
'''
#只需设置lag = 3 或 5 或 10即可
for i in [3,5,10]:
	date=OVER_YEARS(i)
	date.to_excel(path+r'本公司任职%d年以上的基金经理比例.xlsx'%i)
	date.columns=['基金公司名称','成立日期','指标值','标签']
	date=pd.merge(date,cor_name,on='基金公司名称',how='inner')
	date = date[['公司id','标签']]
	date['指数代码']='over%dyearsmanagerrate'%i
	date['报告期'] = rptdate
	date['标签'] = date['标签'].str[:3]+'本公司任职%d年以上的基金经理比例'%i
	date = date[['公司id','指数代码','标签','报告期']]
	rec = [tuple(x) for x in date.values]
	cu_pra.executemany(sql_in,rec)
	fund_dbpra.commit()
	print(date.columns)
#################################################1、近一年、三年新增基金经理人数标签####################################################################
# 只需设置lag = 3 或 1

for i in [1,3]:
	date = NEW_MANAGERS(i)
	date.to_excel(path + r'近%d年新增基金经理人数标签.xlsx' % i)
	date.columns=['基金公司名称','成立日期','指标值','标签']
	date=pd.merge(date,cor_name,on='基金公司名称',how='inner')
	date = date[['公司id','标签']]
	date['指数代码']='last%dnewmanagernum'%i
	date['报告期'] = rptdate
	date['标签'] = date['标签'].str[:3]+'近%d年新增基金经理人数'%i
	date = date[['公司id','指数代码','标签','报告期']]
	
	rec = [tuple(x) for x in date.values]
	cu_pra.executemany(sql_in,rec)
	fund_dbpra.commit()
	print(date.columns)
#################################################2、近一年、三年离职基金经理人数标签####################################################################
# 只需设置lag = 3 或 1

for i in [1, 3]:
	date = OFF_MANAGERS(i)
	date.to_excel(path + r'近%d年离职基金经理人数标签.xlsx' % i)
	date.columns=['基金公司名称','成立日期','标签']
	date=pd.merge(date,cor_name,on='基金公司名称',how='inner')
	date = date[['公司id','标签']]
	date['指数代码']='last%dquitmanagernum'%i
	date['报告期'] = rptdate
	date['标签'] = date['标签'].str[:3]+'近%d年离职基金经理人数'%i
	date = date[['公司id','指数代码','标签','报告期']]
	
	rec = [tuple(x) for x in date.values]
	cu_pra.executemany(sql_in,rec)
	fund_dbpra.commit()
	print(date.columns)
#################################################3、近一年、三年基金经理离职率标签####################################################################
# 只需设置lag = 3 或 1
# def OFF_MANAGERS(lag=3):
for i in [1, 3]:
	date = OFF_MANAGERS_PER(i)
	date.to_excel(path + r'近%d年基金经理离职率标签.xlsx' % i)
	date.columns=['基金公司名称','成立日期','指标值','标签']
	date=pd.merge(date,cor_name,on='基金公司名称',how='inner')
	date = date[['公司id','标签']]
	date['指数代码']='last%dquitmanagerrate'%i
	date['报告期'] = rptdate
	date['标签'] = date['标签'].str[:3]+'近%d年基金经理离职率'%i
	date = date[['公司id','指数代码','标签','报告期']]
	
	rec = [tuple(x) for x in date.values]
	cu_pra.executemany(sql_in,rec)
	fund_dbpra.commit()
	print(date.columns)

#################################################4、近一年、三年基金经理发生更换的产品比率标签####################################################################
# 只需设置lag = 3 或 1
# 首先取出今日日期
# def SUM_PRODUCT(lag = 1):
for i in [1, 3]:
	date = OFF_MANAGERS_PER(i)
	date.to_excel(path + r'近%d年基金经理发生更换的产品比率标签.xlsx' % i)
	date.columns=['基金公司名称','成立日期','指标值','标签']
	date=pd.merge(date,cor_name,on='基金公司名称',how='inner')
	date = date[['公司id','标签']]
	date['指数代码']='last%dchangemanagerrate'%i
	date['报告期'] = rptdate
	date['标签'] = date['标签'].str[:3]+'近%d年基金经理发生更换的产品比率'%i
	date = date[['公司id','指数代码','标签','报告期']]
	
	rec = [tuple(x) for x in date.values]
	cu_pra.executemany(sql_in,rec)
	fund_dbpra.commit()
	print(date.columns)
#只需改动F14_1104（截止日期）为'20161231','20171231','20181231'即可得到年报数据
# def value_rank(str1 = '2018'):
#str1可设置为不同年份，如：‘2017’、‘2016’
for i in ['2018','2017','2016']:
	date = value_rank(i)
	date.to_excel(path + r'%s年净资产排名标签.xlsx' % i)
	date.columns=['基金公司名称','成立日期','标签']
	date=pd.merge(date,cor_name,on='基金公司名称',how='inner')
	date = date[['公司id','标签']]
	date['指数代码']='%dyearvaluerate'%i
	date['报告期'] = rptdate
	date['标签'] = date['标签'].str[:3]+'%d年净资产排名'%i
	date = date[['公司id','指数代码','标签','报告期']]
	
	rec = [tuple(x) for x in date.values]
	cu_pra.executemany(sql_in,rec)
	fund_dbpra.commit()
	print(date.columns)
	
#基金公司偏股类基金总规模行业排名标签	
print(res_sum_stock.columns)
res_sum_stock.columns = ['基金公司名称','成立日','标签']
res = pd.merge(res_sum_stock,cor_name,on='基金公司名称',how='inner')
res['指数代码']='fundstockscale'
res['报告期'] = rptdate
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#基金公司偏债类基金总规模行业排名标签
print(res_sum_bond.columns)
res_sum_bond.columns = ['基金公司名称','成立日','标签']
res = pd.merge(res_sum_bond,cor_name,on='基金公司名称',how='inner')
res['指数代码']='fundbondscale'
res['报告期'] = rptdate
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#基金公司货币基金总规模行业排名标签
print(res_sum_monetary.columns)
res_sum_monetary.columns = ['基金公司名称','成立日','标签']
res = pd.merge(res_sum_monetary,cor_name,on='基金公司名称',how='inner')
res['指数代码']='fundmonetaryscale'
res['报告期'] = rptdate
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#基金公司QDII基金总规模行业排名标签
print(res_sum_QDII.columns)
res_sum_monetary.columns = ['基金公司名称','成立日','标签']
res = pd.merge(res_sum_monetary,cor_name,on='基金公司名称',how='inner')
res['指数代码']='fundQDIIscale'
res['报告期'] = rptdate
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#主打产品类型标签（数量维度二级分类）
print(res_company_class2_num.columns)
res_company_class2_num.columns = ['基金公司名称','标签']
res = pd.merge(res_company_class2_num,cor_name,on='基金公司名称',how='inner')
res['指数代码']='fundcompanyclass2num'
res['报告期'] = rptdate
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()
#主打产品类型标签（规模维度二级分类）
print(res_company_class2.columns)
res_company_class2.columns = ['基金公司名称','标签']
res = pd.merge(res_company_class2,cor_name,on='基金公司名称',how='inner')
res['指数代码']='fundcompanyclass2scale'
res['报告期'] = rptdate
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#擅长产品类型标签 未打标签
print(res_fund_data.columns)


#一级分类和二级分类产品表现标签
print(res_SHI_tops.columns)
res_SHI_tops.columns = ['标签','基金公司名称','成立日']
res = pd.merge(res_sum_monetary,cor_name,on='基金公司名称',how='inner')
res['指数代码']='class2performance'
res['报告期'] = rptdate
res['标签'] = '主打'+res['标签']+'分类基金'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#机构持有标签
#def get_inst_tag(x):
#	x['权重'] = x['基金规模']/x['基金规模'].sum()
#	x['加权机构持有比例'] = x['机构持有比例']*x['权重']
#	all = x['加权机构持有比例'].sum()
#	x['标签'] = '高-基金资产机构持有比例' if  all> 0.8 else all < 0.3 '低-基金资产机构持有比例' else '中-基金资产机构持有比例'
	
#print(res_institution_hold.columns)


#总基金资产机构持有比例
print(res_whole_sum.columns)
res_whole_sum.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_whole_sum,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companyallinstitutionhold'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'总基金资产机构持有比例'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#非货币基金基金资产机构持有比例
print(res_sum_non_monetary.columns)
res_sum_non_monetary.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_sum_non_monetary,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companynonmonetaryinstitutionhold'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'非货币基金基金资产机构持有比例'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#偏股类基金资产机构持有比例
print(res_sum_stock_inst_hold.columns)
res_sum_non_monetary.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_sum_non_monetary,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companystockinstitutionhold'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'偏股类基金资产机构持有比例'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#偏债类基金资产机构持有比例
print(res_sum_bond_inst_hold.columns)
res_sum_bond_inst_hold.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_sum_bond_inst_hold,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companybondinstitutionhold'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'偏债类基金资产机构持有比例'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#货币基金基金资产机构持有比例
print(res_sum_monetary_inst_hold.columns)
res_sum_monetary_inst_hold.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_sum_monetary_inst_hold,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companymonetaryinstitutionhold'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'货币基金基金资产机构持有比例'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#QDII基金基金资产机构持有比例
print(res_sum_QDII_inst_hold.columns)
res_sum_QDII_inst_hold.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_sum_QDII_inst_hold,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companyQDIIinstitutionhold'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'QDII基金机构持有比例'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#基金公司基金经理总人数标签
print(res_sum_manager.columns)
res_sum_manager.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_sum_manager,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companyallmanagernum'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'基金经理总人数'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#基金公司基金经理平均管理年数标签
print(res_mean_manage_years.columns)
res_mean_manage_years.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_mean_manage_years,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companymanageryears'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'基金经理平均管理年限'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

#基金公司基金经理最大管理年限标签
print(res_max_manage_years.columns)
res_max_manage_years.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_max_manage_years,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companymanagermaxyears'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'基金经理最长管理年限'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()
#
print(res_within_3years.columns)
res_within_3years.columns=['基金公司名称','成立日','标签']
res = pd.merge(res_within_3years,cor_name,on='基金公司名称',how='inner')
res['指数代码']='companywithin3yearsmanagernum'
res['报告期'] = rptdate
res['标签'] = res['标签'].str[:3]+'本公司任职3年以内基金经理比例'
res = res[['公司id','指数代码','标签','报告期']]
rec = [tuple(x) for x in res.values]
cu_pra.executemany(sql_in,rec)
fund_dbpra.commit()

fund_dbpra.close()
	
#res_sum_stock
#res_sum_bond
#res_sum_monetary
#res_sum_QDII
#res_company_class2
#res_fund_data
#res_SHI_tops
#res_institution_hold
#res_whole_sum
#res_sum_non_monetary
#res_sum_stock_inst_hold
#res_sum_bond_inst_hold
#res_sum_monetary_inst_hold
#res_sum_QDII_inst_hold
#res_sum_manager
#res_manage_date
#res_mean_manage_years
#res_max_manage_years
#res_within_3years