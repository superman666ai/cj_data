import numpy as np
import pandas as pd
#
arr1 = [1,2,3]
arr2 = [1,np.nan,3]
d = {
    'a':arr1,
    'b':arr2



}

df = pd.DataFrame(d)
print(df)

print(df['b'].isna().any())



# arr1 = np.random.randint(1,100,(5,3))
# print(arr1)
# df = pd.DataFrame(arr1,columns=['a','b','c'])
#
# df.ix[1,:-1] = np.nan
# df.ix[1:-1,0] = np.nan
# df.dropna(inplace=True,how='any',axis=0)
# print(df)

# import cx_Oracle
#
#
# [userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
# fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
# cu = fund_db.cursor()