# encoding=utf-8
"""
增强型指数基金 跟踪误差&信息比率
"""
import cx_Oracle
import pandas as pd
from functions import ToolClass, get_values_auto_fill
from datetime import datetime, timedelta
from WindPy import w
import numpy as np

w.start()

INDEX_START = '20091201'
ENDDATE = '20190628'

N = np.sqrt(250)

[userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)

cu = fund_db.cursor()


def get_track_index(stock_index):
    """
    功能
    --------
    找到某指数的收盘价数据,对于不同类型的指数，有不同的数据来源，此函数将之前定义的函数综合起来

    参数
    --------
    stock_index:基金代码，字符格式，如'000001

    返回值
    --------
    返回一个dataframe,index为日期，有一列为指数收盘价，列名为指数代码

    参看
    --------
    market_fetch()：从数据库取A股指数的收盘价数据
    get_market_wind()：从wind客户端取任意指数数据
    market_fetch_CBA()：从数据库取中债指数收盘价数据
    market_fetch_Hongkong()：从数据库取港股指数收盘价数据

    示例
    --------
    >>>get_track_index('000300.SH').head()
           000300
日期
20020104  1316.455
20020107  1302.084
20020108  1292.714
20020109  1272.645
20020110  1281.261
     """
    i = stock_index
    if i == None:
        market = market_fetch('h11009')
    elif i[-3:] in ['.SH', '.SZ', 'CSI', '.MI']:
        if i == 'RMS.MI':
            market = get_market_wind(i)
        else:
            stock = i[0:6]
            market = market_fetch(stock)
    elif i[-3:] == '.CS':
        stock = i[0:8]
        market = market_fetch_CBA(stock)
    elif i[-3:] == '.HI':
        market = market_fetch_Hongkong(i)
    else:
        market = get_market_wind(i)
    if market.empty:
        market = get_market_wind(i)
        print(str(i) + '指数没有取到,使用客户端取数')
    return market


def get_market_wind(code):
    """
        功能
        --------
        从wind客户端提取指数的从2015年1月以来的全部收盘价数据，一些美股的指数和全球指数数据库中没有，需要从wind客户端取数据

        参数
        --------
        code:指数代码，字符格式，如'000300'，不带后面的后缀

        返回值
        --------
        返回一个dataframe,只有一列，是指数收盘价数据，列名为指数代码，index是日期

        参看
        --------
        无关联函数

        示例
        --------
        >>>get_market_wind("892400.MI").head()
            892400.MI
    20150105  408.010446
    20150106  404.051019
    20150107  406.057610
    20150108  413.842395
    20150109  411.687899
         """

    df = w.wsd(code, "close", INDEX_START, ENDDATE, "", "Currency=CNY", usedf=True)[1]
    # df = w.wsd(code, "close", "2015-01-01", end_date, "","Currency=CNY", usedf=True)[1]
    df.index = pd.Series(df.index).apply(lambda x: str(x)[:4] + str(x)[5:7] + str(x)[8:10])
    df.columns = [code]
    return df


# 取市场指数数据
def market_fetch(stock_index):
    """
    功能
    --------
    得到某A股指数的全部收盘价数据

    参数
    --------
    stock_index:指数代码，字符格式，如'000300'，不带后面的后缀

    返回值
    --------
    返回一个dataframe,只有一列，是指数收盘价数据，列名为指数代码，index是日期

    参看
    --------
    无关联函数

    示例
    --------
    >>>market_fetch('000300').head()
            000300
    日期
    20020104  1316.455
    20020107  1302.084
    20020108  1292.714
    20020109  1272.645
    20020110  1281.261
     """
    sql1 = '''
    SELECT
      T0.F2_1425 日期,
      T0.F7_1425 AS 复权收盘价
      FROM
        wind.TB_OBJECT_1425 T0
      LEFT JOIN wind.TB_OBJECT_1090 T1 ON T1.F2_1090 = T0.F1_1425
      WHERE
        T1.F16_1090 = '%(index_code)s'
      AND T1.F4_1090 = 'S'
      ORDER BY
        T0.F2_1425
    ''' % {'index_code': stock_index}
    market = pd.DataFrame(cu.execute(sql1).fetchall(), columns=['日期', '指数收盘价'])
    market.index = market['日期']
    del market['日期']
    market.columns = [stock_index]
    return market


def market_fetch_CBA(stock_index):
    """
        功能
        --------
        得到某中债指数的全部收盘价数据

        参数
        --------
        stock_index:指数代码，字符格式，如'CBA00111'，不带后面的后缀

        返回值
        --------
        返回一个dataframe,只有一列，是指数收盘价数据，列名为指数代码，index是日期

        参看
        --------
        无关联函数

        示例
        --------
        >>>market_fetch_CBA('CBA00111').head()
            CBA00111
日期
20020104   99.9642
20020107   99.9349
20020108  100.2792
20020109  100.4043
20020110  100.4195
         """
    sql1 = '''
        SELECT
          T0.F2_1655 日期,
          T0.F3_1655 AS 复权收盘价
          FROM
            wind.TB_OBJECT_1655 T0
          LEFT JOIN wind.TB_OBJECT_1090 T1 ON T1.F2_1090 = T0.F1_1655
          WHERE
            T1.F16_1090 = '%(index_code)s'
          AND T1.F4_1090 = 'S'
          ORDER BY
            T0.F2_1655
        ''' % {'index_code': stock_index}
    market = pd.DataFrame(cu.execute(sql1).fetchall(), columns=['日期', '指数收盘价'])
    market.index = market['日期']
    del market['日期']
    market.columns = [stock_index]
    return market


def market_fetch_Hongkong(stock_index):
    """
       功能
       --------
       得到某港股指数的全部收盘价数据

       参数
       --------
       stock_index:指数代码，字符格式，如'HSI.HI'

       返回值
       --------
       返回一个dataframe,只有一列，是指数收盘价数据，列名为指数代码，index是日期

       参看
       --------
       无关联函数

       示例
       --------
       >>>market_fetch_Hongkong('HSI.HI').head()
               HSI.HI
    日期
    19640731  100.00
    19640831   98.81
    19640930  101.21
    19641130  101.42
    19641231  101.45
        """
    sql1 = '''
    SELECT
      T0.G2_1038 日期,
      T0.G7_1038 AS 收盘价
      FROM
        wind.GB_OBJECT_1038 T0
      LEFT JOIN wind.GB_OBJECT_1001 T1 ON T1.G1_1001 = T0.G1_1038
      WHERE
        T1.G16_1001 = '%(index_code)s'
      ORDER BY
        T0.G2_1038
    ''' % {'index_code': stock_index}
    market = pd.DataFrame(cu.execute(sql1).fetchall(), columns=['日期', '指数收盘价'])
    market.index = market['日期']
    del market['日期']
    market.columns = [stock_index]
    return market


def tracking_err(arr1: np.array, arr2: np.array, n):
    """
        跟踪误差
    :param arr1: 产品收益率
    :param arr2: 指标收益率
    :param frep: 频率
    :return:
    """

    res = (arr1 - arr2).std() * n
    return res


# @jit
def information_ratio(arr1: np.array, arr2: np.array, tack_err: float, n):
    """
    信息比率
    :param arr1: 产品收益率
    :param arr2: 指标收益率
    :param frep: 频率
    :return:
    """
    # print((1 + arr1).prod())
    # print((1 + arr2).prod())
    # print('tracking_err:',tracking_err(arr1, arr2, n))
    # res = ((1 + arr1).prod() - (1 + arr2).prod()) / tracking_err(arr1, arr2, n)
    res = (arr1 - arr2).mean() / tack_err * n
    return res


class ZenQiangZhiShu(ToolClass):
    """计算类"""

    def __init__(self):
        super(ZenQiangZhiShu, self).__init__()

    def get_pre_trade_day(self, today):
        """获取前一个交易日"""
        sql_get_pre_week_trade_day = f"""
        select f1_1010 from wind.tb_object_1010
        where f1_1010 <={today}   and rownum <=1
        order by f1_1010 desc 
        """

        res = self._get_db_data(sql_get_pre_week_trade_day, db='reader')
        return res[0][0]

    def get_zzjj(self, start_date: str, end_date: str):
        """
        获取增强型指数基金
        :return:
        """

        sql = f"""
        
        select cpdm,rptdate from t_fund_classify_his where  SFZSJJ = '是（增强）'
        and rptdate >={start_date} and rptdate <= {end_date}
                
        """

        sql_res = self._get_db_data(sql, db='pra')
        df = pd.DataFrame(sql_res, columns=['cpdm', 'rptdate'])
        return df

    def get_all_bond(self,start_date:str,end_date:str):
        """
        获取全部的基金
        :return:
        """

        sql = f"""

                select cpdm,rptdate from t_fund_classify_his where yjfl = '债券型' and rptdate >={start_date} and rptdate <= {end_date}

                """

        sql_res = self._get_db_data(sql, db='pra')
        df = pd.DataFrame(sql_res, columns=['cpdm', 'rptdate'])
        return df


    def get_index(self, codes: list, reportdate: str):
        reportdate = self.get_pre_trade_day(reportdate)
        df = w.wsd(tuple(list(map(lambda x: x + '.OF', codes))), "fund_trackindexcode", reportdate, reportdate).Data
        # print(df)
        return df

    def add_index(self, df: pd.DataFrame):
        """
        向df中添加 跟踪指标 列
        :param df:
        :return:
        """
        # 补充跟踪指标
        report_list = df['rptdate'].tolist()
        report_list = list(set(report_list))
        df_fund_index = None

        for report in report_list:
            df_i = df.loc[df['rptdate'] == report][:]
            codes = df_i['cpdm'].tolist()
            t = str(int(report[:4]) + 1) + '1231' if report[:4] !='2018' else ENDDATE
            indexes = self.get_index(codes, t)

            df_i['跟踪指标'] = indexes[0]
            df_fund_index = df_i.append(df_fund_index)

        return df_fund_index

    def get_index_return(self, df: pd.DataFrame):
        """
        计算所有指标的收益率，存在字典里面
        :param df:
        :return:
        """
        # 计算指标收益率,放在字典里面
        index_list = df['跟踪指标'].drop_duplicates().tolist()
        df_index_return = {}
        total_number = len(index_list)
        has_been = 0
        for index in index_list:
            has_been += 1
            print(f'\r读取指标净值{index}，进度：{has_been}/{total_number}', end='')
            index_return = get_track_index(index)
            index_return = index_return.pct_change()
            index_return.columns = ['收益率']
            index_return.dropna(subset=['收益率'], inplace=True)
            index_return = index_return[INDEX_START:ENDDATE]
            index_return['跟踪指标'] = index
            # print(index_return)
            df_index_return[str(index)] = index_return

        return df_index_return

    def get_fund_return(self, df: pd.DataFrame):
        """
        用for循环去基金的所有收益率，同时存入字典
        :param df:
        :return:
        """
        code_list = df['cpdm'].drop_duplicates().tolist()
        res_dict = {}
        total_number = len(code_list)
        has_been = 0
        for code in code_list:
            has_been += 1
            print(f'\r读取基金净值{code}，进度：{has_been}/{total_number}', end='')
            fund_return = get_values_auto_fill(code, INDEX_START, ENDDATE)
            # print(fund_return)
            res_dict[code] = fund_return
        return res_dict

    def track_err(self, df: pd.DataFrame, funds_return: dict, indexs_return: dict):
        """
        想表中添加跟踪误差
        :param df: 需要添加的表
        :param fund_return: 基金收益率字典
        :param index_return: 指标收益率字典
        :return:
        """
        # print(df)
        track_err_res = []
        information_ratio_res = []
        total_number = len(df)
        has_been = 0
        for v in df.values:
            has_been += 1
            print(f'\r计算跟踪误差和信息比率，进度：{has_been}/{total_number}',end='')
            code = v[0]
            year = v[2]
            index = v[3]
            start_date = year + '0101'
            end_date = year + '1231'
            # start_date = '20160630'
            # end_date = '20190630'


            fund_return = funds_return[code]
            fund_return.reset_index(inplace=True)
            # print(fund_return)
            fund_return.set_index('交易日期', inplace=True)
            fund_return = fund_return[start_date:end_date]
            fund_return.reset_index(inplace=True)
            # print(fund_return)
            # print('*'*50)
            index_return = indexs_return[index]
            index_return = index_return[start_date:end_date]
            index_return.reset_index(inplace=True)
            index_return.columns = ['交易日期','收益率','跟踪指标']
            # index_return.rename(columns = {'日期':'交易日期'},inplace=True)
            # print(index_return)
            df_temp = pd.merge(fund_return,index_return,how='left',on='交易日期')
            # print(df_temp)
            fund_return_arr = df_temp['收益率_x'].values
            index_return_arr = df_temp['收益率_y'].values

            track_err = tracking_err(fund_return_arr, index_return_arr, N)
            information_ratio_value = information_ratio(fund_return_arr, index_return_arr, track_err, N)*N
            # print(track_err)
            # print(information_ratio_value)
            track_err_res.append(track_err)
            information_ratio_res.append(information_ratio_value)

        df['跟踪误差'] = track_err_res
        df['信息比率'] = information_ratio_res
        return df

    def run(self):
        """
        主函数(作废)
        :return:
        """
        start_date = '20091231'
        end_date = '20181231'
        df_zqzs = self.get_zzjj(start_date, end_date)
        df_zqzs['year'] = df_zqzs['rptdate'].apply(lambda x: str(int(x[:4]) + 1))
        # 补充跟踪指标
        df_fund_index = self.add_index(df_zqzs)
        # 计算指标收益率,放在字典里面
        df_index_return = self.get_index_return(df_fund_index)

        # 这里开始按在用fro循环计算每个基金的收益率，存在字典里面
        df_fund_return = self.get_fund_return(df_fund_index)
        # 计算每个基金在每个周期内的跟踪误差和信息比率
        df_fund_index = self.track_err(df_fund_index, df_fund_return, df_index_return)

        df_fund_index.to_excel('10年以来的跟踪误差及信息比率.xlsx')
        # df_fund_index['跟踪误差'] =

        # self.get_index(['040002','163808'],'20091231')

    def run002(self):

        """
        主函数
        :return:
        """
        start_date = '20181231'
        end_date = '20181231'
        df_zqzs = self.get_all_bond(start_date, end_date)

        df_zqzs['year'] = df_zqzs['rptdate'].apply(lambda x: str(int(x[:4]) + 1))
        print(df_zqzs)
        # 补充跟踪指标
        df_fund_index = self.add_index(df_zqzs)
        # 计算指标收益率,放在字典里面
        df_index_return = self.get_index_return(df_fund_index)

        # 这里开始按在用fro循环计算每个基金的收益率，存在字典里面
        df_fund_return = self.get_fund_return(df_fund_index)
        # 计算每个基金在每个周期内的跟踪误差和信息比率
        df_fund_index = self.track_err(df_fund_index, df_fund_return, df_index_return)

        df_fund_index.to_excel('债券型近三年的跟踪误差及信息比率.xlsx')






def run():
    zqzs = ZenQiangZhiShu()

    zqzs.run002()


if __name__ == '__main__':
    run()
    # aaa = get_track_index('000852.SH')
    # print(aaa)
