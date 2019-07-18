# encoding=utf-8
"""
增强型指数基金 跟踪误差&信息比率
"""
import cx_Oracle
import pandas as pd
from functions import ToolClass, get_values_auto_fill
from datetime import datetime
# from WindPy import w
import numpy as np
from sql import sql_oracle

# w.start()

INDEX_START = '20091201'
ENDDATE = '20190628'

sql_tag_insert = "INSERT INTO fund_tag_info( fundId, tagId, tagValue, algid, batchno, endableFlag, createUser, createDate ) VALUES(:fundId, :tagId, :tagValue, :algid, :batchno, :endableFlag, :createuser, :createDate)"
SQL_INDEX_INSERT = "INSERT INTO fund_index_info( fundId, indexCode, indexValue, reportdate, algid, batchno,  createUser, createDate ) VALUES(:fundId, :indexCode, :indexValue,:reportdate, :algid, :batchno, :createuser, :createDate)"



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

class DBSession(object):
    def __init__(self, sql_string='', batch_size=500):
        self.cu_wind = sql_oracle.cu
        self.cu_pra = sql_oracle.cu_pra_sel
        self.fund_db_pra = sql_oracle.fund_db_pra
        self.rec_list = []
        self.sql_string = sql_string
        self.batch_size = 500

    def __del__(self):
        self.finish()

    def finish(self):
        if self.rec_list:
            self.db_insert(self.sql_string, self.rec_list)
            self.rec_list = []
        return

    def add_info(self, rec):
        self.rec_list.append(rec)
        if len(self.rec_list) > self.batch_size:
            self.db_insert(self.sql_string, self.rec_list)
            self.rec_list = []
        return

    def db_insert(self, sql, rec):
        try:
            self.cu_pra.prepare(sql)
            self.cu_pra.executemany(None, rec)
            self.fund_db_pra.commit()
            # print('insert suc')
        except cx_Oracle.DatabaseError as e:
            self.fund_db_pra.rollback()
            # 其他错误处理
            raise (e)

    def DBInsert(self, sql, rec):
        try:
            self.cu_pra.prepare(sql)
            self.cu_pra.executemany(None, rec)
            self.fund_db_pra.commit()
            # print('insert suc')
        except cx_Oracle.DatabaseError as e:
            self.fund_db_pra.rollback()
            # 其他错误处理
            raise (e)


class RunSeasonLable(object):
    def __init__(self):
        self.end_date = '20190611'
        self._init_db_info()
        # 生成要跑的日期序列
        # self._init_report_date()
        self.cu_wind = sql_oracle.cu
        self.cu_pra = sql_oracle.cu_pra_sel
        return

    def _init_db_info(self):
        # 当前算法id
        self.algid = '0000000003'
        # 当天日期
        self.todaydate = datetime.datetime.now().strftime('%Y%m%d')
        # 当前文件名称
        self.thisfilename = os.path.basename(__file__)
        self.db_insert_session = DBSession(SQL_INDEX_INSERT)
        return

    def _init_report_date(self):
        # 取出前3年日前, 与单前日期进行组合
        # self.run_date_list = [(20171231, 20141231, 20151231, 20161231)...]
        # self.half_year_date_list = [(20171231, 20170630)...]
        if not self.end_list:
            raise Exception('lack of args: end_list')
        t_list = [pd.date_range(end=i, periods=4, freq='12M', closed='left').strftime('%Y%m%d').tolist() for i in
                  self.end_list]
        r_list = map(lambda x, y: [x] + y, self.end_list, t_list)
        self.run_date_list = list(r_list)

        t_list = [pd.date_range(end=i, periods=2, freq='6M', closed='left').strftime('%Y%m%d').tolist() for i in
                  self.end_list]
        r_list = map(lambda x, y: [x] + y, self.end_list, t_list)
        self.half_year_date_list = list(r_list)
        return

    def _set_end_list(self, input_endlist):
        self.end_list = input_endlist
        self._init_report_date()
        return

    def get_fund_code_by_filter(self, input_filter=[], rptdate='20190331'):
        if not filter:
            return []
        # ejfl_filter_str = str(tuple(input_filter))
        ejfl_filter_str = ','.join(["'" + i + "'" for i in input_filter])
        lastday = datetime.datetime.now() + datetime.timedelta(days=-1)
        lastday = lastday.strftime('%Y%m%d')
        this_year = lastday[0:4]
        if rptdate[0:4] == this_year:
            rptdate1 = str(int(this_year) - 1) + '1231'
        else:
            rptdate1 = rptdate[0: 4] + '1231'
        sql = f'''
            select distinct CPDM from t_fund_classify_his
            where ejfl in ({ejfl_filter_str}) and rptdate = '{rptdate1}'
        '''
        # return: [(value,), ...]
        sql_rst = self.cu_pra.execute(sql).fetchall()
        code_list = []
        if sql_rst:
            code_list = [x[0] for x in sql_rst]
        return code_list

    def get_fund_code(self):
        ejfl_filter = ['标准配置型', '可转债型', '环球股票', '普通债券型', '股票型', '纯债型', '灵活配置型', '激进配置型', '激进债券型', '保守配置型', '沪港深股票型',
                       '沪港深配置型', ' 纯债型']
        ejfl_filter_str = str(tuple(ejfl_filter))
        sql = f'''
            select distinct CPDM from t_fund_classify_his
            where ejfl in {ejfl_filter_str}
        '''
        # return: [(value,), ...]
        sql_rst = self.cu_pra.execute(sql).fetchall()
        code_list = []

        if sql_rst:
            code_list = [x[0] for x in sql_rst]

        code_list = list(set(code_list))
        return code_list

    # 计算季度标签
    def run_standard_deviation(self):
        fund_code = self.get_fund_code()
        # fund_code = fund_code[0: 100]
        rec = []
        run_cnt = 0
        fd_num = len(fund_code)
        logging.info('funds number: {}'.format(fd_num))
        for code in fund_code:
            run_cnt += 1
            logging.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
            for date_list in self.run_date_list:
                report_date = date_list[0]

                for year_lag in range(1, 4):
                    tmp_date = date_list[4 - year_lag]
                    start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
                    try:
                        std_value = standard_deviation(code, start_date, report_date)
                        logging.debug('start_date: {} end_date: {}'.format(start_date, report_date))
                        logging.debug('code {} year_lag {} std_value {}'.format(code, year_lag, std_value))
                    except Exception as err:
                        logging.warning(err)
                        std_value = None
                    rec.append((code, 'StandardDeviation%dYear' % (year_lag), str(std_value), None, self.algid,
                                report_date, self.thisfilename, self.todaydate))
                    if len(rec) > 500:
                        # self.db_insert_session.DBInsert(sql_index_insert, rec)
                        rec = []
        # self.db_insert_session.DBInsert(sql_index_insert, rec)
        return

    # 计算下行波动率
    def run_down_std(self):
        fund_code = self.get_fund_code_by_filter(DOWN_STD_FILTER)
        run_cnt = 0
        fd_num = len(fund_code)
        logging.info('funds number: {}'.format(fd_num))
        for code in fund_code:
            run_cnt += 1
            logging.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
            for date_list in self.run_date_list:
                report_date = date_list[0]
                for year_lag in range(1, 4):
                    tmp_date = date_list[4 - year_lag]
                    start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
                    try:
                        std_value = down_std(code, start_date, report_date)
                        logging.info('start_date: {} end_date: {}'.format(start_date, report_date))
                        logging.info('code {} year_lag {} std_value {}'.format(code, year_lag, std_value))
                    except Exception as err:
                        logging.warning(err)
                        std_value = None
                    rec = (
                        code, 'DesStandardDeviation%dYear' % (year_lag), str(std_value), None, self.algid, report_date,
                        self.thisfilename, self.todaydate)
                    self.db_insert_session.add_info(rec)
        self.db_insert_session.finish()
        return

    # 计算杠杆率
    def run_leverage_ratio(self):
        fund_code = self.get_fund_code_by_filter(LVEREAGE_FILTER)
        run_cnt = 0
        fd_num = len(fund_code)
        logging.info('funds number: {}'.format(fd_num))
        for code in fund_code:
            run_cnt += 1
            logging.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
            for date_list in self.run_date_list:
                report_date = date_list[0]
                # 只跑近一年
                for year_lag in range(1, 2):
                    tmp_date = date_list[4 - year_lag]
                    start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
                    try:
                        rst_dt = leverage_ratio(code, start_date, report_date)
                        rst_value = rst_dt['杠杆率'].values[0]
                        logging.info('start_date: {} end_date: {}'.format(start_date, report_date))
                        logging.info('code {} year_lag {} leverage_ratio {}'.format(code, year_lag, rst_value))
                    except Exception as err:
                        logging.warning(err)
                        rst_value = None
                    rec = (code, 'AvgLeverageRatio', str(rst_value), None, self.algid, report_date, self.thisfilename,
                           self.todaydate)
                    self.db_insert_session.add_info(rec)
        self.db_insert_session.finish()
        return

    def run_alpha(self):

        run_cnt = 0

        # logging.info('funds number: {}'.format(fd_num))
        # for code in fund_code:
        #    run_cnt += 1
        # logging.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
        for date_list in self.run_date_list:

            report_date = date_list[0]
            fund_code = self.get_fund_code_by_filter(ALPHA_FILTER, report_date)
            fd_num = len(fund_code)
            # 只跑近一年
            for year_lag in range(1, 4):
                tmp_date = date_list[4 - year_lag]
                start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
                fund_value_all = None
                for code in fund_code:
                    try:
                        # rst_value = compute_alpha_categroy(code, start_date, report_date, run_type=2)
                        t_list = gen_time_list_out(start_date, report_date, 'weekly')
                        fund_value = get_fund_price(code, start_date, report_date, t_list)
                        # print(code,fund_value)
                        fund_value['基金代码'] = code
                        # logging.info('start_date: {} end_date: {}'.format(start_date, report_date))
                        # logging.info('code {} year_lag {} alpha{}'.format(code, year_lag, rst_value))
                    except Exception as err:
                        logging.warning(err)
                        fund_value = None

                    if fund_value_all is None:
                        fund_value_all = fund_value
                    else:
                        fund_value_all = fund_value_all.append(fund_value)

                fund_value_all['收益率'] = fund_value_all['复权单位净值'].pct_change()
                fund_value_all.dropna(inplace=True)

                # 获取二级分类
                ed_date = report_date[0: 4] + '1231'
                sql_class = '''select cpdm,ejfl from t_fund_classify_his where rptdate = '%s'
                ''' % ed_date
                class_data = pd.DataFrame(self.cu_pra.execute(sql_class).fetchall(), columns=['基金代码', '二级分类'])
                fund_value_all = pd.merge(fund_value_all, class_data, on='基金代码', how='inner')
                fund_value_all_group = pd.DataFrame(fund_value_all.groupby('二级分类').收益率.mean())
                fund_value_all_group.rename(columns={'收益率': '同类平均收益率'}, inplace=True)
                fund_value_all = pd.merge(fund_value_all, fund_value_all_group, on='二级分类', how='left')
                fund_value_all['超额收益'] = fund_value_all['收益率'] - fund_value_all['同类平均收益率']
                fund_data = fund_value_all[['基金代码']]
                fund_data.drop_duplicates(inplace=True)
                fund_value_all_group = pd.DataFrame(fund_value_all.groupby('基金代码').超额收益.mean())
                fund_value_all = pd.merge(fund_data, fund_value_all_group, on='基金代码', how='inner')
                fund_value_all = fund_value_all[['基金代码', '超额收益']]
                fund_value_all['year_lag'] = 'AlphaCategroyMean{}Year'.format(year_lag)
                fund_value_all['algid'] = self.algid
                fund_value_all['report_date'] = report_date
                fund_value_all['thisfilename'] = self.thisfilename
                fund_value_all['todaydate'] = self.todaydate
                fund_value_all['None'] = ''
                fund_value_all = fund_value_all[
                    ['基金代码', 'year_lag', '超额收益', 'None', 'algid', 'report_date', 'thisfilename', 'todaydate']]
                rec = [tuple(x) for x in fund_value_all.values]
                # rec = (code, 'AlphaCategroyFund{}Year'.format(year_lag), str(rst_value), None, self.algid, report_date, self.thisfilename, self.todaydate)
                for r in rec:
                    self.db_insert_session.add_info(r)

        # 跑半年
        for date_list in self.half_year_date_list:
            print(date_list)
            report_date = date_list[0]
            fund_code = self.get_fund_code_by_filter(ALPHA_FILTER, report_date)
            # print(fund_code)
            tmp_date = date_list[1]
            start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
            fund_value_all = None
            for code in fund_code:
                try:
                    # rst_value = compute_alpha_categroy(code, start_date, report_date, run_type=2)
                    t_list = gen_time_list_out(start_date, report_date, 'weekly')
                    fund_value = get_fund_price(code, start_date, report_date, t_list)
                    fund_value['基金代码'] = code
                    # my_logger.info('start_date: {} end_date: {}'.format(start_date, report_date))
                    # my_logger.info('code {} year_lag {} alpha:{}'.format(code, 'half', rst_value))
                except Exception as err:
                    my_logger.warning(err)
                    fund_value = None

                if fund_value_all is None:
                    fund_value_all = fund_value
                else:
                    fund_value_all = fund_value_all.append(fund_value)
            fund_value_all['收益率'] = fund_value_all['复权单位净值'].pct_change()
            fund_value_all.dropna(inplace=True)
            # 获取二级分类
            if report_date[0:4] == '2019':
                ed_date = '20181231'
            else:
                ed_date = report_date[0: 4] + '1231'
            sql_class = '''select cpdm,ejfl from t_fund_classify_his where rptdate = '%s'
            ''' % ed_date
            class_data = pd.DataFrame(self.cu_pra.execute(sql_class).fetchall(), columns=['基金代码', '二级分类'])
            fund_value_all = pd.merge(fund_value_all, class_data, on='基金代码', how='inner')
            fund_value_all_group = pd.DataFrame(fund_value_all.groupby('二级分类').收益率.mean())
            fund_value_all_group.rename(columns={'收益率': '同类平均收益率'}, inplace=True)
            fund_value_all = pd.merge(fund_value_all, fund_value_all_group, on='二级分类', how='left')
            fund_value_all['超额收益'] = fund_value_all['收益率'] - fund_value_all['同类平均收益率']
            fund_data = fund_value_all[['基金代码']]
            fund_data.drop_duplicates(inplace=True)
            fund_value_all = pd.DataFrame(fund_value_all.groupby('基金代码').超额收益.mean())
            fund_value_all_group = pd.DataFrame(fund_value_all.groupby('基金代码').超额收益.mean())
            fund_value_all = pd.merge(fund_data, fund_value_all_group, on='基金代码', how='inner')
            fund_value_all['year_lag'] = 'AlphaCategroyMean6Month'
            fund_value_all['algid'] = self.algid
            fund_value_all['report_date'] = report_date
            fund_value_all['thisfilename'] = self.thisfilename
            fund_value_all['todaydate'] = self.todaydate
            fund_value_all['None'] = ''
            fund_value_all = fund_value_all[
                ['基金代码', 'year_lag', '超额收益', 'None', 'algid', 'report_date', 'thisfilename', 'todaydate']]
            rec = [tuple(x) for x in fund_value_all.values]
            print(rec)
            # rec = (code, 'AlphaCategroyFund6Month'.format(year_lag), str(rst_value), None, self.algid, report_date, self.thisfilename, self.todaydate)
            for r in rec:
                self.db_insert_session.add_info(r)

        self.db_insert_session.finish()
        return

    def run_alpha_type1(self):
        fund_code = self.get_fund_code_by_filter(ALPHA_FILTER_TMP)
        run_cnt = 0
        fd_num = len(fund_code)
        logging.info('funds number: {}'.format(fd_num))
        for code in fund_code:
            run_cnt += 1
            logging.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
            for date_list in self.run_date_list:
                report_date = date_list[0]
                # 只跑近一年
                for year_lag in range(1, 4):
                    tmp_date = date_list[4 - year_lag]
                    start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
                    try:
                        rst_value = compute_alpha_categroy(code, start_date, report_date, run_type=1)
                        logging.info('start_date: {} end_date: {}'.format(start_date, report_date))
                        logging.info('code {} year_lag {} leverage_ratio {}'.format(code, year_lag, rst_value))
                    except Exception as err:
                        logging.warning(err)
                        rst_value = None
                    rec = (
                        code, 'AlphaCategroyMean{}Year'.format(year_lag), str(rst_value), None, self.algid, report_date,
                        self.thisfilename, self.todaydate)
                    self.db_insert_session.add_info(rec)
        self.db_insert_session.finish()
        return

    # 计算最大回撤
    def run_max_draw_down(self):
        fund_code = self.get_fund_code_by_filter(DOWN_STD_FILTER)
        run_cnt = 0
        fd_num = len(fund_code)
        my_logger.info('funds number: {}'.format(fd_num))
        for code in fund_code:
            run_cnt += 1
            my_logger.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
            # 跑近三年
            for date_list in self.run_date_list:
                report_date = date_list[0]
                for year_lag in range(1, 4):
                    tmp_date = date_list[4 - year_lag]
                    start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
                    try:
                        rst_value = max_draw_down(code, start_date, report_date)
                        my_logger.info('start_date: {} end_date: {}'.format(start_date, report_date))
                        my_logger.info('code {} year_lag {} max_draw_down{}'.format(code, year_lag, rst_value))
                    except Exception as err:
                        my_logger.warning(err)
                        rst_value = None
                    rec = (code, 'MaxDrawdown{}Year'.format(year_lag), str(rst_value), None, self.algid, report_date,
                           self.thisfilename, self.todaydate)
                    if rst_value is not None:
                        self.db_insert_session.add_info(rec)
            # 跑半年
            for date_list in self.half_year_date_list:
                report_date = date_list[0]
                tmp_date = date_list[1]
                start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
                try:
                    rst_value = max_draw_down(code, start_date, report_date)
                    my_logger.info('start_date: {} end_date: {}'.format(start_date, report_date))
                    my_logger.info('code {} year_lag {} max_draw_down{}'.format(code, 'half', rst_value))
                except Exception as err:
                    my_logger.warning(err)
                    rst_value = None
                rec = (code, 'MaxDrawdown6Month'.format(year_lag), str(rst_value), None, self.algid, report_date,
                       self.thisfilename, self.todaydate)
                self.db_insert_session.add_info(rec)
        self.db_insert_session.finish()
        return

    # 计算信息比率
    def run_ir(self):
        fund_code = self.get_fund_code_by_filter(DOWN_STD_FILTER)
        run_cnt = 0
        fd_num = len(fund_code)
        my_logger.info('funds number: {}'.format(fd_num))
        for code in fund_code:
            run_cnt += 1
            my_logger.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
            # 跑近三年
            for date_list in self.run_date_list:
                report_date = date_list[0]
                for year_lag in range(1, 4):
                    tmp_date = date_list[4 - year_lag]
                    start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
                    try:
                        rst_value = info_ratio(code, start_date, report_date)
                        my_logger.info('start_date: {} end_date: {}'.format(start_date, report_date))
                        my_logger.info('code {} year_lag {} ir{}'.format(code, year_lag, rst_value))
                    except Exception as err:
                        my_logger.warning(err)
                        rst_value = None
                    rec = (
                        code, 'InformationRatio%{}Year'.format(year_lag), str(rst_value), None, self.algid, report_date,
                        self.thisfilename, self.todaydate)
                    if rst_value is not None:
                        self.db_insert_session.add_info(rec)
            # 跑半年
            for date_list in self.half_year_date_list:
                report_date = date_list[0]
                tmp_date = date_list[1]
                start_date = pd.date_range(start=tmp_date, periods=2, freq='D').strftime('%Y%m%d').tolist()[-1]
                try:
                    rst_value = info_ratio(code, start_date, report_date)
                    my_logger.info('start_date: {} end_date: {}'.format(start_date, report_date))
                    my_logger.info('code {} year_lag {} ir:{}'.format(code, 'half', rst_value))
                except Exception as err:
                    my_logger.warning(err)
                    rst_value = None
                rec = (code, 'InformationRatio6Month'.format(year_lag), str(rst_value), None, self.algid, report_date,
                       self.thisfilename, self.todaydate)
                self.db_insert_session.add_info(rec)
        self.db_insert_session.finish()
        return



class ZenQiangZhiShu(ToolClass):
    """计算类"""

    def __init__(self):
        super(ZenQiangZhiShu, self).__init__()
        self.gen_market()

    def get_pre_trade_day(self, today):
        """获取前一个交易日"""
        sql_get_pre_week_trade_day = f"""
        select f1_1010 from wind.tb_object_1010
        where f1_1010 <={today}   and rownum <=1
        order by f1_1010 desc 
        """

        res = self._get_db_data(sql_get_pre_week_trade_day, db='reader')
        return res[0][0]

    def get_market(self, fund_type, start_date,end_date):
        """获取同类基准
         """
        if self.market.empty:
            self.gen_market()

        market_tmp = self.market_return[start_date:end_date][:]


        if fund_type =='股票型':
            market_type  = market_tmp['中证800']
        elif fund_type == '激进配置型':
            market_type = market_tmp['中证800']*0.8+market_tmp['中证国债']*0.2
        elif fund_type =='标准配置型':
            market_type = market_tmp['中证800']*0.6+market_tmp['中证国债']*0.4
        elif fund_type == '保守配置型':
            market_type = market_tmp['中证800'] * 0.2 + market_tmp['中证国债'] * 0.8
        elif fund_type == '灵活配置型':
            market_type = market_tmp['中证800'] * 0.5 + market_tmp['中证国债'] * 0.5
        elif fund_type == '沪港深股票型':
            market_type = market_tmp['中证800'] * 0.45 + market_tmp['中证国债'] * 0.1+market_tmp['恒生指数']*0.45
        elif fund_type == '沪港深配置型':
            market_type = market_tmp['中证800'] * 0.35 + market_tmp['中证国债'] * 0.3 + market_tmp['恒生指数'] * 0.35
        elif fund_type =='纯债型':
            market_type = market_tmp['中债综合全价指数']
        elif fund_type == '普通债券型':
            market_type = market_tmp['中债综合全价指数']*0.9+market_tmp['中证800'] * 0.1
        elif fund_type == '激进债券型':
            market_type = market_tmp['中债综合全价指数']*0.8+market_tmp['中证800'] * 0.2
        elif fund_type =='短债型':
            market_type = market_tmp['中债综合财富']
        elif fund_type =='可转债型':
            market_type = market_tmp['中证可转债']
        elif fund_type =='环球股票':
            market_type = market_tmp['MSCI']
        else:
            market_type = np.array([])
        # print(market_type)
        rst = pd.DataFrame({'日期':market_type.index,'市场组合收益率':market_type.values})
        # print(rst)


        return rst

    def gen_market(self):
        '''计算同类基准
        中证800收益率, 中证国债收益率, 恒生指数收益率, 中证综合债收益率, 中证短债收益率
        MSCI收益率
        没有用到 中证可转债
        '''
        # 中证800
        market2 = self.market_fetch('000906')
        market1 = self.market_fetch('000001')
        market1 = market1[market1.index < '20050105']
        a = market2[market2.index == '20050104'].iloc[0, 0] / market1[market1.index== '20050104'].iloc[-1, 0]
        market1['000906'] = market1['000001'] * a
        index = market1[market1.index == '20050104'].index.tolist()[0]
        market_800 = pd.concat([market1[['000906']] , market2], axis=0).drop([index])

        # 中证国债
        m_country = self.market_fetch('h11006')
        # 恒生指数
        HSI  = self.get_market_wind('HSI.HI')
        #中证综合债
        m_bond = self.market_fetch('h11009')
        # 中证短债
        market_short = self.market_fetch('h11015')
        # 中证可转债
        market_tran = self.market_fetch('000832')
        # MSCI
        MSCI = self.get_market_wind("892400.MI")
        # 中债综合全价指数
        ZZZHQJ = self.get_buon_index('CBA00203')
        # 中债综合财富
        ZZZHCF = self.get_buon_index('CBA00221')
        self.market = market_800.join([m_country,HSI,m_bond,market_short,market_tran,MSCI,ZZZHQJ,ZZZHCF])
        self.market.columns = ['中证800','中证国债','恒生指数','中证综合债','中证短债','中证可转债','MSCI','中债综合全价指数','中债综合财富']

        self.market_return = pd.DataFrame()

        self.market_return['日期'] = self.market.index.values
        # print(self.market)
        for i in self.market.columns[:]:

            self.market_return[i] = self.market[i].pct_change().values
        self.market_return.set_index('日期',inplace=True)
        # print(self.market_return)
        return self.market,self.market_return

    def market_fetch(self, stock_index):
        '''获取所有交易日指数数据'''
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

        sql_res = self._get_db_data(sql1,db='reader')

        market = pd.DataFrame(sql_res, columns=['日期', '指数收盘价'])
        market.index = market['日期']
        del market['日期']
        market.columns = [stock_index]
        # market = market.pct_change()
        return market

    def get_buon_index(self,code,start_date = '19900101',end_date = '20191231' ):
        """获取各种债券指数"""
        sql = f"""
        
        select f3_1288 日期,f2_1288 收盘价 from wind.TB_OBJECT_1288 t where t.f1_1288 = (select f2_1090 from wind.tb_object_1090 where f16_1090 = '{code}')
        and f3_1288 >= '{start_date}' and f3_1288 <= '{end_date}'
        
        """
        # print(sql)
        sql_res = self._get_db_data(sql, db='reader')

        market = pd.DataFrame(sql_res, columns=['日期', '指数收盘价'])
        market.index = market['日期']
        del market['日期']
        market.columns = [code]
        # market = market.pct_change()
        return market


    def get_market_wind(slef, code, index_start_date='19900101', end_date='20191231'):
        try:
            df = w.wsd(code, "close", index_start_date, end_date, "","Currency=CNY", usedf=True)[1]
            #df = w.wsd(code, "close", "2015-01-01", end_date, "","Currency=CNY", usedf=True)[1]
            df.index = pd.Series(df.index).apply(lambda x: str(x)[:4] + str(x)[5:7] + str(x)[8:10])
            df.columns = [code]
        except:
            df = pd.DataFrame(columns=[code])
        return df

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

    def get_all_bond(self, start_date: str, end_date: str):
        """
        获取全部的基金
        :return:
        """

        sql = f"""

                select cpdm,rptdate,ejfl from t_fund_classify_his where ejfl in ('纯债型','普通债券型','激进债券型','短债型','可转债型')  and rptdate >={start_date} and rptdate <= {end_date}

                """

        sql_res = self._get_db_data(sql, db='pra')
        df = pd.DataFrame(sql_res, columns=['cpdm', 'rptdate','ejfl'])
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

            t = str(int(report[:4]) + 1) + '1231' if report[:4] != '2018' else ENDDATE

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

    def track_err(self, df: pd.DataFrame, funds_return: dict, indexs_return: pd.DataFrame):
        """
        想表中添加跟踪误差
        :param df: 需要添加的表
        :param fund_return: 基金收益率字典
        :param index_return: 指标收益率df
        :return:
        """
        # print(df)
        track_err_res = []
        information_ratio_res = []
        total_number = len(df)
        has_been = 0
        for v in df.values:
            has_been += 1
            print(f'\r计算跟踪误差和信息比率，进度：{has_been}/{total_number}', end='')
            code = v[0]

            ejfl= v[2]
            date_q = v[3]   # 取出季报日
            # start_date = year + '0101'
            # end_date = year + '1231'
            # 取出近三年的周期节点
            start_date = str(int(date_q[:4])-1) + date_q[4:]
            end_date = date_q

            fund_return = funds_return[code][:]
            fund_return.reset_index(inplace=True)
            # print(fund_return)
            fund_return.set_index('交易日期', inplace=True)
            fund_return = fund_return[start_date:end_date]

            fund_return.reset_index(inplace=True)
            # print(fund_return)
            # print('*'*50)
            fund_return.dropna(inplace=True)
            index_return = self.get_market(ejfl,INDEX_START,ENDDATE)



            index_return.columns = ['交易日期', '收益率']
            # index_return.rename(columns = {'日期':'交易日期'},inplace=True)
            # print(index_return)
            df_temp = pd.merge(fund_return, index_return, how='left', on='交易日期')
            # print(df_temp)
            fund_return_arr = df_temp['收益率_x'].values
            index_return_arr = df_temp['收益率_y'].values

            track_err = tracking_err(fund_return_arr, index_return_arr, N)
            information_ratio_value = information_ratio(fund_return_arr, index_return_arr, track_err, N) * N
            # print(track_err)
            # print(information_ratio_value)
            track_err_res.append(track_err)
            information_ratio_res.append(information_ratio_value)

        df['跟踪误差'] = track_err_res
        df['信息比率'] = information_ratio_res
        return df

    def run(self):
        """
        主函数
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
        self.market.reset_index(inplace = True)
        # 计算各个指标的收益率
        index_return = pd.DataFrame()
        index_return['日期'] = self.market['日期'][:]
        for i in self.market.columns[1:]:
            index_return[i] = self.market[i].pct_change()


        # 这里开始按在用fro循环计算每个基金的收益率，存在字典里面
        df_fund_return = self.get_fund_return(df_zqzs)


        # 计算每个基金在每个周期内的跟踪误差和信息比率
        df_fund_index = self.track_err(df_zqzs, df_fund_return, index_return)


        df_fund_index.to_excel('债券型近三年的跟踪误差及信息比率.xlsx')

    def run_jidu(self, date_q):
        """
        按季度跑近三年
        :param date_q:季度日期
        :return:
        """
        # 取分类的日期
        class_date = date_q[:4] + '1231' if date_q[:4] != '2019' else '20181231'

        start_date = class_date
        end_date = class_date
        df_zqzs = self.get_all_bond(start_date, end_date)
        # df_zqzs = df_zqzs[:50]
        df_zqzs['季度'] = date_q
        self.market.reset_index(inplace=True)
        # 计算各个指标的收益率
        index_return = pd.DataFrame()
        index_return['日期'] = self.market['日期'][:]
        for i in self.market.columns[1:]:
            index_return[i] = self.market[i].pct_change()

        # 这里开始按在用fro循环计算每个基金的收益率，存在字典里面
        df_fund_return = self.get_fund_return(df_zqzs)

        # 计算每个基金在每个周期内的跟踪误差和信息比率
        df_fund_index = self.track_err(df_zqzs, df_fund_return, index_return)
        # print(df_fund_index)

        db_insert_session = DBSession(SQL_INDEX_INSERT)
        todaydate = datetime.now().strftime('%Y%m%d')
        for i in df_fund_index.values:
            rec = (i[0], 'TrackingError1Year', str(i[4]), None, '0000000003',
                       "20190331", "债券类近三年.py", todaydate)
            db_insert_session.add_info(rec)

        db_insert_session.finish()


def run():
    zqzs = ZenQiangZhiShu()


    # zqzs.run002()
    zqzs.run_jidu('20190331')

    # zqzs.run002()


if __name__ == '__main__':
    run()
    # aaa = get_track_index('000852.SH')
    # print(aaa)
