#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/5 15:31
# @Author  : GaoJian
# @File    : all_shell.py

# auther: 03tow
# modified: 03tow 20190613
import os
import datetime
import pandas as pd
import logging
import cx_Oracle

from sql import sql_oracle
from season_label import leverage_ratio, standard_deviation
from season_label import down_std, compute_alpha_categroy

# from season_label import max_draw_down, info_ratio
# from season_label import get_fund_price,gen_time_list as gen_time_list_out

# 日志部分
log_dir = 'log'
if os.path.exists(log_dir) and os.path.isdir(log_dir):
    pass
else:
    os.makedirs(log_dir)
log_file_name = 'season_label_0630_alpha_0.log'
LOG_FILE_PATH = os.path.join('log', log_file_name)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s-%(levelname)s-%(message)s',
    datefmt='%y-%m-%d %H:%M',
    filename=LOG_FILE_PATH,
    filemode='a'
)
fh = logging.FileHandler(LOG_FILE_PATH, encoding='utf-8')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

my_logger = logging.getLogger()
my_logger.addHandler(fh)
my_logger.addHandler(ch)

# 入库的sql
sql_tag_insert = "INSERT INTO fund_tag_info( fundId, tagId, tagValue, algid, batchno, endableFlag, createUser, createDate ) VALUES(:fundId, :tagId, :tagValue, :algid, :batchno, :endableFlag, :createuser, :createDate)"
SQL_INDEX_INSERT = "INSERT INTO fund_index_info( fundId, indexCode, indexValue, reportdate, algid, batchno,  createUser, createDate ) VALUES(:fundId, :indexCode, :indexValue,:reportdate, :algid, :batchno, :createuser, :createDate)"

"""----------过滤筛选条件-------------"""
# net_act
DOWN_STD_FILTER = ['标准配置型', '可转债型', '环球股票', '普通债券型', '股票型', '纯债型', '灵活配置型', '激进配置型', '激进债券型', '保守配置型', '沪港深股票型',
                   '沪港深配置型', ' 纯债型']
# net_act_plus
ALPHA_FILTER = ['标准配置型', '可转债型', '环球股票', '普通债券型', '股票型', '纯债型', '灵活配置型', '激进配置型', '激进债券型', '保守配置型', '沪港深股票型', '沪港深配置型',
                ' 纯债型']
ALPHA_FILTER_TMP = ['纯债型']
# 杠杆率过滤条件
LVEREAGE_FILTER = ['保守配置型', '纯债型', '激进债券型', '普通债券型', '可转债型', ' 纯债型']


def gen_time_list(st_date='20100101', ed_date='20181231', rq_flag=False):
    rpts = {}
    if rq_flag:
        rpts = {'任期': [st_date, ed_date]}
    for j in range(int(st_date[0:4]), int(ed_date[0:4]) + 1, 1):
        if j == int(st_date[0:4]):
            rpts[str(j)] = [st_date, str(j) + '1231']
        elif j == int(ed_date[0:4]):
            rpts[str(j)] = [str(j) + '0101', ed_date]
        else:
            rpts[str(j)] = [str(j) + '0101', str(j) + '1231']
    return rpts


def gen_season_report_date(start, end):
    r_list = []
    season_report = ['0331', '0630', '0930', '1231']
    s_int, e_int = int(start[0:4]), int(end[0:4])
    for year in range(s_int, e_int + 1):
        r_list += [str(year) + date for date in season_report]
    r_list = [i for i in r_list if i >= start and i <= end]
    # print(r_list)
    return r_list



class DBSession(object):
    """
    sql封装
    """
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


class RunSeasonLableChild(RunSeasonLable):
    def __init__(self):
        RunSeasonLable.__init__(self)

    def find_fund_clr(self, code):

        sql = '''SELECT distinct(clr) from t_fund_classify_his where cpdm='%(code)s'
                ''' % {'code': code}
        sql_rst = self.cu_pra.execute(sql).fetchall()
        return sql_rst[0][0]

    # 计算季度标签
    def run_standard_deviation(self):
        fund_code = self.get_fund_code()
        # fund_code = fund_code[0: 100]

        run_cnt = 0
        fd_num = len(fund_code)
        logging.info('funds number: {}'.format(fd_num))
        for code in fund_code:
            run_cnt += 1
            logging.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
            for date_list in self.run_date_list:
                report_date = date_list[0]

                start_date = self.find_fund_clr(code)

                if start_date > report_date:
                    continue

                try:
                    std_value = standard_deviation(code, start_date, report_date)
                    logging.debug('start_date: {} end_date: {}'.format(start_date, report_date))
                    logging.debug('code {} year_lag {} std_value {}'.format(code, "", std_value))
                except Exception as err:
                    logging.warning(err)
                    std_value = None

                rec = (code, 'StandardDeviationAll', str(std_value), None, self.algid,
                       report_date, self.thisfilename, self.todaydate)
                # print(rec)
                self.db_insert_session.add_info(rec)
        self.db_insert_session.finish()

    def run_alpha2(self):
        fund_code = self.get_fund_code_by_filter(ALPHA_FILTER)
        run_cnt = 0
        fd_num = len(fund_code)
        logging.info('funds number: {}'.format(fd_num))
        for code in fund_code:
            run_cnt += 1
            logging.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
            for date_list in self.run_date_list:
                report_date = date_list[0]

                start_date = self.find_fund_clr(code)

                if start_date > report_date:
                    continue

                try:
                    rst_value = compute_alpha_categroy(code, start_date, report_date, run_type=2)
                    logging.info('start_date: {} end_date: {}'.format(start_date, report_date))
                    logging.info('code {} year_lag {} alpha{}'.format(code, "", rst_value))
                except Exception as err:
                    logging.warning(err)
                    rst_value = None
                rec = (code, 'AlphaCategroyFundAll', str(rst_value), None, self.algid, report_date, self.thisfilename,
                       self.todaydate)
                self.db_insert_session.add_info(rec)

        self.db_insert_session.finish()
        return

    def run_alpha0(self):
        fund_code = self.get_fund_code_by_filter(ALPHA_FILTER)
        run_cnt = 0
        fd_num = len(fund_code)
        logging.info('funds number: {}'.format(fd_num))
        for code in fund_code:
            run_cnt += 1
            logging.info('code: {} pct: {} / {}'.format(code, run_cnt, fd_num))
            for date_list in self.run_date_list:
                report_date = date_list[0]

                start_date = self.find_fund_clr(code)

                if start_date > report_date:
                    continue

                try:
                    rst_value = compute_alpha_categroy(code, start_date, report_date, run_type=0)
                    logging.info('start_date: {} end_date: {}'.format(start_date, report_date))
                    logging.info('code {} year_lag {} alpha{}'.format(code, "", rst_value))
                except Exception as err:
                    logging.warning(err)
                    rst_value = None
                rec = (
                code, 'AlphaCategroyBenchmarkAll', str(rst_value), None, self.algid, report_date, self.thisfilename,
                self.todaydate)
                print(rec)
                self.db_insert_session.add_info(rec)
        self.db_insert_session.finish()


def main_all_run():
    run_oj = RunSeasonLableChild()

    start_date, end_date = '19980101', '20071231'
    # start_date, end_date = '20080101', '20151231'
    # start_date, end_date = '20160101', '20190331'
    report_dates = gen_season_report_date(start_date, end_date)
    run_oj._set_end_list(report_dates)

    # 波动率
    # run_oj.run_standard_deviation()

    # alpha生涯2
    # run_oj.run_alpha2()

    # alpha生涯0
    run_oj.run_alpha0()



if __name__ == '__main__':
    main_all_run()
