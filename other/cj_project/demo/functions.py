# encoding=utf-8
"""小工具函数"""

import os

import cx_Oracle

from datetime import datetime, timedelta
import pandas as pd


def single(cls):
    """单例"""
    _instance = {}

    def warpper(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]

    return warpper


def to_abspath(path):
    """相对路径转绝对路径"""
    return os.path.abspath(path)


def list_to_sql_list(lists: list):
    """
    list转sql可用的list
    :param lists: list：[1,2,3,4,5]
    :return: '1','2''3','4','5'
    """

    lists = list(map(lambda x: f"''{x}''", lists))
    res = ','.join(lists)
    return res


def nan_cvt(x, dig: int = 4):
    """
    处理df数值，nan转为py中的None，小数默认保留小数点后4位
    :param x: 数值
    :param dig: 保留位数
    :return:
    """
    return None if str(x) == 'nan' else round(x, dig)


class ToolClass:
    """根据7因子打标签"""

    def __init__(self):
        """初始化"""
        # 记录数据库连接数
        self.count_db = 0
        self.connecting = False

    def __del__(self):
        """退出同时关闭数据库"""

        self.connecting = False
        self.close_db()

    def connect_db(self):
        """连接数据库"""
        if self.count_db == 0:
            [userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf',
                                                                 'wind']
            self.fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
            self.cu = self.fund_db.cursor()

            # pra库
            [userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
            self.fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
            self.cu_pra_sel = self.fund_db_pra.cursor()

            self.count_db += 1

    def close_db(self):
        """断开数据库"""
        if self.count_db == 1 and self.connecting == False:
            self.cu.close()
            self.cu_pra_sel.close()

            self.fund_db.close()
            self.fund_db_pra.close()

            self.count_db -= 1

    def _get_db_data(self, sql: str, db: str = 'reader'):
        """专用的短链接读取数据库数据的方法"""
        self.connect_db()
        self.connecting = True
        cu = self.cu if db == 'reader' else self.cu_pra_sel
        sql_res = cu.execute(sql).fetchall()
        self.connecting = False
        self.close_db()
        return sql_res

    def _commit_db(self, sql: str, rec: list, db: str = 'reader'):
        """提交事务用"""
        self.connect_db()
        self.connecting = True
        cu = self.cu if db == 'reader' else self.cu_pra_sel
        fund_db_pra = self.fund_db if db == 'reader' else self.fund_db_pra

        cu.executemany(sql, rec)
        fund_db_pra.commit()

        self.connecting = False
        self.close_db()
        return 'ok'


class GetValue(ToolClass):
    """获取净值的方法"""

    def __init__(self):
        super(GetValue, self).__init__()
        self.offset = 5
        self.time_name = 'f1_1010'
        self.time_table = 'wind.tb_object_1010'
    def set_offset(self, week: int):
        self.offset = week

    def get_all_trade_days(self,start_date:str,end_date:str):
        """
        获取全交易日
        :param start_date: 起始日期
        :param end_date: 截止日期
        :return:df
        """
        assert end_date >= start_date, '截止日必须大于起始日'

        sql_get_trade_days = f"""
                select {self.time_name} from {self.time_table}
                where {self.time_name} <= '{end_date}' and {self.time_name} >='{start_date}'
                order by {self.time_name} asc 
                """

        sql_res = self._get_db_data(sql_get_trade_days,db='reader')
        df_time = pd.DataFrame(sql_res, columns=['交易日期'])
        return df_time


    def get_values(self, code:str, start_date:str, end_date:str):
        """
        查询净值，自定填补
        :param code:
        :param start_date:
        :param end_date:
        :return: df columns 交易日期，复权单位净值，收益率
        """
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        start_new = start_dt - timedelta(weeks=self.offset)

        start_str = start_new.strftime('%Y%m%d')

        sql = f"""
                     select
                f13_1101 as 交易日期, f21_1101 as 复权单位净值 
                from
                wind.tb_object_1101
                left join wind.tb_object_1090
                on f2_1090 = f14_1101
                where 
                F16_1090= '{code}'
                and
                F13_1101 <=  '{end_date}'
                and
                F13_1101 >= '{start_str}'
                order by f13_1101 desc
        """

        sql_res = self._get_db_data(sql, db='reader')
        df = pd.DataFrame(sql_res, columns=['交易日期', '复权单位净值'])
        df_time = self.get_all_trade_days(start_str,end_date)

        df = pd.merge(df_time,df,how='left',on='交易日期')
        df.fillna(method='ffill', inplace=True)

        df['收益率'] = df['复权单位净值'].pct_change()
        df = df.loc[df['交易日期'] >= start_date][:]
        return df


gv = GetValue()

get_values_auto_fill = gv.get_values


@single
class A(object):
    def __init__(self):
        self.id = 1
        self.name = "i"

if __name__ == '__main__':
    path = 'dddd'

    print(to_abspath(path))
