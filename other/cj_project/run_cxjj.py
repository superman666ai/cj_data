import logging
import pandas as pd
import cx_Oracle
import argparse

from season_label import compute_alpha_categroy, standard_deviation


LOG_FILE_NAME = 'test.log'
logging.basicConfig(
                    level = logging.INFO,
                    format='%(asctime)s-%(levelname)s-%(message)s',
                    datefmt='%y-%m-%d %H:%M',
                    filename=LOG_FILE_NAME,
                    filemode='w'
                    )
fh = logging.FileHandler(LOG_FILE_NAME, encoding='utf-8')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")

fh.setFormatter(formatter)
ch.setFormatter(formatter)

t_logger= logging.getLogger()
t_logger.addHandler(fh)
t_logger.addHandler(ch)

#wind库
[userName, password, hostIP, dbName] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf']
try:
	fund_db = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
	cu=fund_db.cursor() 
except cx_Oracle.DatabaseError as e:
    print('数据库链接失败')
    #raise (e)

#投研平台库
[userNamepif, passwordpif, hostIPpif, dbNamepif] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
#[userNamepif, passwordpif, hostIPpif, dbNamepif] = ['pif', 'pif', '172.16.125.151', 'pif']
try:
	fund_dbpra = cx_Oracle.connect(user=userNamepif, password=passwordpif, dsn=hostIPpif + '/' + dbNamepif)
	cu_pra = fund_dbpra.cursor()
except cx_Oracle.DatabaseError as e:
	print('数据库链接失败')
	#raise (e)

def get_new_fund_from_db(base_date):
    ''' 获取基于base_date的近3年次新基金代码
    1 <= X < 3
    '''
    start_date = str(int(base_date[0:4]) -3) + base_date[4: ]
    end_date =  str(int(base_date[0:4]) -1) + base_date[4: ]
    t_logger.info(f'search fund with start_date: {start_date} end_date: {end_date}')
    sql = f'''
    select
        cpdm
    from t_fund_classify_his
    where
        clr > {start_date}
    and
        clr <= {end_date}
    '''
    # t_logger.info(sql)
    sql_rst = cu_pra.execute(sql).fetchall()
    code_list = []
    if sql_rst:
        code_list = [x[0] for x in sql_rst]
    return code_list

def get_company_info(code):
    '''依据 fund_code 获取基金公司信息

    wind.tb_object_1018: 公司基本资料 f35_1018 成立时间 f34_1018 公司id
    '''
    sql = f'''
        select
            f12_1099, fundcompany, f35_1018, f34_1018
        from(
            select 
                f12_1099, ob_object_name_1018 as fundcompany,
                f16_1090, f35_1018, f34_1018 from wind.tb_object_1018   
            right join (
                select *
                    from wind.TB_OBJECT_1099 t 
                    inner join wind.TB_OBJECT_1090 t1
                on t.f1_1099=t1.f2_1090
                where t1.f4_1090 = 'J') x
            on f34_1018 = x.f12_1099
            where f12_1099 is not null)
        where
            f16_1090 = '{code}'
        and
            rownum = 1
    '''
    sql_rst = cu_pra.execute(sql).fetchall()
    (manager_id, company_name, start_up_date, company_code) = sql_rst[0]
    t_logger.info('/tcompany_code: {}, company_name: {}'.format(company_code, company_name))
    return company_code, company_name, start_up_date, manager_id

def compute_alpha(code, start_date, end_date):
    ''' 计算相对收益alpha
    '''
    # to do
    try:
        alpha = compute_alpha_categroy(code, start_date, end_date)
    except Exception as err:
        print(err)
        logging.debug(err)
        alpha = None
    logging.info('\t alpha: {}'.format(alpha))
    return alpha

def compute_std(code, start_date, end_date):
    ''' 计算基金周波动年化
    '''
    try:
        std = standard_deviation(code, start_date, end_date)
    except Exception as err:
        print(err)
        logging.debug(err)
        std = None
    logging.info('\t std: {}'.format(std))
    return std

def gen_start_end_date(base_date):
    s_year_date = str(int(base_date[0: 4]) - 3) + base_date[4: ]
    s_date = pd.date_range(start=s_year_date, periods=2, freq='1D', closed='left').strftime('%Y%m%d').tolist()
    start_date = s_date[-1]
    return start_date, base_date

def get_rank_result(col, value_list):
    ''' 计算统计值的百分比排名
    col: 权重标签, 基金公司相关已经统一运算过rank，这里跳过处理
            基金公司: 'nomoney'
                      'stock_fund'
                      'fof_fund_alpha'
                      'fof_fund_risk'
            基金产品: 'alpha'
                      'std'
    '''
    if col in ['nomoney', 'stock_fund', 'fof_fund_alpha', 'fof_fund_risk']:
        return value_list
    tmp_obj = pd.Series(value_list)
    if col in ['alpha']:
        # 升序排名
        rank_rst = tmp_obj.rank()
    else:
        # 降序排名
        rank_rst = tmp_obj.rank(ascending=False)
    len_rank = len(rank_rst)
    rank_pct = [i * 1.0 / len_rank for i in rank_rst]
    t_logger.info(f'rank_pct: {rank_pct}')
    return rank_pct

def get_fund_value():
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
    logging.debug('get fund value sql:')
    logging.debug(sql)
    fund_value = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', '基金代码', 'fund_value'])
    return fund_value

def get_company_nomoney(data):
    ''' 获取非货币型规模排名
    '''
    non_monetary = data[data['六大类型'] != '货币类']
    sum_non_monetary = non_monetary.groupby(by = ['company','start_up_date'])['fund_value'].sum()
    lmtz = pd.DataFrame(sum_non_monetary, columns=['fund_value'])
    lmtz['rank'] = lmtz['fund_value'].rank()
    lmtz['rank_pct'] = lmtz['rank'] / len(lmtz['rank'])
    return lmtz

def get_company_fund(data):
    ''' 偏股型产品值统计
    '''
    non_monetary = data[data['六大类型'] == '偏股类']
    sum_non_monetary = non_monetary.groupby(by = ['company','start_up_date'])['fund_value'].sum()
    lmtz = pd.DataFrame(sum_non_monetary, columns=['fund_value'])
    lmtz['rank'] = lmtz['fund_value'].rank()
    lmtz['rank_pct'] = lmtz['rank'] / len(lmtz['rank'])
    return lmtz

def get_fund_type_dataframe(rptdate):
    sql_classify = '''select  cpdm, yjfl,ejfl,dl  from t_fund_classify_his where rptdate = '{}'
    '''.format(rptdate)
    data = pd.DataFrame(cu_pra.execute(sql_classify).fetchall(),columns=['基金代码','一级分类','二级分类','六大类型'])
    print(data)
    data = data[['基金代码','六大类型']]
    return data

def get_fof_fund_alpha_rank_from_db(company_code, base_date, fof_type='IF1'):
    fund_code = company_code + 'FOF' + fof_type
    sql = f'''
        select dl_rank
        from t_fof_rank
        where fundid = '{fund_code}'
        and indexcode = 'AlphaCategroy3Year'
        and reportcycle = '{base_date}'
    '''
    sql_rst = cu_pra.execute(sql).fetchall()
    if not sql_rst:
        t_logger.warning('Get alpha rank failed: company_code: {}, base_date: {}'.format(company_code, base_date))
        return None
    rst = sql_rst[0][0]
    return rst

def get_fof_fund_risk_rank_from_db(company_code, base_date, fof_type='IF1'):
    fund_code = company_code + 'FOF' + fof_type
    sql = f'''
        select dl_rank
        from t_fof_rank
        where fundid = '{fund_code}'
        and indexcode = 'RiskIndex3Year'
        and reportcycle = '{base_date}'
    '''
    sql_rst = cu_pra.execute(sql).fetchall()
    if not sql_rst:
        t_logger.warning('Get risk rank failed: company_code: {}, base_date: {}'.format(company_code, base_date))
        return None
    rst = sql_rst[0][0]
    return rst

def get_sum_manager_percent(date):
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
            WHERE F3_1272 IS NOT NULL AND F4_1272 IS NULL OR F4_1272 <= '%(date)s')d
            ON c.F1_1099 = d.F1_1272
            GROUP BY c.OB_OBJECT_NAME_1018,c.F35_1018,d.F2_1272)e
            GROUP BY e.OB_OBJECT_NAME_1018,e.F35_1018
    
            '''% {'date': date}

    sum_manager = pd.DataFrame(cu.execute(sql).fetchall(), columns=['company', 'start_up_date', 'sum_manager'])
    #sum_manager = sum_manager.sort_values("sum_manager", axis=1, ascending=False, inplace=True)
    #sum_manager["percentile"] = np.percentile(sum_manager["sum_manager"], sum_manager["sum_manager"])
    return sum_manager

def my_run(base_date):
    t_logger.info('base_date: {}'.format(base_date))


    '''
    # 查到基于base_date的次新基金代码
    new_fund_codes = get_new_fund_from_db(base_date)
    fund_cnts = len(new_fund_codes)

    # 计算alpha 和 std 需求
    start_date, end_date =  gen_start_end_date(base_date)
    t_logger.info(f'totally {fund_cnts}')

    # 获取基金公司对应基金产品的基金净值数据
    # fund_value.shape = (7704, 4)
    fund_value = get_fund_value()

    # 获取fof基金分类信息表
    # fund_type = (5172, 4)
    # 手动设定取类型从上一年取
    fund_type = get_fund_type_dataframe('20181231')

    # 组合基金类型和基金净值
    # fund_data.shape (7704, 5)
    # columns = ['基金代码', '六大类型', 'company', 'start_up_date', 'fund_value']
    fund_data = pd.merge(fund_type, fund_value, on='基金代码', how='right')

    # 基金公司非货币基金产品总数标
    company_nomoney_dt = get_company_nomoney(fund_data)
    logging.info('### company_nomoney_dt ###')
    logging.info(company_nomoney_dt)
    # 基金公司偏股型基金产品总数标
    company_fund_dt = get_company_fund(fund_data)
    logging.info('### company_nomoney_dt ###')
    logging.info(company_fund_dt)
    '''
    ###     投研团队的相关标签
    # 基金公司基金经理人数
    # sum_manager_num = get_sum_manager_percent(base_date)
    # 基金经理团队经验()
    # 基金经理稳定性(离职率百分比)


    ###     基金经理维度
    fund_manager_info = get_fund_manager_info(base_date)


    rst_dict = {}
    rst_dt = pd.DataFrame()
    # 这里股票类型没有按照 偏股 和 偏债进行区分????
    for fund_code in new_fund_codes:
        t_logger.info(f'fund code: {fund_code}')

        # 获取基金公司信息
        company_code, company_name, start_up_date, manager_id = get_company_info(fund_code)

        '''
        # 偏股产品集FOF近三年的相对收益指标Alpha
        fof_fund_alpha = get_fof_fund_alpha_rank_from_db(company_code, base_date, fof_type='IF1')
        rst_dict.setdefault('fof_fund_alpha', []).append(fof_fund_alpha)
        fof_fund_risk = get_fof_fund_risk_rank_from_db(company_code, base_date, fof_type='IF1')
        rst_dict.setdefault('fof_fund_risk', []).append(fof_fund_risk)

        # 非货基规模百分比
        try:
            nomoney_product_sum_pct = company_nomoney_dt.loc[(company_name, start_up_date)]['rank_pct']
        except Exception as err:
            t_logger.warning('lingyao look here {}'.format(err))
            nomoney_product_sum_pct = None
        rst_dict.setdefault('nomoney', []).append(nomoney_product_sum_pct)

        # 偏股规模百分比 
        try:
            fund_product_sum_pct = company_fund_dt.loc[(company_name, start_up_date)]['rank_pct']
        except Exception as err:
            t_logger.warning('lingyao look here {}'.format(err))
            fund_product_sum_pct = None
        rst_dict.setdefault('stock_fund', []).append(nomoney_product_sum_pct)

        # 计算近3年alpha
        alpha = compute_alpha(fund_code, start_date, end_date)
        rst_dict.setdefault('alpha', []).append(alpha)

        # 计算近三年波动率
        std = compute_std(fund_code, start_date, end_date)
        rst_dict.setdefault('std', []).append(std)
        '''

        #### 基金经理维度指标
        #  基金经理产品集FOF完整生涯的规模能力百分位排名 
        #  基金经理管理年限（多人管理等权重）的百分位排名
        #  基金经理产品集FOF近三年的相对收益指标Alpha的百分位排名
        #  基金经理产品集FOF近三年的风险管理指标的百分位排名
        #  基金经理产品集FOF近三年的择时能力的百分位排名
        #  基金经理产品集FOF近三年的择股能力的百分位排名



    for col, value_list in rst_dict.items():
        rank_rst = get_rank_result(col, value_list)
        rst_dt[col + '百分比'] = rank_rst
    if not rst_dt.empty:
        rst_dt.index = new_fund_codes
        rst_dt.to_excel('report.xlsx')
    return

def main_run():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', dest='date', default='20190331', help='report base date')
    args = parser.parse_args()
    my_run(args.date)
    return

if __name__ == '__main__':
    main_run()
