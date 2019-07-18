#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/4 18:23
# @Author  : GaoJian
# @File    : test.py


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

def run_alpha(self):
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
            rec = (code, 'AlphaCategroyBenchmarkAll', str(rst_value), None, self.algid,
                   report_date, self.thisfilename, self.todaydate)
            self.db_insert_session.add_info(rec)

    self.db_insert_session.finish()
