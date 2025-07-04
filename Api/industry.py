# -*- coding: UTF-8 -*-

import time
import pymysql
import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import datetime
import sys


# 行情、基本面数据
def common_daily(d1=None, d2=None):
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    try:
        engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/common_daily?charset=utf8')
    except Exception as e:
        print("error!")
        print(e)

    print("common_daily:")

    if d1 == None or d2 == None:
        d1 = datetime.datetime(2024, 1, 6, 0, 0, 0, 0)
        d2 = datetime.datetime.now()
        exists = "replace"

    day = (d2 - d1).days
    print("预测common_daily天数：%s" % day)
    dt = d1

    for i in range(0, day + 1):
        try:
            time.sleep(1)
            tmp_date = dt.__format__("%Y%m%d")
            print(tmp_date)

            cal_st_dt = dt - datetime.timedelta(days=30)
            cal_st_date = cal_st_dt.__format__("%Y%m%d")

            df_old_dates = pro.trade_cal(start_date=cal_st_date, end_date=tmp_date, is_open='1')
            old_dates = df_old_dates['cal_date'].tolist()

            # print(old_dates)

            df1 = pro.daily(trade_date=tmp_date)
            # print(df1)
            df2 = pro.daily_basic(trade_date=tmp_date,
                                  fields="ts_code,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_mv,circ_mv")
            # print(df2)
            data = pd.merge(df1, df2, how='inner', on='ts_code', suffixes=('', '_x'))

            data["conti_up"] = 0
            # print(data)

            for k in range(9):
                kk = k + 1
                df_y = pro.daily(trade_date=old_dates[kk], fields="ts_code,pct_chg")

                col_name = "y_pct" + str(k + 1)
                df_y = df_y.rename(columns={'pct_chg': col_name})

                data = pd.merge(data, df_y, how='left', on='ts_code', suffixes=('', '_x'))
                # print(k)

            # 一板
            data.loc[
                (data['pct_chg'] > 9)
                , 'conti_up'] = 1

            # 两板
            data.loc[
                (data['pct_chg'] > 9) &
                (data['y_pct1'] > 9)
                , 'conti_up'] = 2

            # 三板
            data.loc[
                (data['pct_chg'] > 9) &
                (data['y_pct1'] > 9) &
                (data['y_pct2'] > 9)
                , 'conti_up'] = 3

            # 四板
            data.loc[
                (data['pct_chg'] > 9) &
                (data['y_pct1'] > 9) &
                (data['y_pct2'] > 9) &
                (data['y_pct3'] > 9)
                , 'conti_up'] = 4

            # 五板
            data.loc[
                (data['pct_chg'] > 9) &
                (data['y_pct1'] > 9) &
                (data['y_pct2'] > 9) &
                (data['y_pct3'] > 9) &
                (data['y_pct4'] > 9)
                , 'conti_up'] = 5

            # 六板
            data.loc[
                (data['pct_chg'] > 9) &
                (data['y_pct1'] > 9) &
                (data['y_pct2'] > 9) &
                (data['y_pct3'] > 9) &
                (data['y_pct4'] > 9) &
                (data['y_pct5'] > 9)
                , 'conti_up'] = 6

            # 七板
            data.loc[
                (data['pct_chg'] > 9) &
                (data['y_pct1'] > 9) &
                (data['y_pct2'] > 9) &
                (data['y_pct3'] > 9) &
                (data['y_pct4'] > 9) &
                (data['y_pct5'] > 9) &
                (data['y_pct6'] > 9)
                , 'conti_up'] = 7

            # 八板
            data.loc[
                (data['pct_chg'] > 9) &
                (data['y_pct1'] > 9) &
                (data['y_pct2'] > 9) &
                (data['y_pct3'] > 9) &
                (data['y_pct4'] > 9) &
                (data['y_pct5'] > 9) &
                (data['y_pct6'] > 9) &
                (data['y_pct7'] > 9)
                , 'conti_up'] = 8

            # 九板
            data.loc[
                (data['pct_chg'] > 9) &
                (data['y_pct1'] > 9) &
                (data['y_pct2'] > 9) &
                (data['y_pct3'] > 9) &
                (data['y_pct4'] > 9) &
                (data['y_pct5'] > 9) &
                (data['y_pct6'] > 9) &
                (data['y_pct7'] > 9) &
                (data['y_pct8'] > 9)
                , 'conti_up'] = 9

            # 十板
            data.loc[
                (data['pct_chg'] > 9) &
                (data['y_pct1'] > 9) &
                (data['y_pct2'] > 9) &
                (data['y_pct3'] > 9) &
                (data['y_pct4'] > 9) &
                (data['y_pct5'] > 9) &
                (data['y_pct6'] > 9) &
                (data['y_pct7'] > 9) &
                (data['y_pct8'] > 9) &
                (data['y_pct9'] > 9)
                , 'conti_up'] = 10

            print(data)
            # data.to_csv("abc.csv")
            data.to_sql(tmp_date, engine, if_exists='replace')
        except Exception as e:
            print(e)
        else:
            print("common_daily:" + tmp_date)
        dt = dt + datetime.timedelta(days=1)




