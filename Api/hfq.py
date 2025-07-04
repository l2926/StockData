# -*- coding: UTF-8 -*-

import time
import pymysql
import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import datetime
import sys

ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
pro = ts.pro_api()

# 创建sqlalchemy数据库连接
try:
    engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/hfq_daily?charset=utf8')

except Exception as e:
    print("sqlalchemy engine create error!")
    print(e)


stock_df = pro.stock_basic(list_status='L',fields="ts_code,symbol")


hfq_daily_list = {}
# code_list = ['000001.SZ','000002.SZ','000004.SZ']
for idx2,row2 in stock_df.iterrows():
    code = row2["ts_code"]
    print(code)
    daily_df = ts.pro_bar(ts_code=code,start_date='20200101',end_date='20250611',adj='hfq',fields='trade_date,close')
    # print(daily_df)

    for idx,row in daily_df.iterrows():
        date = row["trade_date"]
        close = row['close']
        # print("----")
        # print(date)
        # print(code)
        dic = {
                "ts_code":[code],
                "close":[close]
            }
        df = pd.DataFrame(dic)
        if hfq_daily_list.get(date) is None:
            hfq_daily_list[date] = df
        else:
            hfq_daily_list[date] = pd.concat([hfq_daily_list[date],df],ignore_index=True)

print(hfq_daily_list)
for key in hfq_daily_list:
    # print(key)
    hfq_daily_list[key].to_sql(key,engine,if_exists="replace")