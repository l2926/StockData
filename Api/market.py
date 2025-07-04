# -*- coding: UTF-8 -*-

import time
import pymysql
import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import datetime
import sys


# 每日行情
def daily_common(d1=None, d2=None):
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    # 创建sqlalchemy数据库连接
    try:
        engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/daily_common?charset=utf8')

    except Exception as e:
        print("sqlalchemy engine create error!")
        print(e)

    # 创建pymysql数据库连接
    db = pymysql.connect(host='localhost',
                         user='root',
                         password='123456',
                         database='daily_common')

    # 如果日期参数为空，则更新2006年至今的所有的数据
    if d1 == None or d2 == None:
        d1 = datetime.datetime(2001, 1, 1, 0, 0, 0, 0)
        d2 = datetime.datetime.now()
        exists = "replace"
    else:
        exists = "append"

    print("daily_common:")

    # 确定起始日期的字符串
    st_date = d1.__format__("%Y%m%d")
    ed_date = d2.__format__("%Y%m%d")

    # print("====date====")
    # print(st_date)

    # print(ed_date)

    # 所有股票列表
    stock_list = pro.stock_basic(list_status='L', fields='ts_code,symbol')

    # 开始遍历，存储每一只股票的每日行情和每日指标
    for idx, row in stock_list.iterrows():
        time.sleep(0.5)
        code = row['ts_code']
        symbol = row['symbol']

        # 删除旧有行情
        try:
            # 删除要插入日期的数据
            cursor = db.cursor()

            del_sql = "delete from `" + symbol + "` where trade_date >= " + st_date + " and trade_date <= " + ed_date + ";"

            cursor.execute(del_sql)

            db.commit()

        except Exception as e:
            print("sql del error!")
            print(e)

        # 插入新的行情
        try:
            # 获取当前股票的每日行情和每日指标
            daily_df = ts.pro_bar(ts_code=code, start_date=st_date, end_date=ed_date, adj="hfq")
            daily_basic = pro.daily_basic(ts_code=code, start_date=st_date, end_date=ed_date)

            daily_common = pd.merge(daily_df, daily_basic, on='trade_date', how='left', suffixes=('', '_right'))
            daily_common = daily_common.drop(columns=['ts_code_right', 'close_right'])

            # print(daily_common)

            # 存储行情数据
            daily_common.to_sql(symbol, engine, if_exists=exists)
            print(code)
        except Exception as e:
            print('sql insert error!')
            print(e)

    # 关闭mysql数据库
    db.close()

    return


