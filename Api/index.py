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
def daily_sw(d1=None, d2=None):
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    # 创建sqlalchemy数据库连接
    try:
        engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/daily_index?charset=utf8')

    except Exception as e:
        print("sqlalchemy engine create error!")
        print(e)

    # 创建pymysql数据库连接
    db = pymysql.connect(host='localhost',
                         user='root',
                         password='123456',
                         database='daily_index')

    # 如果日期参数为空，则更新2006年至今的所有的数据
    if d1 == None or d2 == None:
        d1 = datetime.datetime(2001, 1, 1, 0, 0, 0, 0)
        d2 = datetime.datetime.now()
        exists = "replace"
    else:
        exists = "append"

    print("daily_sw:")

    # 确定起始日期的字符串
    st_date = d1.__format__("%Y%m%d")
    ed_date = d2.__format__("%Y%m%d")

    # print("====date====")
    # print(st_date)

    # print(ed_date)

    level_list = ['L1', 'L2', 'L3']

    for level in level_list:
        # 所有股票列表
        index_code_list = pro.index_classify(level='L1', src='SW2021', fields='index_code')

        # 开始遍历，存储每一只股票的每日行情和每日指标
        for idx, row in index_code_list.iterrows():
            time.sleep(0.5)
            index_code = row['index_code']

            # 删除旧有行情
            try:
                # 删除要插入日期的数据
                cursor = db.cursor()

                del_sql = "delete from `" + index_code + "` where trade_date >= " + st_date + " and trade_date <= " + ed_date + ";"

                cursor.execute(del_sql)

                db.commit()

            except Exception as e:
                print("sql del error!")
                print(e)

            # 插入新的行情
            try:
                # 获取当前股票的每日行情和每日指标
                daily_index_df = pro.sw_daily(ts_code=row['index_code'], start_date=st_date, end_date=ed_date)
                # print(daily_common)

                # 存储行情数据
                index_code = index_code.split('.')[0]
                daily_index_df.to_sql(index_code, engine, if_exists=exists)
                print(index_code)
            except Exception as e:
                print('sql insert error!')
                print(e)

    # 关闭mysql数据库
    db.close()

    return


# 每日行情
def daily_index(d1=None, d2=None):
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    # 创建sqlalchemy数据库连接
    try:
        engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/daily_index?charset=utf8')

    except Exception as e:
        print("sqlalchemy engine create error!")
        print(e)

    # 创建pymysql数据库连接
    db = pymysql.connect(host='localhost',
                         user='root',
                         password='123456',
                         database='daily_index')

    # 如果日期参数为空，则更新2006年至今的所有的数据
    if d1 == None or d2 == None:
        d1 = datetime.datetime(2001, 1, 1, 0, 0, 0, 0)
        d2 = datetime.datetime.now()
        exists = "replace"
    else:
        exists = "append"

    print("daily_index:")

    # 确定起始日期的字符串
    st_date = d1.__format__("%Y%m%d")
    ed_date = d2.__format__("%Y%m%d")

    # print("====date====")
    # print(st_date)

    # print(ed_date)

    # 所有股票列表
    index_list = ['000001.SH', '399001.SZ', '399005.SZ', '399006.SZ']

    # 开始遍历，存储每一只股票的每日行情和每日指标
    for index_code in index_list:
        time.sleep(0.5)

        # 删除旧有行情
        try:
            # 删除要插入日期的数据
            cursor = db.cursor()

            del_sql = "delete from `" + index_code + "` where trade_date >= " + st_date + " and trade_date <= " + ed_date + ";"

            cursor.execute(del_sql)

            db.commit()

        except Exception as e:
            print("sql del error!")
            print(e)

        # 插入新的行情
        try:
            # 获取当前股票的每日行情和每日指标
            daily_market = pro.index_daily(ts_code=index_code, start_date=st_date, end_date=ed_date)
            daily_basic = pro.index_dailybasic(ts_code=index_code, start_date=st_date, end_date=ed_date)

            daily_index = pd.merge(daily_market, daily_basic, on='trade_date', how='left', suffixes=('', '_right'))
            daily_index = daily_index.drop(columns=['ts_code_right'])

            # 存储行情数据
            index_code = index_code.split('.')[0]
            daily_index.to_sql(index_code, engine, if_exists=exists)
            print(index_code)
        except Exception as e:
            print('sql insert error!')
            print(e)

    # 关闭mysql数据库
    db.close()

    return


# 申万行业基本面数据，每日保存一个表
def shenwan_daily(d1=None, d2=None):
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    try:
        engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/shenwan_daily?charset=utf8')
    except Exception as e:
        print("error!")
        print(e)

    print("shenwan_daily:")

    if d1 == None or d2 == None:
        d1 = datetime.datetime(2025, 1, 6, 0, 0, 0, 0)
        d2 = datetime.datetime.now()
        exists = "replace"

    day = (d2 - d1).days
    print("预测shenwan_daily天数：%s" % day)
    dt = d1

    for i in range(0, day + 1):
        try:
            time.sleep(1)
            tmp_date = dt.__format__("%Y%m%d")
            print(tmp_date)
            df = pro.sw_daily(trade_date = tmp_date)
            df.to_sql(tmp_date, engine, if_exists='replace')
        except Exception as e:
            print(e)
        else:
            print("shenwan_daily:" + tmp_date)
        dt = dt + datetime.timedelta(days=1)
