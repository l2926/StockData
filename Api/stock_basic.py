# -*- coding: UTF-8 -*-

import time
import pymysql
import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import datetime
import sys

# 股票列表
def stock_list():
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/stock_list?charset=utf8')

    print("stock_list:")

    list_status = {"L", "D", "P"}
    for statu in list_status:
        try:
            df1 = pro.stock_basic(exchange='', list_status=statu)
            print(statu)
            df1.to_sql("stock_list", engine, if_exists='append')
        except Exception as e:
            print(e)
        else:
            print("OK")


# 获取行业成员
def industry_member():
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    print("industry_member:")

    try:
        engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/stock_list?charset=utf8')

        # 获取行业分类
        index_classify_df = pro.index_classify(src="SW2021")
        i = 0

        data = pro.stock_basic()
        # print(data)

        # 初始化data行业相关字段
        data["index_code_l1"] = ""
        data["industry_name_l1"] = ""
        data["index_code_l2"] = ""
        data["industry_name_l2"] = ""
        data["index_code_l3"] = ""
        data["industry_name_l3"] = ""

        # 遍历各行业
        for index, row in index_classify_df.iterrows():
            time.sleep(1)
            index_code = row['index_code']
            industry_name = row['industry_name']
            level = row['level']
            # print(level)

            # 获取行业成员
            member = pro.index_member(index_code=index_code, fields='con_code,in_date,out_date,is_new')
            # print(member)

            # 检查成员是否已过期
            member = member[member['is_new'] == 'Y']

            # 成员去重
            member.drop_duplicates('con_code', 'first', inplace=True)

            # 所有股票加上行业标识
            member["is_exist"] = 1
            member.rename(columns={'con_code': 'ts_code'}, inplace=True)

            # 输出统计
            print("====")
            print(data.count()['ts_code'])
            print(member.count()['ts_code'])

            # 数据合并
            data = pd.merge(data, member, how='left', on='ts_code', suffixes=('', '_x'))
            # print(data)

            # 指定一级行业index_code和industry_name
            if level == 'L1':
                data.loc[(data['is_exist'] == 1), 'index_code_l1'] = index_code
                data.loc[(data['is_exist'] == 1), 'industry_name_l1'] = industry_name

            # 指定二级行业index_code和industry_name
            if level == 'L2':
                data.loc[(data['is_exist'] == 1), 'index_code_l2'] = index_code
                data.loc[(data['is_exist'] == 1), 'industry_name_l2'] = industry_name

            # 指定三级行业index_code和industry_name
            if level == 'L3':
                data.loc[(data['is_exist'] == 1), 'index_code_l3'] = index_code
                data.loc[(data['is_exist'] == 1), 'industry_name_l3'] = industry_name

            data.drop('is_exist', axis=1, inplace=True)
            print(level + ":" + industry_name + ":" + index_code)

        print(data)
        # data.to_csv("industry_member.csv")
        data.to_sql("industry_member", engine, if_exists='replace')
    except Exception as e:
        print(e)



# 股票信息
def stock_company():
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/stock_list?charset=utf8')

    print("stock_company:")

    df = pro.stock_company(fields="ts_code,com_name,exchange,reg_capital,setup_date,province,city,introduction,website,employees,main_business,business_scope")

    df.to_sql("stock_company", engine, if_exists='replace')