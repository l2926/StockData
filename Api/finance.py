# -*- coding: UTF-8 -*-

import time
import pymysql
import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import datetime
import sys


def get_fina_main():
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    engine = create_engine('mysql+pymysql://root:123456@127.0.0.1/finance?charset=utf8')

    years = ['2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018',
             '2019', '2020', '2021']
    # years = ['2016','2017','2018','2019','2020','2021']
    months = ['0630', '1231']

    stock_list = pro.stock_basic()

    res_df = pd.DataFrame()
    for idx, row in stock_list.iterrows():
        stock_id = row["ts_code"]
        df = pro.fina_mainbz_vip(ts_code=stock_id, type='P', fields="end_date,bz_item,bz_sales,bz_profit")
        print(df)
        time.sleep(0.2)
        try:
            if not df.empty:
                for y in years:
                    for m in months:
                        date = y + m

                        # print(date)
                        tmp_df = df[df["end_date"] == date]
                        tmp_df['bz_sales'] = tmp_df['bz_sales'] / 100000000
                        tmp_df['bz_profit'] = tmp_df['bz_profit'] / 100000000

                        tmp_df = tmp_df[["bz_item", "bz_sales", "bz_profit"]]
                        # print(tmp_df)

                        tmp_df = tmp_df.groupby("bz_item").agg('mean')
                        tmp_df = tmp_df.sort_values(by='bz_sales', ascending=False)

                        cur_str = ""
                        for bz_item, bz_value in tmp_df.iterrows():
                            row_str = "" + bz_item + ","
                            for col in bz_value:
                                col = round(float(col), 2)
                                row_str = row_str + str(col) + ","
                            cur_str = cur_str + row_str + ";"

                        # print(cur_str)
                        res_df.loc[idx, "ts_code"] = stock_id
                        res_df.loc[idx, date] = cur_str
                        # print(res_df)
        except Exception as e:
            print("stock_id错误:" + stock_id)
        print(stock_id + " OK!")
    # print(res_df)

    # res_df.to_csv("bbb.csv")
    res_df.to_sql("fina_main2", engine, if_exists='replace')


def get_top_hold(idx_hold):
    ts.set_token('b1e3f5ea2320e6b97d1f52aaf86f403c0428e3c79afddef809a31d6b')
    pro = ts.pro_api()

    engine = create_engine('mysql+pymysql://root:@127.0.0.1/finance?charset=utf8')

    years = ['2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018',
             '2019', '2020', '2021']
    # years = ['2016','2017','2018','2019','2020','2021']
    months = ['0331', '0630', '0930', '1231']

    stock_list = pro.stock_basic()

    res_df = pd.DataFrame()
    for idx, row in stock_list.iterrows():
        stock_id = row["ts_code"]

        if idx_hold == 0:
            df = pro.top10_floatholders(ts_code=stock_id)
        else:
            df = pro.top10_holders(ts_code=stock_id)
        # print(df)
        time.sleep(0.2)

        for y in years:
            for m in months:
                date = y + m

                # print(date)
                tmp_df = df[df["end_date"] == date]
                tmp_df['hold_amount'] = round(tmp_df['hold_amount'] / 10000, 2)

                if idx_hold == 0:
                    tmp_df = tmp_df[["holder_name", "hold_amount"]]
                else:
                    tmp_df = tmp_df[["holder_name", "hold_amount", "hold_ratio"]]
                # print(tmp_df)

                cur_str = ""
                if not tmp_df.empty:
                    tmp_df = tmp_df.groupby("holder_name").agg('mean')
                    tmp_df = tmp_df.sort_values(by='hold_amount', ascending=False)
                    # print(tmp_df)

                    for nm, row in tmp_df.iterrows():
                        # print(i)
                        # print(row)
                        # print("++++")
                        row_str = ""
                        cur_str = cur_str + nm + ","
                        for col in row:
                            # print(col)
                            row_str = row_str + str(col) + ","
                        cur_str = cur_str + row_str + ";"

                # print(cur_str)
                res_df.loc[idx, "ts_code"] = stock_id
                res_df.loc[idx, date] = cur_str
                # print(res_df)

        print(stock_id + " OK!")
    print(res_df)

    db_name = ""
    if idx_hold == 0:
        db_name = "top10_float_hold"
    else:
        db_name = "top10_hold"

    # res_df.to_csv("aaa.csv")
    res_df.to_sql(db_name, engine, if_exists='replace')


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

stock_df = pro.stock_basic(list_status='L',fields="ts_code,symbol")

for idx,row in stock_df.iterrows():
    code = row["ts_code"]
    symbol = row["symbol"]
    df = pro.balancesheet(ts_code=code,fields="total_assets,total_liab")
    print(df)
    df.to_csv(symbol)