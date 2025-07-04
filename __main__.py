from Api import stock_basic
from Api import industry
from Api import market
from Api import finance
from Api import index
import datetime


### 一段时间的更新
def range_update(st_dt, ed_dt):
    st_date = st_dt.__format__("%Y%m%d")
    end_date = ed_dt.__format__("%Y%m%d")

    print("range_update starting...")
    print("Updating:" + st_date + "--" + end_date)
    # market.daily_common(st_dt,ed_dt)
    industry.common_daily(st_dt, ed_dt)
    # index.daily_index(st_dt,ed_dt)
    # index.daily_sw(st_dt, ed_dt)
    print("Update completed:" + st_date + "--" + end_date)


### 一次性的更新
def once_update():
    # stock_basic.stock_list()
    stock_basic.stock_company()
    # stock_basic.industry_member()
    # market.daily_common()
    # industry.common_daily()
    # finance.get_fina_main()
    # index.daily_index()
    # index.daily_sw()
    # index.shenwan_daily()


### 自动更新
def auto_update():
    today_dt = datetime.date.today()
    industry.common_daily(today_dt, today_dt)
    market.daily_common(today_dt, today_dt)
    return


if __name__ == '__main__':
    st_date = datetime.datetime(2025, 4, 1)
    ed_date = datetime.datetime(2025, 4, 1)
    range_update(st_date, ed_date)
    # once_update()
    # auto_update()
