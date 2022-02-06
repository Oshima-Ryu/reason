import time

import tushare as ts

from spider.stock_daily_utils import save_daily_data

ts.set_token('3ae5f1009c3a59e9a189fa028922aeb0e3a6285e168608a9aaefe2df')
pro = ts.pro_api()


def get_all_code():
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    return data["ts_code"].tolist()


#获取指定日期的所有股票数据
def get_all_stock_data_by_date(date):
    df = pro.daily(trade_date='20220110')


def save_stock_data(stock_data):
    stock_dict_list = []
    for i, r in stock_data.iterrows():
        stock_dict = r.to_dict()
        stock_dict_list.append(stock_dict)
    save_daily_data(stock_dict_list)


def get_and_save_stock_his_data(stock_code_list):
    i = 0
    for code in stock_code_list:
        print(i, code, len(stock_code_list))
        i = i + 1
        if i <= 3162:
            continue
        df = pro.daily(ts_code=code)
        save_stock_data(df)

        if i % 100 == 0:
            time.sleep(20)


def spider_start():
    all_code_list = get_all_code()
    get_and_save_stock_his_data(all_code_list)


if __name__ == "__main__":
    spider_start()