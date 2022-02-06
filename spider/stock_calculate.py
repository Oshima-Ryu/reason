import json

from spider.stock_daily_utils import get_stock_daily_data, save_daily_data, update_daily_data
from spider.stock_kline_spider import get_all_code
import pandas as pd


def do_calculate(stock_daily_data):
    stock_info_list = []
    for item in stock_daily_data:
        stock_info_list.append(json.loads(item["stock_info"]))
    stock_info_df = pd.DataFrame(stock_info_list).sort_values("trade_date")
    stock_info_df['MA5'] = stock_info_df.close.rolling(window=5).mean()  # M5移动平均线
    stock_info_df['MA10'] = stock_info_df.close.rolling(window=10).mean()  # M10移动平均线
    stock_info_df['MA20'] = stock_info_df.close.rolling(window=20).mean()  # M20移动平均线
    stock_info_df['MA30'] = stock_info_df.close.rolling(window=30).mean()  # M30移动平均线
    stock_info_df['MA60'] = stock_info_df.close.rolling(window=60).mean()  # M60移动平均线
    return stock_info_df


def save_cal_data(stock_calculate_pd):
    stock_dict_list = []
    for i, r in stock_calculate_pd.iterrows():
        stock_dict = r.to_dict()
        stock_dict_list.append(stock_dict)
    if len(stock_dict_list) == 0:
        return
    update_daily_data(stock_dict_list)


def calculate_and_save_stock_data(all_ts_code_list):
    i = 1
    for ts_code in all_ts_code_list:
        print(i, ts_code, len(all_ts_code_list))
        i = i+1
        stock_daily_data = get_stock_daily_data(ts_code[:6])
        if len(stock_daily_data) == 0:
            continue
        stock_calculate_pd = do_calculate(stock_daily_data)
        save_cal_data(stock_calculate_pd)


def spider_start():
    all_code_list = get_all_code()
    calculate_and_save_stock_data(all_code_list)


if __name__ == "__main__":
    spider_start()