import json
import time

from spider.bk_daily_untils import get_all_lines, get_bk_daily_info_by_date, save_bk_cal
from spider.config import TS_TOKEN, START_DATE
from spider.stock_daily_utils import batch_get_stock_daily_data
import tushare as ts

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

bk_stock_map = {}


def init():
    global bk_stock_map
    bk_stock_map = get_bk_stock_map()


def convert_date_str(date_key):
    timeArray = time.strptime(date_key, "%Y-%m-%d")
    otherStyleTime = time.strftime("%Y%m%d", timeArray)
    return otherStyleTime


def get_bk_lines_data(bk_code):
    bk_klines = get_all_lines(bk_code)
    bk_line_list = []
    for item in bk_klines:
        temp = json.loads(item["bk_info"])
        bk_dict = {**item, **temp}
        del bk_dict['bk_info']
        bk_line_list.append(bk_dict)
    print(bk_line_list)


def get_bk_stock_map():
    bk_stock_map = {}
    with open('data/bk_stock', 'r') as f:
        bk_stock_data = json.load(f)
        for item in bk_stock_data:
            bk_stock_map[item["code"]] = item["stock_code"]
    return bk_stock_map


# 计算板块下的数据
def do_calculate(bk_daily_data):
    bk_stock_cal_res = {}
    bk_code = bk_daily_data["bk_code"]
    bk_stock_list = bk_stock_map[bk_code]  # 板块下的股票代码
    date_key = convert_date_str(bk_daily_data["date_key"])
    bk_stock_cal_res["bk_code"] = bk_code
    bk_stock_cal_res["date_key"] = bk_daily_data["date_key"]

    stock_data_list = batch_get_stock_daily_data(bk_stock_list, date_key)

    up_MA20 = 0
    for stock_data in stock_data_list:
        stock_info = json.loads(stock_data["stock_info"])
        if stock_info["close"] > stock_info["MA20"]:
            up_MA20 = up_MA20 + 1

    bk_stock_cal_res["market_breath"] = up_MA20 / len(stock_data_list)  # 市场宽度
    return bk_stock_cal_res


def do_calculate_batch(bk_daily_data_list):
    bk_stock_cal_res = []
    for bk_daily_data in bk_daily_data_list:
        bk_stock_cal_res.append(do_calculate(bk_daily_data))
    return bk_stock_cal_res


def get_need_cal_date():
    # 交易日-日期
    end_date = time.strftime("%Y%m%d", time.localtime())
    trade_date_list = \
    pro.trade_cal(exchange='', start_date='20210101', end_date=end_date).loc[lambda x: x['is_open'] == 1].sort_values(
        "cal_date", ascending=False)["cal_date"].tolist()
    return trade_date_list


def get_bk_daily_data(date):
    return get_bk_daily_info_by_date(date)


def start_calculate():
    init()
    need_cal_date_list = get_need_cal_date()
    for date in need_cal_date_list:
        if date < START_DATE:
            continue
        print("bk calculate start, date:", date)
        bk_daily_data_list = get_bk_daily_data(date)
        bk_cal_res = do_calculate_batch(bk_daily_data_list)
        save_bk_cal(bk_cal_res)


if __name__ == "__main__":
    start_calculate()
