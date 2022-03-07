import json
import time

import requests

from spider.bk_daily_untils import get_all_lines, get_bk_daily_info_by_date, save_bk_cal, init_bk_daily_data, \
    get_bk_all_data, update_cal_info_daily_data
from spider.bk_list_spider import get_bk_list
from spider.config import TS_TOKEN, START_DATE
from spider.stock_daily_utils import batch_get_stock_daily_data
import tushare as ts
import pandas as pd

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

bk_stock_map = {}
bk_info_map = {}


def init():
    global bk_stock_map, bk_info_map
    bk_stock_map = get_bk_stock_map()
    bk_info_map = get_bk_info_map()


def convert_date_str(date_key):
    timeArray = time.strptime(date_key, "%Y-%m-%d")
    otherStyleTime = time.strftime("%Y%m%d", timeArray)
    return otherStyleTime


def convert_date_strV2(date_key):
    timeArray = time.strptime(date_key, "%Y%m%d")
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    return otherStyleTime


def get_data(url):
    r = requests.get(url=url)
    data_text = r.text
    return data_text


def extract_data(data_text):
    left_index = data_text.find(":{")
    right_index = data_text.rfind("}")
    temp = data_text[left_index+1:right_index]
    temp_json = json.loads(temp)["diff"]
    bk_info_list = []
    for item in temp_json:
        bk_info = {}
        bk_info["name"] = item["f14"]
        bk_info["code"] = item["f12"]
        bk_info_list.append(bk_info)
    return bk_info_list


def get_bk_lines_data(bk_code):
    bk_klines = get_all_lines(bk_code)
    bk_line_list = []
    for item in bk_klines:
        temp = json.loads(item["bk_info"])
        bk_dict = {**item, **temp}
        del bk_dict['bk_info']
        bk_line_list.append(bk_dict)
    print(bk_line_list)


def get_bk_info_map():
    bk_info_map = {}
    bk_list = get_bk_list()
    for item in bk_list:
        bk_info_map[item["code"]] = item["name"]
    return bk_info_map


def get_bk_stock_map():
    bk_stock_map = {}
    with open('data/bk_stock', 'r') as f:
        bk_stock_data = json.load(f)
        for item in bk_stock_data:
            bk_stock_map[item["code"]] = item["stock_code"]
    return bk_stock_map


#计算市场宽度
def do_calculate_market_breath(bk_daily_data):
    bk_stock_cal_res = json.loads(bk_daily_data["bk_cal"])
    bk_code = bk_daily_data["bk_code"]
    bk_stock_list = bk_stock_map[bk_code]  # 板块下的股票代码
    date_key = convert_date_str(bk_daily_data["date_key"])
    bk_stock_cal_res["bk_code"] = bk_code
    bk_stock_cal_res["date_key"] = bk_daily_data["date_key"]

    stock_data_list = batch_get_stock_daily_data(bk_stock_list, date_key)

    up_MA20 = 0
    for stock_data in stock_data_list:
        stock_info = json.loads(stock_data["stock_info"])

        if stock_info.get("MA20") is None:
            continue

        if stock_info["close"] > stock_info["MA20"]:
            up_MA20 = up_MA20 + 1

    bk_stock_cal_res["market_breath"] = up_MA20 / len(stock_data_list)  # 市场宽度
    return bk_stock_cal_res


# 计算板块下的数据
def do_calculate(bk_daily_data):
    res = do_calculate_market_breath(bk_daily_data)
    return res


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


def init_and_get_bk_daily_data(date):
    db_data = get_bk_daily_info_by_date(date)
    init_data_list = []
    need_init_bk_code_list = get_need_init_bk_code(db_data)
    for init_bk_code in need_init_bk_code_list:
        bk_daily_data = init_bk_daily_data(init_bk_code, bk_info_map[init_bk_code], convert_date_strV2(date))
        init_data_list.append(bk_daily_data)
    db_data.extend(init_data_list)
    return db_data


def get_need_init_bk_code(db_data):
    db_bk_code = []
    for db_item in db_data:
        db_bk_code.append(db_item["bk_code"])
    all_bk_code = list(bk_stock_map.keys())
    need_init_bk_list = list(set(all_bk_code).difference(set(db_bk_code)))
    return need_init_bk_list


#天维度，计算板块指标：市场宽度
def calculate_one_day():
    need_cal_date_list = get_need_cal_date()
    for date in need_cal_date_list:
        if date < START_DATE:
            continue
        print("bk calculate start, date:", date)
        bk_daily_data_list = init_and_get_bk_daily_data(date)
        bk_cal_res = do_calculate_batch(bk_daily_data_list)
        save_bk_cal(bk_cal_res)


def do_calculate_line_feature(bk_data_list):
    bk_feature_list = []
    for item in bk_data_list:
        if item.get("bk_cal") is None:
            bk_data = {"bk_code": item["bk_code"], "date_key": item["date_key"]}
        else:
            bk_data = json.loads(item["bk_cal"])
        if item.get("bk_info") is None:
            continue
        bk_data['close'] = float(json.loads(item["bk_info"])['closing_price'])
        bk_feature_list.append(bk_data)
    bk_info_df = pd.DataFrame(bk_feature_list).sort_values("date_key")
    bk_info_df['MA5'] = bk_info_df.close.rolling(window=5).mean()  # M5移动平均线
    bk_info_df['MA10'] = bk_info_df.close.rolling(window=10).mean()  # M10移动平均线
    # bk_info_df['MA20'] = bk_info_df.close.rolling(window=20).mean()  # M20移动平均线
    # bk_info_df['MA30'] = bk_info_df.close.rolling(window=30).mean()  # M30移动平均线
    # bk_info_df['MA60'] = bk_info_df.close.rolling(window=60).mean()  # M60移动平均线
    bk_info_df.eval('BIAS5 = (close - MA5) / MA5 * 100', inplace=True)  # 5日乖离率
    bk_info_df.eval('BIAS10 = (close - MA10) / MA10 * 100', inplace=True)  # 10日乖离率
    return bk_info_df



def get_bk_list():
    url = "http://36.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112403426913336747959_1643969051370&pn=1&pz=200&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222&_=1643969051371"
    data_text = get_data(url)
    data_dict = extract_data(data_text)
    return data_dict


def save_cal_data(stock_calculate_pd):
    bk_dict_list = []
    for i, r in stock_calculate_pd.iterrows():
        stock_dict = r.to_dict()
        if stock_dict["date_key"] < convert_date_strV2(START_DATE):
            continue
        bk_dict_list.append(stock_dict)
    if len(bk_dict_list) == 0:
        return
    update_cal_info_daily_data(bk_dict_list)


def calculate_and_save_bk_data(all_bk_list):
    i = 1
    for bk_info in all_bk_list:
        print(i, bk_info, len(all_bk_list))
        i = i + 1
        bk_data_list = get_bk_all_data(bk_info["code"])
        if len(bk_data_list) == 0:
            continue
        bk_calculate_pd = do_calculate_line_feature(bk_data_list)
        save_cal_data(bk_calculate_pd[-15:])


def get_all_bk_code():
    return get_bk_list()


#计算板块的趋势指标：均线
def calculate_bk_kline():
    all_code_list = get_all_bk_code()
    calculate_and_save_bk_data(all_code_list)


def start_calculate():
    init()
    calculate_bk_kline()
    calculate_one_day()


if __name__ == "__main__":
    init()
    start_calculate()
