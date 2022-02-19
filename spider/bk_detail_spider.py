import json
import time

import requests

from spider.bk_daily_untils import save_daily_data
from spider.bk_list_spider import get_bk_list


# 领涨url
# http://push2.eastmoney.com/api/qt/clist/get?np=1&fltt=1&invt=2&cb=jQuery35105044628275367318_1643971382130&fs=b%3ABK1032&fields=f14%2Cf12%2Cf13%2Cf1%2Cf2%2Cf4%2Cf3%2Cf152&fid=f3&pn=1&pz=8&po=1&ut=fa5fd1943c7b386f172d6893dbfba10b&_=1643971382131

# 日线
# http://1.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery35105044628275367318_1643971382126&secid=90.BK1032&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&beg=0&end=20500101&smplmt=460&lmt=1000000&_=1643971382127
# http://4.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery35107091239938558553_1644332922803&secid=90.BK0475&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&end=20500101&lmt=120&_=1644332922808
# 周线
# http://98.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery35105193667456293645_1644046353261&secid=90.BK1032&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=102&fqt=1&beg=0&end=20500101&smplmt=460&lmt=1000000&_=1644046353268
from spider.config import START_DATE


def convert_date_str(date_key):
    timeArray = time.strptime(date_key, "%Y%m%d")
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    return otherStyleTime


def save_data_json(data_dict, name):
    file_name = name
    with open('data/' + file_name, 'w', encoding='utf-8') as f:
        json.dump(data_dict, f, ensure_ascii=False)


def get_data(url):
    r = requests.get(url=url)
    data_text = r.text
    return data_text


def extract_data(data_text):
    left_index = data_text.find(":{")
    right_index = data_text.rfind("}")
    temp = data_text[left_index + 1:right_index]
    kline_json = json.loads(temp)["klines"]
    kline_list = []
    for item in kline_json:
        # 日期，开盘，收盘，最高，最低，成交量，成交额，振幅（%），涨跌幅（%），涨跌额，换手率（%）
        temp_list = item.split(",")
        if temp_list[0] < convert_date_str(START_DATE):
            continue
        kline = {}
        kline["date"] = temp_list[0]  # 日期
        kline["opening_price"] = temp_list[1]  # 开盘价
        kline["closing_price"] = temp_list[2]  # 收盘价
        kline["highest_price"] = temp_list[3]  # 最高价
        kline["lowest_price"] = temp_list[4]  # 最低价
        kline["volume"] = temp_list[5]  # 成交量
        kline["turnover"] = temp_list[6]  # 成交额
        kline["amplitude"] = temp_list[7]  # 振幅
        kline["fluctuation_range"] = temp_list[8]  # 涨跌幅
        kline["change_amount"] = temp_list[9]  # 涨跌额
        kline["turnover_rate"] = temp_list[10]  # 换手率
        kline_list.append(kline)
    return kline_list


def get_bk_kline_data(bk_code):
    url = "http://1.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery35105044628275367318_1643971382126&secid=90." + bk_code + "&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&beg=0&end=20500101&smplmt=460&lmt=1000000&_=1643971382127"
    data_text = get_data(url)
    kline_data = extract_data(data_text)
    return kline_data


def spider_start():
    bk_list = get_bk_list()
    for bk_info in bk_list:
        print(bk_info)
        bk_kline_data = get_bk_kline_data(bk_info["code"])
        # save_data_json(bk_kline_data, bk_info["name"])
        save_daily_data(bk_info, bk_kline_data)
        time.sleep(5)


if __name__ == "__main__":
    spider_start()
