import json
import time

import requests

from spider.bk_list_spider import get_bk_list


def save_bk_stock_data(data_dict, name):
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
    stock_data_json = json.loads(temp)["diff"]
    return stock_data_json


def extract_code_data(stock_data):
    stock_code = []
    for stock in stock_data:
        stock_code.append(stock["f12"])
    return stock_code


def get_bk_stock_data(bk_code):
    url = "https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112302318761676120853_1644082720678&fid=f62&po=1&pz=200&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=b%3A" + bk_code + "&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124%2Cf1%2Cf13"
    data_text = get_data(url)
    stock_data = extract_data(data_text)
    stock_code_list = extract_code_data(stock_data)
    return stock_code_list


def spider_start():
    bk_list = get_bk_list()
    for bk_info in bk_list:
        print(bk_info)
        bk_stock_data = get_bk_stock_data(bk_info["code"])
        bk_info["stock_code"] = bk_stock_data
        # save_data_json(bk_kline_data, bk_info["name"])
        time.sleep(3)
    save_bk_stock_data(bk_list, "bk_stock")


if __name__ == "__main__":
    spider_start()