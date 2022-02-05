import json

import requests


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
    temp = data_text[left_index+1:right_index]
    temp_json = json.loads(temp)["diff"]
    bk_info_list = []
    for item in temp_json:
        bk_info = {}
        bk_info["name"] = item["f14"]
        bk_info["code"] = item["f12"]
        bk_info_list.append(bk_info)
    return bk_info_list


def get_bk_list():
    url = "http://36.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112403426913336747959_1643969051370&pn=1&pz=200&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222&_=1643969051371"
    data_text = get_data(url)
    data_dict = extract_data(data_text)
    return data_dict


def spider_start():
    bk_list = get_bk_list()
    save_data_json(bk_list, "dongfang_bk")
    print(bk_list)
    pass


if __name__ == "__main__":
    spider_start()