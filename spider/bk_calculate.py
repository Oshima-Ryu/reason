import json

from spider.bk_daily_untils import get_all_lines

def get_bk_lines_data(bk_code):
    bk_klines = get_all_lines(bk_code)
    bk_line_list = []
    for item in bk_klines:
        temp = json.loads(item["bk_info"])
        bk_dict = {**item, **temp}
        del bk_dict['bk_info']
        bk_line_list.append(bk_dict)
    print(bk_line_list)


def cal_MA():
    bk_klines_pd = get_bk_lines_data("BK1032")


def start_calculate():
    cal_MA()


if __name__ == "__main__":
    start_calculate()
