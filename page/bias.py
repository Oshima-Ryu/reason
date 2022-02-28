import json
import random

from pyecharts.faker import Faker

from spider.bk_daily_untils import get_bk_data_from_date
import pandas as pd


def rebuild_data(bk_data):
    rebuild_res = []
    try:
        for item in bk_data:
            rebuild_dict = {}
            rebuild_dict["bk_name"] = item["bk_name"]
            rebuild_dict["date_key"] = item["date_key"]

            bk_cal_detail = json.loads(item["bk_cal"])
            rebuild_dict["BIAS10"] = bk_cal_detail["BIAS10"]

            rebuild_res.append(rebuild_dict)
    except Exception as e:
        print(e)

    return rebuild_res


def draw_start():
    bk_data = get_bk_data_from_date("20220211")

    show_data = rebuild_data(bk_data)

    df = pd.DataFrame(show_data).sort_values("date_key", ascending=False)
    date_key_list = ["bk_name"]
    date_key_list.extend(pd.DataFrame(df['date_key']).drop_duplicates(['date_key']).sort_values("date_key", ascending=False)['date_key'].tolist())
    print(date_key_list)
    temp = df.set_index(['bk_name', 'date_key'])['BIAS10'].unstack()

    df2 = temp.rename_axis(columns=None).reset_index()
    df2 = df2[date_key_list]
    print(df2)
    df2.to_csv("bias10.csv",encoding="utf_8_sig")


if __name__ == "__main__":
    draw_start()
