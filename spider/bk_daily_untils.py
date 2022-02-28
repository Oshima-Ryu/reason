import json
import sqlite3
import time

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def convert_date_str(date_key):
    timeArray = time.strptime(date_key, "%Y%m%d")
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    return otherStyleTime


def get_bk_all_data(bk_code):
    conn = sqlite3.connect("bk.db")
    conn.row_factory = dict_factory
    c = conn.cursor()
    sql = "select * from bk_daily_info where bk_code = '" + bk_code + "' order by date_key desc"
    res = c.execute(sql).fetchall()
    return res


def get_bk_daily_info_by_date(date):
    date_key = convert_date_str(date)
    conn = sqlite3.connect("bk.db")
    conn.row_factory = dict_factory
    c = conn.cursor()
    sql = "select * from bk_daily_info where date_key = '" + date_key + "'"
    res = c.execute(sql).fetchall()
    return res


def get_bk_data_from_date(date):
    date_key = convert_date_str(date)
    conn = sqlite3.connect("../spider/bk.db")
    conn.row_factory = dict_factory
    c = conn.cursor()
    sql = "select * from bk_daily_info where date_key >= '" + date_key + "'"
    res = c.execute(sql).fetchall()
    return res


def get_all_lines(bk_code):
    conn = sqlite3.connect("bk.db")
    conn.row_factory = dict_factory
    c = conn.cursor()
    sql = "select * from bk_daily_info where bk_code = '" + bk_code + "' order by date_key desc"
    res = c.execute(sql).fetchall()
    return res


def save_daily_data(bk, bk_daily_list):
    conn = sqlite3.connect("bk.db")
    c = conn.cursor()
    for item in bk_daily_list:
        try:
            bk_code = bk["code"]
            bk_name = bk["name"]
            date_key = item["date"]
            bk_info = json.dumps(item, ensure_ascii=False)
            add_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            c.execute("insert into bk_daily_info (bk_code, bk_name, date_key, bk_info, add_time) \
                                        values(?,?,?,?,?)", (
                bk_code, bk_name, date_key, bk_info, add_time
            ))
        except Exception as e:
            print("insert exception", e)
    conn.commit()
    conn.close()


def init_bk_daily_data(bk_code, bk_name, date):
    conn = sqlite3.connect("bk.db")
    c = conn.cursor()
    try:
        bk_code = bk_code
        bk_name = bk_name
        date_key = date
        # bk_info = json.dumps(item, ensure_ascii=False)
        add_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        c.execute("insert into bk_daily_info (bk_code, bk_name, date_key, add_time) \
                                        values(?,?,?,?)", (
            bk_code, bk_name, date_key, add_time
        ))
    except Exception as e:
        print("insert exception", e)
    conn.commit()
    conn.close()
    return {"bk_name": bk_name, "bk_code": bk_code, "date_key": date}


def save_bk_cal(bk_cal_data_list):
    conn = sqlite3.connect("bk.db")
    c = conn.cursor()

    for bk_cal_data in bk_cal_data_list:
        try:
            # print(stock_dict)
            bk_code = bk_cal_data["bk_code"]
            date_key = bk_cal_data["date_key"]
            bk_cal = json.dumps(bk_cal_data, ensure_ascii=False)
            c.execute("update bk_daily_info set bk_cal = ? where bk_code = ? and date_key = ?", (
                bk_cal, bk_code, date_key
            ))
        except:
            print("update exception")
    conn.commit()
    conn.close()


def update_cal_info_daily_data(bk_cal_list):
    conn = sqlite3.connect("bk.db")
    c = conn.cursor()

    for bk_cal_dict in bk_cal_list:
        try:
            # print(stock_dict)
            bk_code = bk_cal_dict["bk_code"]
            date_key = bk_cal_dict["date_key"]
            cal_info = json.dumps(bk_cal_dict, ensure_ascii=False)
            c.execute("update bk_daily_info set bk_cal = ? where bk_code = ? and date_key = ?", (
                cal_info, bk_code, date_key
            ))
        except Exception as e:
            print("update exception", e)
    conn.commit()
    conn.close()



