import json
import sqlite3
import time

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


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
        except :
            print("insert exception")
    conn.commit()
    conn.close()


