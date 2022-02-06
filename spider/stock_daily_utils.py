import json
import sqlite3
import time

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_stock_daily_data(stock_code):
    conn = sqlite3.connect("bk.db")
    conn.row_factory = dict_factory
    c = conn.cursor()
    sql = "select * from stock_daily_info where stock_code = '" + stock_code + "' order by date_key desc"
    res = c.execute(sql).fetchall()
    return res


def save_daily_data(stock_dict_list):
    conn = sqlite3.connect("bk.db")
    c = conn.cursor()

    for stock_dict in stock_dict_list:
        try:
            stock_code = stock_dict["ts_code"][:6]
            stock_ts_code = stock_dict["ts_code"]
            date_key = stock_dict["trade_date"]
            stock_info = json.dumps(stock_dict, ensure_ascii=False)
            add_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            c.execute("insert into stock_daily_info (stock_code, stock_ts_code, date_key, stock_info, add_time) \
                                        values(?,?,?,?,?)", (
                stock_code, stock_ts_code, date_key, stock_info, add_time
            ))
        except :
            print("insert exception")
    conn.commit()
    conn.close()


def update_daily_data(stock_dict_list):
    conn = sqlite3.connect("bk.db")
    c = conn.cursor()

    for stock_dict in stock_dict_list:
        try:
            # print(stock_dict)
            stock_code = stock_dict["ts_code"][:6]
            date_key = stock_dict["trade_date"]
            stock_info = json.dumps(stock_dict, ensure_ascii=False)
            c.execute("update stock_daily_info set stock_info = ? where stock_code = ? and date_key = ?", (
                stock_info, stock_code, date_key
            ))
        except :
            print("update exception")
    conn.commit()
    conn.close()