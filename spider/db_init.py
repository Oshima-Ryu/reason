import sqlite3


def create_bk_info_table():
    conn = sqlite3.connect("bk.db")
    c = conn.cursor()
    c.execute('''create table bk_daily_info
                           (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           bk_code text,
                           bk_name text,
                           date_key text,
                           bk_info text,
                           add_time
                       );''')
    conn.close()

def create_stock_info_table():
    conn = sqlite3.connect("bk.db")
    c = conn.cursor()
    c.execute('''create table stock_daily_info
                           (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           stock_code text,
                           stock_ts_code text,
                           stock_name text,
                           date_key text,
                           stock_info text,
                           stock_cal text,
                           add_time
                       );''')
    conn.close()

if __name__ == '__main__':
    # create_bk_info_table()
    create_stock_info_table()