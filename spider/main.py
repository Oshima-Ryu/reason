from spider import bk_detail_spider, bk_stock_spider, stock_calculate, bk_calculate
from spider.stock_kline_spider import spider_start_special_date

if __name__ == "__main__":
    # #抓取所有板块每日数据
    bk_detail_spider.spider_start()
    #
    # #抓取所有股票每日数据
    spider_start_special_date()
    # #
    # #计算股票指标
    stock_calculate.calculate_start()

    #计算板块指标
    bk_calculate.start_calculate()
