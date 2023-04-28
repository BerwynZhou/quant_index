# main.py
import logging
from a_share import DownloadTradeData,CalData
from config import filename, savename, start_day, today, target_day, update, rolling_dic, rank_dic

# 配置 logging 模块
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.debug("开始计算数据...")

    max_num = max([max(rolling_dic[col]) for col in rolling_dic.keys()])
    logging.debug("计算 max_num...")

    download_data = DownloadTradeData(start_day, today, filename, update)
    df = download_data.get_read_data()

    logging.debug("下载交易数据完成...")

    cal_data = CalData(df, target_day, rolling_dic, rank_dic, max_num)
    result_df = cal_data.cal_data_fun()

    logging.debug("计算数据完成...")

    result_df.to_csv(savename)
    # cal_data.sort_csv_by_rank(savename)
    
    logging.debug("数据计算完成，结果已保存到 %s", savename)

if __name__ == "__main__":
    main()