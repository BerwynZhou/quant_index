import os
import pandas as pd
import baostock as bs
from tqdm import tqdm


class DownloadTradeData:
    def __init__(self, start_day, today, filename, update):
        self.today = today
        self.start_day = start_day
        self.filename = filename
        self.update = update

    def get_data(self, rs):
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        return result

    def download_history_data(self, code):
        rs = bs.query_history_k_data_plus(code,
                                          "date,code,close,turn,amount,volume,peTTM,isST",
                                          start_date=self.start_day,
                                          end_date=self.today,
                                          frequency="d",
                                          adjustflag="2")
        data = self.get_data(rs)
        return data

    def get_read_data(self):
        if os.path.exists(self.filename):
            df = pd.read_csv(self.filename, index_col='Unnamed: 0')
            if self.update:
                lg = bs.login()
                rs = bs.query_stock_basic()
                rs = rs.get_data()
                code_list = list(rs[rs["type"] == "1"]["code"])

                ndata = []
                for code in tqdm(code_list):
                    temp = self.download_history_data(code)
                    col = temp.columns
                    v = temp.values.tolist()
                    ndata.extend(v)

                ndf = pd.DataFrame(data=ndata, columns=col)
                lg = bs.logout()
                df = pd.concat([df, ndf])
                df.to_csv(self.filename)
            else:
                pass
        else:
            lg = bs.login()
            rs = bs.query_stock_basic()
            rs = rs.get_data()
            code_list = list(rs[rs["type"] == "1"]["code"])

            ndata = []
            for code in tqdm(code_list):
                temp = self.download_history_data(code)
                col = temp.columns
                v = temp.values.tolist()
                ndata.extend(v)
            ndf = pd.DataFrame(data=ndata, columns=col)
            lg = bs.logout()

            df = ndf.copy()
            df.to_csv(self.filename)

        df = df.set_index(['code', 'date'])
        df = df.loc[~df.index.duplicated(keep='last')]

        def floatfun(x):
            import numpy as np
            try:
                r = float(x)
            except:
                r = np.nan
            return r

        df = df.applymap(lambda x: floatfun(x))

        return df

def concat_no_duplicate_columns(df1, df2):
    common_columns = list(set(df1.columns) & set(df2.columns))
    df2_unique_columns = list(set(df2.columns) - set(common_columns))
    result_df = pd.concat([df1, df2[df2_unique_columns]], axis=1)
    return result_df
class CalData:
    def __init__(self, df, target_day, rolling_dic, rank_dic, max_num):
        self.df = df
        self.target_day = target_day
        self.rolling_dic = rolling_dic
        self.rank_dic = rank_dic
        self.max_num = max_num

    def get_temp(self):
        daterange = sorted(list(set([x[1] for x in self.df.index.tolist()])))
        dr = daterange[daterange.index(self.target_day) - self.max_num * 2:daterange.index(self.target_day) + 1]
        temp = self.df.loc[(slice(None), dr), :]
        return temp, dr

    def fun_rolling(self, temp, dr, col):
        N1, N2, N3, N4, N5 = self.rolling_dic[col]

        unstack_data = temp[col].unstack().T
        ma_n1 = pd.DataFrame(unstack_data.rolling(N1).mean().unstack())
        ma_n2 = pd.DataFrame(unstack_data.rolling(N2).mean().unstack())
        min_n2 = pd.DataFrame(unstack_data.rolling(N3).min().unstack())
        max_n2 = pd.DataFrame(unstack_data.rolling(N4).max().unstack())
        vol_n2 = pd.DataFrame(unstack_data.rolling(N5).std().unstack())

        raw_columns = temp[['close', 'volume', 'turn', 'amount', 'peTTM']]

        new_columns = [f'Ma{N1}_{col}', f'Ma{N2}_{col}', f'Min{N2}_{col}', f'Max{N2}_{col}', f'Vol{N2}_{col}',
                       f'Ma{N1}/Ma{N2}_{col}']
        calculated_columns = [ma_n1, ma_n2, min_n2, max_n2, vol_n2, ma_n1 / ma_n2]

        for idx, new_col in enumerate(new_columns):
            raw_columns.insert(raw_columns.columns.get_loc(col) + idx + 1, new_col, calculated_columns[idx])

        result = raw_columns.loc[(slice(None), dr[-N2:]), :].dropna()
        return result

    def fun_rank(self, temp, col, ascending=True):
        data = temp.copy()

        # 对每个指标使用 groupby 和 rank 方法进行排序
        data['rank_close'] = data.groupby('code')['close'].rank(method='dense', ascending=False)
        data['rank_turn'] = data.groupby('code')['turn'].rank(method='dense', ascending=False)
        data['rank_amount'] = data.groupby('code')['amount'].rank(method='dense', ascending=False)
        data['rank_volume'] = data.groupby('code')['volume'].rank(method='dense', ascending=False)
        data['rank_peTTM'] = data.groupby('code')['peTTM'].rank(method='dense', ascending=True)

        return data

    def cal_data_fun(self):
        temp, dr = self.get_temp()
        rdf = pd.DataFrame()

        for col in self.rolling_dic.keys():
            temp_rdf = self.fun_rolling(temp, dr, col)
            rdf = concat_no_duplicate_columns(rdf, temp_rdf)

        temp_rdf = self.fun_rank(rdf, col, ascending=True)
        rdf = concat_no_duplicate_columns(rdf, temp_rdf)

        return rdf
    
    # def sort_csv_by_rank(self,file_path):
    #     # 读取 CSV 文件
    #     df = pd.read_csv(file_path)
    #     sorted_df = df.sort_values(
    #     by=["rank_peTTM", "rank_amount", "rank_turn", "rank_volume", "rank_close"],
    #     ascending=True)

        # 覆盖原文件
        # sorted_df.to_csv(file_path, index=False)
