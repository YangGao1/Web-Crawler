# -*- coding: utf-8 -*-
"""
Created on 2018-03-06

@author: yang gao
"""
import urllib2
import time
import os
import numpy as np
import pandas as pd
import sys
import codecs
import csv
import shutil
from urllib import urlretrieve
import itertools

reload(sys)
sys.setdefaultencoding('utf-8')


class dailydata(object):
    "Search Data"

    def __init__(self, date):
        self.date = date

    def update_SHFE(self):
        filepath_rawdata = os.path.dirname(os.path.dirname(os.path.abspath("SHFE_query_price,py")))
        savepath_root = r'{}\raw_data2'.format(filepath_rawdata)
        today = self.date
        year = today[0:4]
        month = today[4:6]
        day = today[6:8]
        print year + month + day
        savepath_1 = savepath_root + '\\' + today[0:4] + '\\' + today[0:6] + '\\' + today[0:8]
        if not os.path.isdir(savepath_1):
            os.makedirs(savepath_1)
        url = r'http://www.shfe.com.cn/data/dailydata/kx/kx{}{}{}.dat'.format(year, month, day)
        print url
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}

        try:
            req = urllib2.Request(url, headers=headers)
            f = urllib2.urlopen(req)
            h = f.read()
            f.close()

            data_total = eval(h)
            data_instrument = data_total['o_curinstrument']

            i = 0
            while i < len(data_instrument) - 2:
                if '\xe6\x80\xbb\xe8\xae\xa1' in data_instrument[i]['PRODUCTNAME']:
                    break
                if not data_instrument[i]['DELIVERYMONTH'] == '\xe5\xb0\x8f\xe8\xae\xa1':
                    product_id = data_instrument[i]['PRODUCTID'].split('_')[0]
                    index1 = []
                    instru_day = []
                    while i < len(data_instrument) - 2 and not data_instrument[i][
                        'DELIVERYMONTH'] == '\xe5\xb0\x8f\xe8\xae\xa1':
                        index1 = [product_id + data_instrument[i]['DELIVERYMONTH']]
                        instru_day = [data_instrument[i]]
                        i += 1
                        df = pd.DataFrame(instru_day, index=index1)
                        df['TradingDay'] = [today]
                        df['TurnOver'] = [0]
                        df.columns = ['ClosePrice', 'deliverymonth', 'HighestPrice', 'LowestPrice', 'OpenInterests',
                                      'openinterestchg', 'OpenPrice', 'orderno', 'presettlementprice', 'productid',
                                      'productname', 'productsortno', 'SettlementPrice', 'TotalVolume', 'increase1',
                                      'increase2', 'TradingDay', 'TurnOver']
                        df.fillna('missing')
                        df_temp = pd.DataFrame(df.loc[[df.index], ['TradingDay', 'ClosePrice', 'HighestPrice',
                                                                   'LowestPrice', 'OpenInterests', 'OpenPrice',
                                                                   'SettlementPrice', 'TotalVolume', 'TurnOver']])
                        df_temp.to_csv(savepath_1 + '\\' + index1[0] + '.SHFE' + '.csv', index=False, encoding='gb2312')



                else:
                    i += 1
            print today, 'success'


        except:
            print today, "faliure"

    def update_DCE(self):
        def restoreNumber(numStr):
            if type(numStr) == str or type(numStr) == unicode:
                numList = numStr.split(',')
                numStr = ''.join(numList)
            return float(numStr)

        instruments = ['a', 'b', 'bb', 'c', 'cs', 'fb', 'i', 'j', 'jd', 'jm', 'l', 'm', 'p', 'pp', 'v', 'y']
        filepath_rawdata = os.path.dirname(os.path.dirname(os.path.abspath("SHFE_query_price,py")))
        savepath_root = r'{}\raw_data2'.format(filepath_rawdata)
        today = self.date
        year = today[0:4]
        month = today[4:6]
        month2 = int(today[5]) - 1
        query_month = str(month2)
        day = today[6:8]
        print year + month + day
        savepath_1 = savepath_root + '\\' + today[0:4] + '\\' + today[0:6] + '\\' + today[0:8] + '\\'
        for instru in instruments:
            url = 'http://www.dce.com.cn/publicweb/quotesdata/exportDayQuotesChData.html?' \
                  'dayQuotes.variety={}&dayQuotes.trade_type=0&year={}&month={}&day={}&exportFlag=excel' \
                .format(instru, year, query_month, day)
            save_file = savepath_1 + year + month + day + '.xls'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
            req = urllib2.Request(url, headers=headers)
            f = urllib2.urlopen(req)
            data = f.read()
            if len(data) > 13824:
                with open(save_file, 'wb') as code:
                    code.write(data)
                f.close()

            savepath_2 = savepath_root + '\\' + today[0:4] + '\\' + today[0:6] + '\\' + today[0:8]

            df = pd.read_excel(save_file)
            df.columns = ['commodity', 'contract', 'OpenPrice', 'HighestPrice', 'LowestPrice', 'ClosePrice',
                          'SettlementPrice22',
                          'SettlementPrice', 'increase1', 'increase2', 'TotalVolume', 'OpenInterest', 'openintrestchg',
                          'Turnover']
            df = df.drop(range(len(df.index) - 2, len(df.index)))
            df['contract'] = instru + df['contract'].apply(lambda x: str(int(x)))
            number_col = ['ClosePrice', 'HighestPrice', 'LowestPrice', 'OpenInterest', 'OpenPrice', 'SettlementPrice',
                          'TotalVolume', 'Turnover']

            for col in number_col:
                df[col] = df[col].apply(lambda x: restoreNumber(x))

            for index1 in df.index:
                df_temp = df.loc[[index1], ['ClosePrice', 'HighestPrice', 'LowestPrice', 'OpenInterest', 'OpenPrice',
                                            'SettlementPrice', 'TotalVolume', 'Turnover']]
                df_temp['TradingDay'] = [today]
                df_temp2 = df_temp.loc[
                    [index1], ['TradingDay', 'ClosePrice', 'HighestPrice', 'LowestPrice', 'OpenInterest', 'OpenPrice',
                               'SettlementPrice', 'TotalVolume', 'Turnover']]
                df_temp2.to_csv(savepath_2 + '\\' + str(df.loc[index1, 'contract']) + '.DCE' + '.csv', index=False,
                                encoding='gb2312')

            os.remove(save_file)

    def update_CFFEX(self):
        filepath_rawdata = os.path.dirname(os.path.dirname(os.path.abspath("CFFEX_query_price,py")))
        savepath_root = r'{}\raw_data2'.format(filepath_rawdata)
        today = self.date
        year = today[0:4]
        month = today[4:6]
        day = today[6:8]
        print year + month + day
        savepath_1 = savepath_root + '\\' + today[0:4] + '\\' + today[0:6] + '\\' + today[0:8] + '\\'
        savepath2 = savepath_root + '\\' + today[0:4] + '\\' + today[0:6] + '\\' + today[0:8]
        if not os.path.isdir(savepath_1):
            os.makedirs(savepath_1)
        savefile = savepath_1 + year + month + day + '.csv'
        url = r'http://www.cffex.com.cn/fzjy/mrhq/{}{}/{}/{}{}{}_1.csv'.format(year, month, day, year, month, day)
        print url
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
        req = urllib2.Request(url, headers=headers)

        try:
            f = urllib2.urlopen(req)
            data = f.read()
            with open(savefile, 'wb') as code:
                code.write(data)
            print today, 'success'
            f.close()
            df = pd.read_csv(savefile)
            df.columns = ['code', 'OpenPrice', 'HighestPrice', 'LowestPrice', 'TotalVolume', 'TurnOver', 'Holding',
                          'ClosePrice', 'SettlementPrice', 'Increase1', 'Increase2', 'IV', 'Delta']
            for index1 in df.index:

                if not df['code'][index1] == '\xd0\xa1\xbc\xc6' and not df['code'][index1] == '\xba\xcf\xbc\xc6':
                    instr_id = df['code'][index1]
                    instr_id = instr_id.replace(' ', '')
                    df_temp = df.loc[
                        [index1], ['ClosePrice', 'HighestPrice', 'LowestPrice', 'OpenInterest', 'OpenPrice',
                                   'SettlementPrice', 'TotalVolume', 'TurnOver']]
                    df_temp['TradingDay'] = [today]
                    df_temp['OpenInterest'] = [0]
                    df_temp2 = df_temp.loc[
                        [index1], ['TradingDay', 'ClosePrice', 'HighestPrice', 'LowestPrice', 'OpenInterest',
                                   'OpenPrice', 'SettlementPrice', 'TotalVolume', 'TurnOver']]
                    df_temp2.to_csv(savepath2 + '\\' + instr_id + '.CFFEX' + '.csv', index=False, encoding='gb2312')

                else:
                    pass

            os.remove(savefile)




        except:
            print today, 'failure'

    def update_CZCE(self):
        def restoreNumber(numStr):
            if type(numStr) == str or type(numStr) == unicode:
                numList = numStr.split(',')
                numStr = ''.join(numList)
            return float(numStr)
        filepath_rawdata = os.path.dirname(os.path.dirname(os.path.abspath("CZCE_query_price,py")))
        savepath_root = r'{}\raw_data2'.format(filepath_rawdata)
        today = self.date
        year = today[0:4]
        month = today[4:6]
        day = today[6:8]
        print year + month + day
        savepath_1 = savepath_root + '\\' + today[0:4] + '\\' + today[0:6] + '\\' + today[0:8] + '\\'
        savepath2 = savepath_root + '\\' + today[0:4] + '\\' + today[0:6] + '\\' + today[0:8]
        if not os.path.isdir(savepath_1):
            os.makedirs(savepath_1)
        savefile = savepath_1 + year + month + day + '.xls'
        url = r'http://www.czce.com.cn/portal/DFSStaticFiles/Future/{}/{}/FutureDataDaily.xls'.format(year,
                                                                                                      year + month + day)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
        req = urllib2.Request(url, headers=headers)

        try:
            f = urllib2.urlopen(req)
            data = f.read()
            with open(savefile, 'wb') as code:
                code.write(data)
            f.close()
            print today, 'success'
            df = pd.read_excel(savefile,header=1)
            df.columns = ['code','YesterdaySettle','OpenPrice','HighestPrice','LowestPrice','ClosePrice','SettlementPrice','Increase1','Increase2','TotalVolume','ShortVolume','Diff','TurnOver','JG']
            df = df.drop(range(len(df.index) - 2, len(df.index)))
            number_col = ['ClosePrice', 'HighestPrice', 'LowestPrice', 'OpenPrice', 'SettlementPrice',
                          'TotalVolume', 'TurnOver']

            for col in number_col:
                df[col] = df[col].apply(lambda x: restoreNumber(x))

            for index1 in df.index:
                if not df['code'][index1] == u'\u5c0f\u8ba1':

                    instr_id = df['code'][index1]
                    instr_id = str(instr_id)
                    df_temp = df.loc[
                        [index1], ['ClosePrice', 'HighestPrice', 'LowestPrice', 'OpenInterest', 'OpenPrice',
                                   'SettlementPrice', 'TotalVolume', 'TurnOver']]
                    df_temp['TradingDay'] = [today]
                    df_temp['OpenInterest'] = [0]
                    df_temp2 = df_temp.loc[
                        [index1], ['TradingDay', 'ClosePrice', 'HighestPrice', 'LowestPrice', 'OpenInterest',
                                   'OpenPrice', 'SettlementPrice', 'TotalVolume', 'TurnOver']]
                    df_temp2.to_csv(savepath2 + '\\' + instr_id + '.CZCE' + '.csv', index=False, encoding='gb2312')
                else:
                    pass

            os.remove(savefile)


        except:
            print today, 'failure'


if __name__ == '__main__':
    today = dailydata('20180302')

    today.update_SHFE()
    today.update_DCE()
    today.update_CFFEX()
    today.update_CZCE()
