
# 讀入一些package
import sqlite3
import os
import requests
from io import StringIO
import pandas as pd
from datetime import datetime
from datetime import date
import sys
import math

# 爬取資料
def crawl_legal_person(date):
    
    datestr = date.strftime('%Y%m%d')
    datestr_format = datetime.strptime(datestr,'%Y%m%d').strftime('%Y-%m-%d')
    txt = None
    filename = "three_" + datestr + ".csv"
    print(filename)
    # 下載三大法人資料
    try:
        if not os.path.isfile(filename):
            print("\ndownloading..... ")
            r = requests.get('http://www.tse.com.tw/fund/T86?response=csv&date='+datestr+'&selectType=ALLBUT0999')#
            with open(filename, "wb") as file:
                file.write(r.text.encode('utf-8'))        

        with open(filename, encoding='utf-8') as f:
                txt = f.read()
    except:
        return None
    
    # 製作三大法人的DataFrame
    try:
        df = pd.read_csv(StringIO(txt), header=1).dropna(how='all', axis=1).dropna(how='any')
    except:
        return None
    
    # 微調整（為了配合資料庫的格式）
    # 刪除逗點
    df = df.astype(str).apply(lambda s: s.str.replace(',',''))
    # 刪除「證券代號」中的「"」和「=」
    df['stock_id'] = df['證券代號'].str.replace('=','').str.replace('"','')
    # 刪除「證券代號」這個欄位
    df = df.drop(['證券代號'], axis=1)
    df.columns = ["stock_name","for_1_buy","for_1_sell","for_1_net","for_2_buy","for_2_sell","for_2_net","trust_buy","trust_sell","trust_net","deal_net","deal_1_buy","deal_1_sell","deal_1_net","deal_2_buy","deal_2_sell","deal_2_net","three_net","stock_id"]
    print(df)
    # 設定index
    df['deal_date'] = datestr_format
    df = df.set_index(['stock_id', 'deal_date'])
    df['for_net'] = df['for_1_net'].astype(float) + df['for_2_net'].astype(float)
    # 將dataframe的型態轉成數字
    return df.apply(lambda s: pd.to_numeric(s, errors='coerce')).dropna(how='all', axis=1)

if len(sys.argv) > 1:
    datestr = sys.argv[1]
else:
    datestr = '20180801'

if datetime.strptime(datestr,'%Y%m%d').weekday() > 4 :
    print("not work day")
    sys.exit()

my_date = datetime.strptime(datestr,'%Y%m%d')
my_df = crawl_legal_person(my_date)

new_df = my_df[ ['three_net','for_net','trust_net','deal_net'] ].copy()
new_df = new_df.fillna(0)
new_df.reset_index(inplace=True)
print( new_df.query('stock_id == "3481"') )


cnx = sqlite3.connect('twstock.db')
new_df.to_sql("daily_three",con = cnx,index=False,if_exists='append',index_label=False)
