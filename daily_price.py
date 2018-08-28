#-*- coding=utf-8 -*-
import requests
from io import StringIO
import pandas as pd
import numpy as np
import os.path
import sqlite3 
from datetime import datetime
import sys

datestr = None
if len(sys.argv) > 1:
    datestr = sys.argv[1]
else:
    datestr = '20180801'

if datetime.strptime(datestr,'%Y%m%d').weekday() > 4 :
    print("not work day")
    sys.exit()

datestr_format = datetime.strptime(datestr,'%Y%m%d').strftime('%Y-%m-%d')
filename = datestr + ".csv"

if not os.path.isfile(datestr + ".csv"):
    print("\ndownloading..... ")
    r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL')
    with open(datestr + ".csv", "wb") as file:
        file.write(r.text.encode('utf-8'))

txt = None
with open(filename, encoding='utf-8') as f:
        txt = f.read()

#pd.concat([df.query('col1 == 1') for df in pd.read_csv(StringIO(txt), chunksize=1)])

df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) 
                                     for i in txt.split('\n') 
                                     if len(i.split('",')) == 17 and i[0] != '='])), header=0)
df = df.iloc[:, :-1]
df['deal_date'] = datestr_format
df = df.astype(str).apply(lambda s: s.str.replace(',',''))
df.columns = ["stock_id","stock_name","volume","tx_records","tx_money","open","high","low","close","dev","dev_percent","final_buy_price","final_buy_quantity","final_sell_price","final_sell_quantity","pe_ratio","deal_date"]
cnx = sqlite3.connect('twstock.db')
df.to_sql("daily_price",con = cnx,index=False,if_exists='append',index_label=False)
