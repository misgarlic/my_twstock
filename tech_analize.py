#-*- coding=utf-8 -*-
import pandas as pd
import sqlite3
import talib
import numpy as np
from datetime import datetime
from datetime import date
import sys
import dateutil.relativedelta

def Get_kd_ma(data):
    indicators={}
    #indicators['k'],indicators['d']=talib.STOCH(np.array(data['high']),np.array(data['low']),np.array(data['close']),\
    #fastk_period=9,slowk_period=3,slowk_matype=0,slowd_period=3,slowd_matype=0)
    tmp = lm_kdj(data,9)
    indicators['stock_id']=data['stock_id']
    indicators['k'] = tmp['kdj_k']
    indicators['d'] = tmp['kdj_d']

    indicators['ma5']=data['close'].rolling(MA[0]).mean()
    indicators['ma10']=data['close'].rolling(MA[1]).mean()
    indicators['ma20']=data['close'].rolling(MA[2]).mean()
    indicators['ma30']=data['close'].rolling(MA[3]).mean()
    indicators['ma60']=data['close'].rolling(MA[4]).mean()

    indicators=pd.DataFrame(indicators)
    return indicators

def lm_kdj(df, n,ksgn='close'):    
    lowList = df['low'].rolling(n).min()
    lowList.fillna(value=df['low'].expanding().min(), inplace=True)
    highList = df['high'].rolling(n).max()
    highList.fillna(value=df['high'].expanding().max(), inplace=True)
    rsv = (df[ksgn] - lowList) / (highList - lowList) * 100

    df['kdj_k'] = rsv.ewm(com=2).mean()
    df['kdj_d'] = df['kdj_k'].ewm(com=2).mean()
    df['kdj_j'] = 3.0 * df['kdj_k'] - 2.0 * df['kdj_d']
    return df   

## program start here
if len(sys.argv) > 1:
    datestr = sys.argv[1]
else:
    datestr = '20180801'

if datetime.strptime(datestr,'%Y%m%d').weekday() > 4 :
    print("not work day")
    sys.exit()

my_date = datetime.strptime(datestr,'%Y%m%d')
my_date =  my_date + dateutil.relativedelta.relativedelta(months=-3)
datestr_format = datetime.strptime(datestr,'%Y%m%d').strftime('%Y-%m-%d')
base_datestr_format = my_date.strftime('%Y-%m-%d')

MA=[5,10,20,30,60,120]     
stock_id = '2882'
conn = sqlite3.connect("twstock.db")
df = pd.read_sql_query("""
select stock_id, deal_date, open, high, low, close from daily_price where deal_date >= ? order by stock_id, deal_date;
""", conn, params=[base_datestr_format])
df = df.set_index(['deal_date'])
df = df.groupby('stock_id')

cnx = sqlite3.connect('twstock.db')

for stock in df.groups:
    temp = df.get_group(stock).copy()
    temp.replace({'--': None}, inplace=True)

    try:
        kd = Get_kd_ma(temp)
        kd['deal_date'] = datestr_format
        #print(kd.loc[[datestr_format]])
        kd.loc[[datestr_format]].to_sql("daily_ta",con = cnx,index=False,if_exists='append',index_label=False)
    except:
        print(temp)
    finally:
        pass
    

