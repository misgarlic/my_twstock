import pandas as pd
import sqlite3
import talib
import numpy as np

def Get_kd_ma(data):
    indicators={}
    #indicators['k'],indicators['d']=talib.STOCH(np.array(data['high']),np.array(data['low']),np.array(data['close']),\
    #fastk_period=9,slowk_period=3,slowk_matype=0,slowd_period=3,slowd_matype=0)
    tmp = lm_kdj(data,9)
    indicators['stock_id']=data['stock_id']
    indicators['k'] = tmp['kdj_k']
    indicators['d'] = tmp['kdj_d']

    indicators['ma1']=data['close'].rolling(MA[0]).mean()
    indicators['ma2']=data['close'].rolling(MA[1]).mean()
    indicators['ma3']=data['close'].rolling(MA[2]).mean()
    indicators['ma4']=data['close'].rolling(MA[3]).mean()
    indicators['ma5']=data['close'].rolling(MA[4]).mean()

    indicators['close']=data['close']
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

MA=[5,10,20,30,60,120]     
stock_id = '2882'
conn = sqlite3.connect("twstock.db")
df = pd.read_sql_query("""
select stock_id, deal_date, open, high, low, close from daily_price where deal_date >= ? and stock_id = ?;
""", conn, params=['2018-07-01',stock_id])
df = df.set_index(['deal_date'])
kd = Get_kd_ma(df)

#kd = lm_kdj(df,9)
print(kd.loc[['2018-08-24']])