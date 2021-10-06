import os,sys
import numpy as np,pandas as pd
import datetime
from pdb import set_trace
from .convert_file import convertdata,convertdata_staticInfo

def clean_data(df):
    if type(df) is pd.DataFrame:
        Day = df.iloc[0,df.columns.get_loc("UpdateTime")].strftime("%Y-%m-%d")
        df = df[((df["UpdateTime"] >= pd.to_datetime(Day+" "+"09:30:00"))&\
            (df["UpdateTime"] <= pd.to_datetime(Day+" "+"11:30:00"))) |\
                ((df["UpdateTime"] >= pd.to_datetime(Day+" "+"13:00:00"))&\
                    (df["UpdateTime"] <= pd.to_datetime(Day+" "+"15:00:00")))]
        return df
    elif type(df) is pd.DatetimeIndex:
        Day = df[0].strftime("%Y-%m-%d") 
        df = df[((df<pd.to_datetime(df[0].strftime("%Y-%m-%d") +" " + "11:30:00"))&\
        (df>=pd.to_datetime(df[0].strftime("%Y-%m-%d") +" " + "09:30:00")))|\
            ((df>=pd.to_datetime(df[0].strftime("%Y-%m-%d") +" " + "13:00:00"))&\
                (df<pd.to_datetime(df[0].strftime("%Y-%m-%d") +" " + "15:00:00")))]
        return df

def Pre_Context_by(path,freq,typing=0):
    data500 = convertdata(path,typing)
    data500[["TradingDay","InstrumentID","ExchangeID","ExchangeInstID","UpdateTime"]]=\
        data500[["TradingDay","InstrumentID","ExchangeID","ExchangeInstID","UpdateTime"]].applymap(lambda x:x.decode("utf-8"))
    
    Day = data500.iloc[0,data500.columns.get_loc("TradingDay")]
    data500["UpdateTime"] = pd.to_datetime(Day+" "+data500["UpdateTime"])
    data500 = clean_data(data500)

    #context by
    data500 = data500.groupby("UpdateTime").apply(lambda x:x.iloc[0]).drop(columns=["UpdateTime"])
    data500 = data500.resample(freq).bfill().reset_index()
    data500 = clean_data(data500)
    return data500

def Resample_data(data,freq):
    t1 = data[["UpdateTime","LastPrice","BidPrice1","AskPrice1"]].set_index("UpdateTime").resample(freq).\
                bfill().dropna().reset_index()
    t1 = clean_data(t1).set_index("UpdateTime")
    return t1

