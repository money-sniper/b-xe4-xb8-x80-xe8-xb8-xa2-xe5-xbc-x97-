#!/usr/bin/env python
# coding: utf-8

"""
所有的文件转化文件
"""
import pandas as pd,numpy as np

#第一个参数文件路径 第二个参数： 0: ss5; 1: ss10 2：order 3:trade 第三个参数count 读入元素个数 -1 表示读入整个文件
def convertdata(filepath, type1=0, count=-1):
    #参数
    #type = 1
    binfile = open(filepath, 'rb')

    mySS5type = np.dtype({
        'names': ['TradingDay', 'InstrumentID', 'ExchangeID', 'ExchangeInstID', 'LastPrice', 'PreSettlementPrice', 'PreClosePrice', 'PreOpenInterest', 'OpenPrice', 'HighestPrice', 'LowestPrice', 'Volume','Turnover', 'OpenInterest',
                  'ClosePrice', 'SettlementPrice', 'UpperLimitPrice', 'LowerLimitPrice', 'PreDelta', 'CurrDelta', 'UpdateTime', 'UpdateMillisec', 'BidPrice1', 'BidVolume1', 'AskPrice1', 'AskVolume1',
                 'BidPrice2', 'BidVolume2', 'AskPrice2', 'AskVolume2', 'BidPrice3', 'BidVolume3', 'AskPrice3', 'AskVolume3', 'BidPrice4', 'BidVolume4', 'AskPrice4', 'AskVolume4',
                  'BidPrice5', 'BidVolume5', 'AskPrice5', 'AskVolume5', 'AveragePrice'],
        'formats': ['S9', 'S31', 'S9', 'S31', np.float64, np.float64, np.float64, np.float64, np.float64, np.float64, np.float64, np.uint32, np.float64, np.float64, np.float64, np.float64, np.float64, np.float64, np.float64, np.float64,
                    '=S9', 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i',
                    np.float64]}, align=True)

    mySS10type = np.dtype({
        'names': ['TradingDay', 'InstrumentID', 'ExchangeID', 'ExchangeInstID', 'LastPrice', 'PreSettlementPrice', 'PreClosePrice', 'PreOpenInterest', 'OpenPrice', 'HighestPrice', 'LowestPrice','Volume', 'Turnover', 'OpenInterest',
                  'ClosePrice', 'SettlementPrice', 'UpperLimitPrice', 'LowerLimitPrice', 'PreDelta', 'CurrDelta', 'UpdateTime', 'UpdateMillisec', 'BidPrice1', 'BidVolume1', 'AskPrice1', 'AskVolume1',
                 'BidPrice2', 'BidVolume2', 'AskPrice2', 'AskVolume2', 'BidPrice3', 'BidVolume3', 'AskPrice3', 'AskVolume3', 'BidPrice4', 'BidVolume4', 'AskPrice4', 'AskVolume4',
                  'BidPrice5', 'BidVolume5', 'AskPrice5', 'AskVolume5', 'AveragePrice', 'InstrumentName', 'IOPV', 'YieldToMaturity', 'AuctionPrice', 'TradingPhase', 'OpenRestriction',
                  'TradeCount', 'TotalBidVolume', 'WeightedAvgBidPrice', 'AltWeightedAvgBidPrice', 'TotalAskVolume', 'WeightedAvgAskPrice', 'AltWeightedAvgAskPrice',
                  'BidPriceLevel', 'AskPriceLevel', 'BidCount1', 'BidCount2', 'BidCount3', 'BidCount4', 'BidCount5', 'BidPrice6', 'BidVolume6', 'BidCount6',
                  'BidPrice7', 'BidVolume7', 'BidCount7', 'BidPrice8', 'BidVolume8', 'BidCount8', 'BidPrice9', 'BidVolume9', 'BidCount9', 'BidPriceaA', 'BidVolumeA', 'BidCountA',
                  'AskCount1', 'AskCount2',  'AskCount3', 'AskCount4', 'AskCount5', 'AskPrice6', 'AskVolume6', 'AskCount6',
                  'AskPrice7', 'AskVolume7', 'AskCount7','AskPrice8', 'AskVolume8', 'AskCount8','AskPrice9', 'AskVolume9', 'AskCount9','AskPriceA', 'AskVolumeA', 'AskCountA', 'SourceTag'],
        'formats': ['S9', 'S31', 'S9', 'S31', np.float64, np.float64, np.float64, np.float64, np.float64, np.float64, np.float64, np.uint32, np.float64, np.float64, np.float64, np.float64, np.float64, np.float64, np.float64, np.float64,
                    'S9', 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i', np.float64, 'i',
                    np.float64, '21S', np.float64, np.float64, np.float64, '1S', '1S', 'i', 'i', np.float64, np.float64, 'i', np.float64, np.float64, 'i', 'i', 'i', 'i', 'i', 'i', 'i',
                    np.float64, 'i', 'i', np.float64, 'i', 'i', np.float64, 'i', 'i', np.float64, 'i', 'i', np.float64, 'i', 'i', 'i', 'i', 'i', 'i', 'i',
                    np.float64, 'i', 'i', np.float64, 'i', 'i', np.float64, 'i', 'i', np.float64, 'i', 'i', np.float64, 'i', 'i', 'S31']}, align=True)
    myTradetype = np.dtype({
        'names': ['ChannelNum', 'TradeIndex', 'BuyIndex', 'SellIndex', 'UpdateTime', 'ExchangeID', 'InstrumentID', 'Millisec', 'LocalTime', 'TradingDay', 'Price', 'Volume', 'ExecType', 'FunctionCode'],
        'formats': ['i', 'i', 'i', 'i', 'S9', 'S9', 'S16', np.uint16, np.uint32, 'S9', np.float64, np.float64, 'S2', 'S2']})
    myOrdertype = np.dtype({
        'names': ['ChannelNum', 'OrderIndex', 'UpdateTime', 'ExchangeID', 'InstrumentID', 'OrderMillisec', 'LocalTickTime', 'TradingDay', 'Price', 'Volume', 'ExecType', 'FunctionCode'],
        'formats': ['i', 'i', 'S9', 'S9', 'S16', np.uint16, np.uint32, 'S9', np.float64, np.float64, 'S2', 'S2']})

    id = type1
    mytype = mySS10type
    if id == 0:
        mytype = mySS5type
    elif id == 1:
        mytype = mySS10type
    elif id == 2:
        mytype = myOrdertype
    else:
        mytype = myTradetype

    c = np.fromfile(binfile, mytype, count, '')# count 读入元素个数 -1 表示读入整个文件 sep 数据分割字符串 如何是空串，文件为二进制
    binfile.close()

    df = pd.DataFrame(c)
    return df

def convertdata_staticInfo(path):
    fileType = np.dtype({'names': ['ExchangeID','InstrumentID','UpperLimitPrice','LowerLimitPrice','PreSettlementPrice','PreClosePrice','PreIOPV','IsNotTrade'],
        'formats':['S9','S31',np.float64,np.float64,np.float64,np.float64,np.float64,'i'] 
        },align=True)
    binfile = open(path,'rb')
    c = np.fromfile(binfile,fileType,-1,'')
    df = pd.DataFrame(c)
    return df