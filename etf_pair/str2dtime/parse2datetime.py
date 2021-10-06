#!/usr/bin/env python
# coding: utf-8

"""
Offset的str变换为datetime.timedelta
"""
import datetime,re

def split_Offsets_str(Offsets):
    """
    将字符串类型递延时间
    数字部分和类型部分划分
    仅接受min和second
    
    Parameters:
    -----
    Offsets: str

    Returns:
    -----
    int,str
    
    """
    assert type(Offsets) is str,"Offsets must be str"
    re_second = re.compile("([0-9]*)s|([0-9]*)second")
    re_min = re.compile("([0-9]*)min")
    if re_second.search(Offsets):
        return int(re_second.search(Offsets).group(1)),"sec"
    elif re_min.search(Offsets):
        return int(re_min.search(Offsets).group(1)),"min"

class parse2time:
    def __init__(self):
        pass

    def str2delta(self,Offsets):
        """
        递延时间字符串变换成timedelta

        Parameters:
        -----
        str

        Returns:
        -----
        datetime.timedelta

        """
        num,ty = split_Offsets_str(Offsets)
        if ty == "sec":
            return datetime.timedelta(seconds = num)
        elif ty == "min":
            return datetime.timedelta(minutes = num)

    def delta2str(self,Offsets):
        """
        将datetime.timedelta变化为字符串
        
        Parameters:
        -----
        datetime.timedelta

        Returns:
        -----
        str
        
        """
        assert type(Offsets) is datetime.timedelta,"Offsets must be timedelta"
        base_timedelta = datetime.timedelta(seconds = 1)
        base_num = int(Offsets/base_timedelta)
        if base_num < 60:
            return str(base_num)+'s'
        elif (base_num < 3600) & (int(base_num/60) == base_num/60):
            base_num = int(Offsets/datetime.timedelta(minutes = 1))
            return str(base_num)+"min"
        else:
            raise Exception("only except integer min and integer seconds")


