#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ieg_oss_lib
# 功能描述:     IEG-DC-OSS TDW PY库函数
# 输入参数:     
# 目标表名:
# 数据源表:
# 创建人名:     tumizhang
# 创建日期:     2013-11-25
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************

# int    to_number(string str)
# string to_string(int num)
# date   to_date(string sDate)
# string from_date(date dDate, int type = 0)
# int    to_timestamp(date dDate)
# date   from_timestamp(int timestamp)
# date   add_days(date dDate, int n)
# int    is_month_first(date dDate)
# int    is_leapyear(int iDate)
# date   add_months(date dDate, int n)
# string substr(string str, int start_pos, int leng = 65535)
# date   add_months2(date dDate,int months)


#import system module
time = __import__('time')
datetime = __import__('datetime')
calendar = __import__('calendar')
string = __import__('string')
re = __import__('re')


def to_number(str):
    return int(str)


def to_string(num):
    return """%s""" % (num)


def to_date(sDate):
    if len(sDate) == 8: #yyyymmdd
        return datetime.datetime(string.atoi(sDate[0:4]), string.atoi(sDate[4:6]), string.atoi(sDate[6:8]))
    elif len(sDate) == 19 : #yyyy-mm-dd hh:ss:mi
        return datetime.datetime(string.atoi(sDate[0:4]), string.atoi(sDate[5:7]), string.atoi(sDate[8:10]), string.atoi(sDate[11:13]), string.atoi(sDate[14:16]), string.atoi(sDate[17:19]))
    else :
        return None


def from_date(dDate, type = 0):
    if type == 0 :
        return dDate.strftime("%Y-%m-%d %H:%M:%S")
    else :
        return dDate.strftime("%Y%m%d")
 

def to_timestamp(dDate):
    if dDate is None :
        return 0
    else :
        return int(time.mktime(dDate.timetuple()))


def from_timestamp(timestamp): 
    return datetime.datetime.fromtimestamp(timestamp)
   

def add_days(dDate, n):
    
    d = to_timestamp(dDate)
    if d == 0:
        return None
    
    d2 = d + 24 * 60 * 60 * n   
    return from_timestamp(d2)


def is_month_first(dDate):
    sDate = from_date(dDate)
    sDate2 = """%s01 00:00:00""" % (sDate[0:8])
    
    return to_date(sDate2)


def is_leapyear(iDate):
    cDate = to_string(iDate)
    iYear = string.atoi(cDate[0:4])
    
    if (iYear % 40 == 0 or (iYear % 4 == 0 and iYear % 100 != 0)) :
        return 1
    else :
        return 0


def add_months(dDate, n):
    sDate = from_date(dDate)
    iYear = to_number(string.atoi(sDate[0:4]))
    iMon  = to_number(string.atoi(sDate[5:7]))
    iDay  = to_number(string.atoi(sDate[8:10]))
    
    m = n % 12
    y = ((iMon + m) / 13) + n / 12
    
    iYear = iYear + y
    iMon  = (iMon + m) % 12
    if iMon == 0:
        iMon = 12
       
    if iDay == 31 :
        if iMon == 2 or iMon == 4 or iMon == 6 or iMon == 9 or iMon == 11:
            iDay = 30
    
    if iDay >= 29 :
        if iMon == 2 :
            if is_leapyear(iYear) == 0 :
                iDay = 28
            else :
                iDay = 29

    sDate2 = """%04d-%02d-%02d %s""" % (iYear, iMon, iDay, sDate[11:19])
    return to_date(sDate2)     


def substr(str, start_pos, leng = 65535):
    if str is None : return None
    
    if start_pos > 0 : start_pos -= 1
    if start_pos < 0 : start_pos += len(str)
    
    if leng <= 0         : return None
    elif leng > len(str) : return str[start_pos:]
    else :
        if leng < 65535 :
            end_pos = start_pos + leng
            return str[start_pos:end_pos]
        else :
            return str[start_pos:]


def get_ext_value(str, tag):
    if str is None or tag is None : return None
    
    tag1 = tag + "[$]"
    tag2 = "[#$%]"
    
    pos1 = string.find(str, tag1, 0)
    pos2 = string.find(str, tag2, pos1)
    
    start_pos = pos1 + len(tag1)
    end_pos = pos2

    return str[start_pos:end_pos]

def add_months2(dDate,months):
    month = dDate.month - 1 + months
    year = dDate.year + month / 12
    month = month % 12 + 1
    day = min(dDate.day,calendar.monthrange(year,month)[1])
    return dDate.replace(year=year, month=month, day=day)




# main entry

#def TDW_PL(tdw, argv=[]):
    #print "hello TDW"
    #print to_number("20120102")
    #print to_string(20120102)
    #print to_date("20120102")
    #print to_date("2012-01-02 01:02:03")
    #print from_date(to_date("2012-01-02 01:02:03"))
    #print from_date(to_date("2012-01-02 01:02:03"), 1)
    #print to_timestamp(to_date("2012-01-02 01:02:03"))
    #print from_timestamp(1325437323)
    #print add_days(to_date("2012-01-02 01:02:03"), 1)
    #print add_days(to_date("20120102"), 1)
    #print add_months(to_date("2012-01-02 01:02:03"), 1)
    #print add_months(to_date("20120102"), 2)
    #print is_month_first(to_date("2012-01-02 01:02:03"))
    #print is_month_first(to_date("20120102"))
    #print get_ext_value("101[$]15995501427[#$%]102[$]1000[#$%]103[$]中国移动[#$%]104[$]江苏[#$%]106[$]1_9_3_94_1[#$%]403[$]1[#$%]901[$]1004[#$%]", '101')
    
