#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on 2014-12-24

@author: jakegong
'''

#import system module

time = __import__('time')
datetime = __import__('datetime')
string = __import__('string')

# main entry
def TDW_PL(tdw, argv=[]):
    
    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")
    tdw.WriteLog("== argv[1] = " + argv[1] + " ==")
    sDate = argv[0];
    period_days = argv[1]

    ##对日期做统一处理
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")    
    
    pre_date = today_date - datetime.timedelta(days = 2*int(period_days))
    pre_date_str = pre_date.strftime("%Y%m%d") 
    
    
    tdw.WriteLog("pre_date_str=%s"%pre_date_str)      

    tdw.WriteLog("== sDate = " + sDate + " ==")
    tdw.WriteLog("== pre_date_str = " + pre_date_str + " ==")

    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qqtalk_mtgp_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)

    ##----创建结果表
    sql = """
            CREATE TABLE IF NOT EXISTS tb_gpcd_period_user_stay
            (
            dtstatdate INT COMMENT '统计日期',
            business STRING COMMENT '业务类型',
            period int COMMENT '周期天数',
            active_num BIGINT COMMENT '上个周期活跃用户数',
            stay_num BIGINT COMMENT '当前周期留存用户数'
            )"""
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)

    sql = "delete from tb_gpcd_period_user_stay where period=%s and dtstatdate=%s"%(period_days,sDate)
    tdw.WriteLog("sql=%s"%sql)
    res = tdw.execute(sql)


    ##--计算周期留存，这里以7天为例子计算 
    period = int(period_days)
    zero_bits = ''   
    for i in range(period):
        zero_bits += '0'
 
    sql="""
        insert table tb_gpcd_period_user_stay
        select %s, a.business, %d, a.active_num, b.stay_num
        from
        (
        select  business, 
        count(distinct iuin) as active_num
        from  tb_gpcd_reg_account  PARTITION (p_%s) a1
        WHERE  dtstatdate = %s  and ilastactdate >= %s and substr(cbitmap, %d, %d) != "%s"
        group by business         
        ) a
        full outer join
        (
        select business, 
        count(distinct iuin) as stay_num
        from  tb_gpcd_reg_account  PARTITION (p_%s) b1
        WHERE  dtstatdate = %s and ilastactdate >= %s  and substr(cbitmap, %d, %d) != "%s" and substr(cbitmap, 1, %d) != "%s" 
        group by business 
        ) b
        on (a.business = b.business)
        """ %(sDate,period,sDate,sDate,pre_date_str,period + 1,period,zero_bits,sDate,sDate,pre_date_str,period + 1,period,zero_bits,period,zero_bits)
                    
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("==end OK ==")
    
    