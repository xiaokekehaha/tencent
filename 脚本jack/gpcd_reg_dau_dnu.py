#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on 2016-05-04

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

    sDate = argv[0];

    ##对日期做统一处理
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")

    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qqtalk_mtgp_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)

    ##----创建注册表
    sql = """
            CREATE TABLE IF NOT EXISTS tb_gpcd_reg_dnu_dau_result
            (
            dtstatdate INT COMMENT '统计日期',
            business STRING COMMENT '业务类型',
            reg BIGINT COMMENT '历史注册数',
            dnu BIGINT COMMENT '当日新进数',
            dau BIGINT COMMENT '当日活跃数'
            )"""
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)
    
    sql = "delete from tb_gpcd_reg_dnu_dau_result where dtstatdate=%s"%(sDate)
    res = tdw.execute(sql)
    
    sql = """
        insert table  tb_gpcd_reg_dnu_dau_result
        select %s, t1.business, 
        IF(t1.reg IS NOT NULL ,t1.reg, 0),
        IF(t2.dnu IS NOT NULL ,t2.dnu, 0),
        IF(t3.dau IS NOT NULL ,t3.dau, 0)
        from 
        (
        select business, count(distinct iuin) as reg from  tb_gpcd_reg_account  PARTITION (p_%s) t1 WHERE  dtstatdate = %s group by business
        ) t1
        full outer join
        (
        select  business, count(distinct iuin) as dnu from  tb_gpcd_reg_account  PARTITION (p_%s) t1 WHERE  dtstatdate = %s and iregdate = %s group by business
        ) t2
        on (t1.business = t2.business)
        full outer join 
        (
        select  business, count(distinct iuin) as dau from  tb_gpcd_reg_account  PARTITION (p_%s) t1 WHERE  dtstatdate = %s and ilastactdate = %s group by business
        ) t3
        on (t1.business = t3.business)
        """%(sDate, sDate, sDate,sDate,sDate, sDate, sDate,sDate,sDate)
        
    tdw.WriteLog("sql=%s"%sql)
    res = tdw.execute(sql)
    print res

    tdw.WriteLog("==end OK ==")
    
    