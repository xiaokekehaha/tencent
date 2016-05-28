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
    tdw.WriteLog("== argv[1] = " + argv[1] + " ==")
    sDate = argv[0];
    days_ago = argv[1]

    ##对日期做统一处理
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")    
    
    pre_date = today_date - datetime.timedelta(days = int(days_ago))
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
            CREATE TABLE IF NOT EXISTS tb_gpcd_user_stay
            (
            dtstatdate INT COMMENT '统计日期',
            business STRING COMMENT '业务类型',
            type    int    COMMENT '留存类型：0 活跃用户留存，1 新进用户留存',
            days_ago int COMMENT '历史注册数',
            stay_num BIGINT COMMENT '当日留存用户数'
            )"""
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)

    sql = "delete from tb_gpcd_user_stay where type=1 and dtstatdate=%s"%(sDate)
    res = tdw.execute(sql)

    ##--计算X日留存，这里以14天为例子计算
    ##--1、计算新进用户的留存情况
    ##--这里以20150807为例计算
    
    ##--计算前面14天的新进用户在今天的留存情况
    sql="""
        insert table tb_gpcd_user_stay
        select %s, t1.business, 1, t1.delt, t1.stay_num
        from
        (
            select iregdate, business, delt, 
            count(distinct iuin) as stay_num
            from
            (
                  select distinct iuin, datediff(ilastactdate, iregdate) as delt, business, iregdate
                  from  tb_gpcd_reg_account  PARTITION (p_%s) a1
                  where iregdate >= %s and iregdate < %s and ilastactdate = %s
            ) t
            group by iregdate, business, delt
        ) t1
        """ % (sDate,sDate,pre_date_str,sDate,sDate)
    tdw.WriteLog("%s"%sql)
    res = tdw.execute(sql)  

    tdw.WriteLog("==end OK ==")
    
    