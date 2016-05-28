#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zhibo_tb_useracti.py
# 功能描述:     手游宝直播活跃新增情况统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-10-17
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime

def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")
    today_str_2 = today_date.strftime("%Y-%m-%d")
    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，直播的用户数据
    sql = '''
           CREATE TABLE  IF NOT EXISTS iplat_fat_syb_app_zhibo_tbuseracti_v2
        (
         dtstatedate INT,
         slogintype STRING,
         iclienttype INT,
         reg_uin BIGINT ,
         act_uin BIGINT
        ) '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM  iplat_fat_syb_app_zhibo_tbuseracti_v2  WHERE dtstatedate=%s''' % (today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##计算直播用户活跃新增数据
    sql = ''' 
    INSERT TABLE iplat_fat_syb_app_zhibo_tbuseracti_v2
       SELECT 
       %s AS dtstatdate,
         slogintype,
         iclienttype,
         COUNT(DISTINCT reg_uin) as reg_uin,
         COUNT(DISTINCT act_uin) as act_uin
         FROM 
         (
         SELECT 
         slogintype,
         iclienttype,
         CASE WHEN iRegDate = %s THEN isybid ELSE NULL END AS reg_uin,
         CASE WHEN iLastActDate = %s THEN isybid ELSE NULL END AS act_uin
         FROM iplat_fat_syb_app_zhibo_round_tbAccount_v2 WHERE dtstatdate=%s 
         
         )t GROUP BY slogintype,iclienttype
    '''%(sDate,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    