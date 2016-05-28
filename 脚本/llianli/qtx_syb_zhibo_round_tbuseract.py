#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zhibo_round_tbuseract.py
# 功能描述:     手游宝直播数据统计-所有直播账户入库
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
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，直播所有用户数据
    sql = '''
            CREATE TABLE IF NOT EXISTS iplat_fat_syb_app_zhibo_round_tbuseract_v2
     (
     dtstatdate BIGINT,
     slogintype STRING,
     iclienttype BIGINT,
     isybid BIGINT
     )PARTITION BY LIST (dtstatdate)
            (
            partition p_20151016  VALUES IN (20151016),
            partition p_20151017  VALUES IN (20151017)
            ) '''
            
    res = tdw.execute(sql)


    sql=''' alter table  iplat_fat_syb_app_zhibo_round_tbuseract_v2 DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' alter table iplat_fat_syb_app_zhibo_round_tbuseract_v2 ADD PARTITION p_%s VALUES IN (%s) '''%(today_str,today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##从原始日志中写入直播的用户数据
    sql = ''' 
    INSERT TABLE iplat_fat_syb_app_zhibo_round_tbuseract_v2
     SELECT
     %s AS dtstatdate,
     CASE WHEN GROUPING(slogintype) = 1 THEN '-100' ELSE slogintype END AS slogintype,
     CASE WHEN GROUPING(client_type) = 1 THEN -100 ELSE client_type END AS client_type,
     CAST(userid AS BIGINT ) AS isybid 
     FROM 
     (
     SELECT 
     userid,
     CASE 
         WHEN CAST(userid AS BIGINT) <= 4294967295  THEN 'qq'
         ELSE 'wx'
         END 
     AS slogintype,
     client_type,
     enter_time,
     dteventtime 
     FROM ieg_tdbank::qtalk_dsl_videoroomevent_fht0 
     WHERE  tdbank_imp_date >= %s00 
     AND tdbank_imp_date <= %s23 
     AND  room_mode = 19 
     AND event_type = 1
     )
     GROUP BY userid,cube(slogintype,client_type)
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    tdw.WriteLog("== end OK ==")
    
    
    
    