#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zhibo_time_range.py
# 功能描述:     手游宝直播用户直播时长数据统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-10-20
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
    
    nxt_date = today_date + datetime.timedelta(days = 1)
    nxt_date_str = nxt_date.strftime("%Y%m%d")
    
    pre_30_date = today_date - datetime.timedelta(days = 30)
    pre_30_date_str = pre_30_date.strftime("%Y%m%d")
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，直播留存率表
    sql = '''
           CREATE TABLE IF NOT EXISTS qtx_syb_zhibo_time_range
        (
        dtstatdate BIGINT ,
        slogintype STRING,
        iclienttype INT,
        total_uin BIGINT,
        ionlinetime BIGINT,
        time_0_uin BIGINT,
        time_0_5_uin BIGINT,
        time_5_10_uin BIGINT,
        time_10_30_uin BIGINT,
        time_30_uin BIGINT
        )
         '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM  qtx_syb_zhibo_time_range WHERE dtstatdate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##统计人均时长和时长分布情况
    sql = ''' 
    INSERT TABLE qtx_syb_zhibo_time_range
        SELECT
        %s AS dtstatdate,
        CASE WHEN GROUPING(slogintype) = 1 THEN '-100' ELSE slogintype END AS slogintype,
        CASE WHEN GROUPING(iclienttype) = 1 THEN -100 ELSE iclienttype END AS iclienttype,
        COUNT(DISTINCT isybid) AS total_uin,
        SUM(ionlinetime) AS ionlinetime,
        SUM(time_0) AS time_0_uin,
        SUM(time_0_5) AS time_0_5_uin,
        SUM(time_5_10) AS time_5_10_uin,
        SUM(time_10_30) AS time_10_30_uin,
        SUM(time_30) AS time_30_uin
        FROM 
        (
        SELECT
        slogintype,
        iclienttype,
        isybid,
        ionlinetime,
        CASE WHEN ionlinetime = 0 THEN 1 ELSE 0 END AS time_0,
        CASE WHEN ionlinetime > 0 AND ionlinetime <= 300 THEN 1 ELSE 0 END AS time_0_5,
        CASE WHEN ionlinetime > 300 AND ionlinetime <= 600 THEN 1 ELSE 0 END AS time_5_10,
        CASE WHEN ionlinetime > 600 AND ionlinetime <= 1800 THEN 1 ELSE 0 END AS time_10_30,
        CASE WHEN ionlinetime > 1800 THEN 1 ELSE 0 END AS time_30
        FROM 
        (
        SELECT
        slogintype,
        iclienttype,
        isybid,
        SUM(ionlinetime) as ionlinetime
        FROM 
        (
        SELECT
        slogintype,
        iclienttype,
        isybid,
        CASE 
        WHEN  enter_timestamp <= unix_timestamp('%s 00:00:00') AND leave_timestamp <= unix_timestamp('%s 23:59:59')  THEN leave_timestamp - unix_timestamp('%s 00:00:00')
        WHEN  enter_timestamp <= unix_timestamp('%s 00:00:00') AND leave_timestamp > unix_timestamp('%s 23:59:59')  THEN unix_timestamp('%s 23:59:59') - unix_timestamp('%s 00:00:00')
        WHEN  enter_timestamp > unix_timestamp('%s 00:00:00') AND leave_timestamp <= unix_timestamp('%s 23:59:59')  THEN leave_timestamp - enter_timestamp
        WHEN  enter_timestamp > unix_timestamp('%s 00:00:00') AND leave_timestamp > unix_timestamp('%s 23:59:59')  THEN  unix_timestamp('%s 23:59:59') - enter_timestamp
        ELSE 0
        END AS ionlinetime 
        FROM
        (
        
        
        SELECT 
     CAST(userid AS BIGINT) AS isybid,
     CASE 
         WHEN CAST(userid AS BIGINT) <= 4294967295  THEN 'qq'
         ELSE 'wx'
         END 
     AS slogintype,
     client_type AS iclienttype,
     unix_timestamp(enter_time) AS enter_timestamp,
     unix_timestamp(dteventtime) AS leave_timestamp 
     FROM ieg_tdbank::qtalk_dsl_videoroomevent_fht0 
     WHERE  tdbank_imp_date >= %s00 
     AND tdbank_imp_date <= %s06
     AND  room_mode = 19  AND event_type = 2
     AND unix_timestamp(enter_time) <= unix_timestamp('%s 23:59:59')
     AND unix_timestamp(dteventtime) >= unix_timestamp('%s 00:00:00')

        )t1
        )t2
        GROUP BY slogintype,
        iclienttype,
        isybid
        )t3
        )t4 GROUP BY cube(slogintype,   iclienttype)
    '''%(sDate,today_str_2,today_str_2,today_str_2,today_str_2,today_str_2,today_str_2,today_str_2,today_str_2,today_str_2,today_str_2,today_str_2,today_str_2,sDate,nxt_date_str,today_str_2,today_str_2)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    