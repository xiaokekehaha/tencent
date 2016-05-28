#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zhibo_tbstayscale.py
# 功能描述:     手游宝直播用户留存率数据统计
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
            CREATE TABLE  IF NOT EXISTS iplat_fat_syb_app_zhibo_tbstayscale_v2
        (
         dtstatedate INT,
         dtactday INT,
         daydelta INT,
         sflag STRING,
         slogintype STRING,
         iclienttype INT,
         istayuin BIGINT
        ) 
         '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM  iplat_fat_syb_app_zhibo_tbstayscale_v2 WHERE dtstatedate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##统计注册用户的留存率数据
    sql = ''' 
    INSERT TABLE iplat_fat_syb_app_zhibo_tbstayscale_v2
      
      SELECT 
      %s as dtstatdate,
      t.iRegDate as iRegDate,
      datediff(%s,t.iRegDate) as daydelta,
      'reg',
      t.slogintype,
      t.iclienttype,
      COUNT(DISTINCT t.isybid) 
      FROM 
      (
      SELECT 
      slogintype,
      iclienttype,
      isybid ,
      iRegDate
      FROM iplat_fat_syb_app_zhibo_round_tbAccount_v2 WHERE  dtstatdate = %s AND iRegDate >= %s AND iRegDate < %s 
      )t 
      JOIN
      (
      SELECT 
      slogintype,
      iclienttype,
      isybid 
      FROM iplat_fat_syb_app_zhibo_round_tbAccount_v2 WHERE  dtstatdate = %s AND  iLastActDate = %s 
      )t1 
      on (t.slogintype = t1.slogintype AND t.iclienttype = t1.iclienttype  AND t.isybid = t1.isybid  ) 
      GROUP BY t.iRegDate,t.slogintype,t.iclienttype
    '''%(sDate,sDate,sDate,pre_30_date_str,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##统计活跃用户的留存率数据
    sql = ''' 
     INSERT TABLE iplat_fat_syb_app_zhibo_tbstayscale_v2
        SELECT 
        %s as dtstatdate,
        t.actday as actday,
        datediff(%s,t.actday) as daydelta,
        'act',
        t.slogintype,
        t.iclienttype,
        COUNT(DISTINCT t.isybid) 
        FROM 
        (
        SELECT 
        slogintype,
        iclienttype,
        isybid ,
        dtstatdate as actday
        FROM iplat_fat_syb_app_zhibo_round_tbUserAct_v2 WHERE dtstatdate >= %s AND dtstatdate < %s
        )t 
        JOIN
        (
        SELECT 
        slogintype,
        iclienttype,
        isybid 
        FROM iplat_fat_syb_app_zhibo_round_tbAccount_v2 WHERE  dtstatdate = %s AND  iLastActDate = %s 
        )t1 
        on (t.slogintype = t1.slogintype AND t.iclienttype = t1.iclienttype AND t.isybid = t1.isybid  ) 
        GROUP BY t.actday,t.slogintype,t.iclienttype
    '''%(sDate,sDate,pre_30_date_str,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    tdw.WriteLog("== end OK ==")
    
    
    
    