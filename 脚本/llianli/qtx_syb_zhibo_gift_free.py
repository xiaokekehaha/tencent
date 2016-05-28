#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_zhibo_gift_free.py
# 功能描述:     手游宝直播免费礼物数据
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-10-21
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


     ##创建表，直播免费礼物表
    sql = '''
            CREATE TABLE IF NOT EXISTS tb_syb_zhibo_free_gift
   (
   dtstatdate INT,
   imainroomid INT,
   slogintype STRING,
   iclienttype BIGINT,
   igiftid INT,
   igifttimes BIGINT,
   igiftnum BIGINT,
   igiftuin BIGINT
   )
         '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM  tb_syb_zhibo_free_gift WHERE dtstatdate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##每个直播免费礼物数据
    sql = ''' 
    INSERT TABLE tb_syb_zhibo_free_gift
   SELECT
   %s AS dtstatdate,
   imainroomid,
   CASE WHEN GROUPING(slogintype) = 1 THEN '-100' ELSE slogintype END AS slogintype,
   CASE WHEN GROUPING(iclienttype) = 1 THEN -100 ELSE iclienttype END AS iclienttype,
   igiftid,
   COUNT(*) AS igifttimes,
   SUM(igiftnum) AS igiftnum,
   COUNT(DISTINCT isybid) AS igiftuin
   FROM 
   (
   SELECT 
   isybid,
   -100 AS imainroomid,
   CASE 
         WHEN isybid <= 4294967295  THEN 'qq'
         ELSE 'wx'
         END 
     AS slogintype,
     CASE 
         WHEN  vostype ='3' THEN 301 
         WHEN  vostype ='5' THEN 302 
         ELSE 0 
     END AS iclienttype,
     
     CAST(vv8 AS BIGINT) AS igiftid,
   CAST(vv10 AS BIGINT) AS igiftnum
   FROM ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
   where tdbank_imp_date BETWEEN '%s00' AND '%s23' 
   AND   ireportid = 800 AND iactiontype = 17 AND iactionid = 42
   
   UNION ALL 
   
   SELECT 
   isybid,
   CAST(vv9 AS BIGINT) AS imainroomid,
   CASE 
         WHEN isybid <= 4294967295  THEN 'qq'
         ELSE 'wx'
         END 
     AS slogintype,
     CASE 
         WHEN  vostype ='3' THEN 301 
         WHEN  vostype ='5' THEN 302 
         ELSE 0 
     END AS iclienttype,
     
     CAST(vv8 AS BIGINT) AS igiftid,
   CAST(vv10 AS BIGINT) AS igiftnum
  FROM ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
   where tdbank_imp_date BETWEEN '%s00' AND '%s23' 
   AND   ireportid = 800 AND iactiontype = 17 AND iactionid = 42
   )t
   GROUP BY igiftid, imainroomid,cube(slogintype,iclienttype)
    '''%(sDate,sDate,sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
   
    

    tdw.WriteLog("== end OK ==")
    
    
    
    