#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_ad_click_total.py
# 功能描述:     手游宝广告点击总体数据
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
    
    
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，各广告位点击情况
    sql = '''
   CREATE TABLE IF NOT EXISTS tb_syb_app_ad_click
   (
   dtstatdate INT,
   id int,
   gameid STRING,
   tagid STRING,
   posid STRING,
   pv INT,
   uv   INT
   )
         '''
    res = tdw.execute(sql)


    sql=''' DELETE FROM  tb_syb_app_ad_click WHERE dtstatdate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##每个直播免费礼物数据
    sql = ''' 
        SELECT
    %s as dtstatdate,
    id,
    gameid,
    tagid,
    posid,
    COUNT(*) AS pv,
    COUNT(DISTINCT sybid) AS uv  
    FROM 
    (
    SELECT 
    id,
    CASE WHEN id = 1100679302 THEN get_json_object(kv,'$.uin')  ELSE get_json_object(kv,'$.syb_id') END AS sybid,
    CASE WHEN ei = 'GENERAL_AD_CLICK' THEN get_json_object(kv,'$.game_id')  ELSE get_json_object(kv,'$.gameid') END AS gameid,
    CASE WHEN ei = 'GENERAL_AD_CLICK' THEN get_json_object(kv,'$.ad_type')  ELSE get_json_object(kv,'$.tagId') END AS tagid,
    CASE WHEN ei = 'GENERAL_AD_CLICK' THEN get_json_object(kv,'$.position')  ELSE get_json_object(kv,'$.index') END AS posid 
    FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND cdate = %s AND id in (1200679337,1100679302) 
    AND ei in ('GENERAL_AD_CLICK','MGC_ENTRANCE_CLICKED')
    )t GROUP BY id,gameid,tagid,posid
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    tdw.WriteLog("== end OK ==")
    
    
    
    