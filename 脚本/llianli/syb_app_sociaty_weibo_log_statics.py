#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_sociaty_weibo_log_data.py
# 功能描述:     手游宝直播免费礼物数据
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank :: gqq_dsl_day_task_bill_fht0 
# 创建人名:     llianli
# 创建日期:     2015-11-03
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
    
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，公会新鲜事数据统计
    sql = '''
               CREATE TABLE IF NOT EXISTS tb_syb_app_ttxd_sociaty_weibo_event_data
    (
    dtstatdate INT COMMENT '统计日期',
    oper_type INT COMMENT '-100:全部，1：发表，2：转发，3：点赞，4：回复',
    platid INT COMMENT '-100:全部，0：安卓，1：ios',
    uv BIGINT COMMENT '人数',
    pv BIGINT COMMENT '次数'
    )
         '''
            
    res = tdw.execute(sql)


    sql=''' DELETE FROM  tb_syb_app_ttxd_sociaty_weibo_event_data WHERE dtstatdate = %s''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   
 
    
    ##公会新鲜事数据写入
    sql = ''' 
    INSERT TABLE tb_syb_app_ttxd_sociaty_weibo_event_data
        SELECT 
        %s AS dtstatdate,
        CASE WHEN GROUPING(oper_type) = 1 THEN -100 ELSE oper_type END AS oper_type,
        CASE WHEN GROUPING(platid) = 1 THEN -100 ELSE platid END AS platid,
        COUNT(DISTINCT openid) AS uv,
        COUNT(*) AS pv
        FROM
        (
        SELECT
        oper_type,
        openid AS openid,
        platid
        FROM ieg_tdbank::qtalk_dsl_guildexcitsvr_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
        )t
        GROUP BY cube(oper_type,platid)
    '''%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
   

    tdw.WriteLog("== end OK ==")
    
    
    
    