#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     syb_app_dau_superluo.py
# 功能描述:     手游宝数据给到运营部
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_syb_app_dau
# 数据源表:     ieg_mg_oss_app::iplat_dm_syb_app_round_tbUserActivity
# 创建人名:     llianli
# 创建日期:     2016-05-16
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20141201'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

##    sql = """set hive.inputfiles.splitbylinenum=true"""
##    res = tdw.execute(sql)
##    sql = """set hive.inputfiles.line_num_per_split=1000000"""
##    res = tdw.execute(sql)


    sql = """
                 CREATE TABLE IF NOT EXISTS tb_syb_app_dau
             (
             dtstatdate INT COMMENT '统计日期',
             dau BIGINT COMMENT '日活跃'
             ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql=""" delete from tb_syb_app_dau where dtstatdate=%s """ % (sDate)
    tdw.WriteLog(sql)
    
    res = tdw.execute(sql)


    sql = """
            INSERT TABLE tb_syb_app_dau 
         SELECT 
         %s AS dtstatdate,
         iactivitynum 
         FROM ieg_mg_oss_app::iplat_dm_syb_app_round_tbUserActivity WHERE dtstatdate = %s 
         AND  iperiod = 1 
         AND splattype = '-100' 
         AND sAccountType  = '-100' 
         AND sgamecode = '-100' 
         AND splat = '-100'
                    """ % (sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    