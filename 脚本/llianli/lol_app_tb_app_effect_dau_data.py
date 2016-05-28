#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_dau_tb_app_effect_dau_data.py
# 功能描述:     掌盟掌火有效活跃用户统计
# 输入参数:     yyyymmdd    例如：20160309
# 目标表名:     ieg_qt_community_app.tb_app_effect_dau_data
# 数据源表:     ieg_qt_community_app::tb_app_reg_account
# 创建人名:     llianli
# 创建日期:     2016-03-21
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module
import datetime

# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20150111'
    
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")
    
   
    pre_3_date = today_date - datetime.timedelta(days = 3)
    pre_3_date_str = pre_3_date.strftime("%Y%m%d")
    
    pre_7_date = today_date - datetime.timedelta(days = 7)
    pre_7_date_str = pre_7_date.strftime("%Y%m%d")
    
    pre_14_date = today_date - datetime.timedelta(days = 14)
    pre_14_date_str = pre_14_date.strftime("%Y%m%d")
    
    
    pre_30_date = today_date - datetime.timedelta(days = 30)
    pre_30_date_str = pre_30_date.strftime("%Y%m%d")
    
    pre_60_date = today_date - datetime.timedelta(days = 60)
    pre_60_date_str = pre_60_date.strftime("%Y%m%d")

    pre_90_date = today_date - datetime.timedelta(days = 90)
    pre_90_date_str = pre_90_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS tb_app_effect_dau_data
            (
            dtstatdate INT COMMENT '统计日期',
            iclienttype INT COMMENT '客户端类型',
            idaysflag INT COMMENT '相隔时间',
            idauuin BIGINT COMMENT '总活跃用户',
            ieffectuin BIGINT COMMENT '有效用户'
            ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""DELETE FROM tb_app_effect_dau_data WHERE dtstatdate = %s""" % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
   ##月有效活跃用户
    sql = """
            INSERT TABLE tb_app_effect_dau_data
            SELECT
            %(sDate)s AS dtstatdate,
            iclienttype,
            30 AS idaysflag,
            COUNT(DISTINCT iuin) AS idaucnt,
            COUNT(DISTINCT ieffectuin) AS ieffectuincnt 
            FROM 
            (
            SELECT
            iclienttype,
            iuin,
            CASE WHEN iactdays > 2 THEN iuin ELSE NULL END AS ieffectuin
            FROM 
            (
            SELECT
            iclienttype,
            iuin,
            length(regexp_replace(substr(cbitmap,1,30),'0','')) AS iactdays 
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s AND ilastactdate > %(pre_30_date_str)s AND ilastactdate <= %(sDate)s
            )t
            )t1
            GROUP BY iclienttype

                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##周有效活跃用户
    sql = """
            INSERT TABLE tb_app_effect_dau_data
            SELECT
            %(sDate)s AS dtstatdate,
            iclienttype,
            7 AS idaysflag,
            COUNT(DISTINCT iuin) AS idaucnt,
            COUNT(DISTINCT ieffectuin) AS ieffectuincnt 
            FROM 
            (
            SELECT
            iclienttype,
            iuin,
            CASE WHEN iactdays > 2 THEN iuin ELSE NULL END AS ieffectuin
            FROM 
            (
            SELECT
            iclienttype,
            iuin,
            length(regexp_replace(substr(cbitmap,1,7),'0','')) AS iactdays 
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s AND ilastactdate > %(pre_7_date_str)s AND ilastactdate <= %(sDate)s
            )t
            )t1
            GROUP BY iclienttype

                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##双周有效活跃
    sql = """
            INSERT TABLE tb_app_effect_dau_data
            SELECT
            %(sDate)s AS dtstatdate,
            iclienttype,
            14 AS idaysflag,
            COUNT(DISTINCT iuin) AS idaucnt,
            COUNT(DISTINCT ieffectuin) AS ieffectuincnt 
            FROM 
            (
            SELECT
            iclienttype,
            iuin,
            CASE WHEN iactdays > 2 THEN iuin ELSE NULL END AS ieffectuin
            FROM 
            (
            SELECT
            iclienttype,
            iuin,
            length(regexp_replace(substr(cbitmap,1,14),'0','')) AS iactdays 
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s AND ilastactdate > %(pre_14_date_str)s AND ilastactdate <= %(sDate)s
            )t
            )t1
            GROUP BY iclienttype

                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
    tdw.WriteLog("== end OK ==")
    
    
    
    