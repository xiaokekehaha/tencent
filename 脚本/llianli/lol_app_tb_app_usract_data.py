#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_tb_app_usract_data.py
# 功能描述:     掌盟掌火活跃活跃新增结果统计
# 输入参数:     yyyymmdd    例如：20160309
# 目标表名:     ieg_qt_community_app.tb_app_usract_data
# 数据源表:     ieg_qt_community_app::tb_app_reg_account
# 创建人名:     llianli
# 创建日期:     2016-03-21
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ********************************q**********************************************


#import system module
import datetime

# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20150111'
    ##对日期做统一处理
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
            CREATE TABLE IF NOT EXISTS tb_app_usract_data
            (
            dtstatdate INT COMMENT '统计日期',
            idayrange INT COMMENT '统计周期 1:日，7：七日，14：双周，30：月',
            iclienttype INT COMMENT '客户端类型',
            iactuin BIGINT COMMENT '活跃用户',
            idnuuin BIGINT COMMENT '统计新增用户'
            )"""
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""DELETE FROM tb_app_usract_data WHERE dtstatdate = %s """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    

    sql = """
            
            INSERT TABLE tb_app_usract_data
            
            SELECT
            %(sDate)s AS dtstatdate,
            1 AS idayrange,
            iclienttype,
            COUNT(DISTINCT iactuin) AS iactuin,
            COUNT(DISTINCT idnuuin) AS idnuuin
            FROM 
            ( 
            SELECT
            iuin,
            iclienttype,
            CASE WHEN iregdate = %(sDate)s THEN iuin ELSE NULL END AS idnuuin,
            CASE WHEN ilastactdate = %(sDate)s THEN iuin ELSE NULL END AS iactuin
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s
            )t
            GROUP BY iclienttype
            
            
            
            UNION ALL 
            
            SELECT
            %(sDate)s AS dtstatdate,
            7 AS idayrange,
            iclienttype,
            COUNT(DISTINCT iactuin) AS iactuin,
            COUNT(DISTINCT idnuuin) AS idnuuin
            FROM 
            ( 
            SELECT
            iuin,
            iclienttype,
            CASE WHEN iregdate > %(pre_7_date_str)s AND iregdate <= %(sDate)s THEN iuin ELSE NULL END AS idnuuin,
            CASE WHEN ilastactdate > %(pre_7_date_str)s AND ilastactdate <= %(sDate)s THEN iuin ELSE NULL END AS iactuin
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s 
            )t
            GROUP BY iclienttype
            
            UNION ALL 
            
            SELECT
            %(sDate)s AS dtstatdate,
            14 AS idayrange,
            iclienttype,
            COUNT(DISTINCT iactuin) AS iactuin,
            COUNT(DISTINCT idnuuin) AS idnuuin
            FROM 
            ( 
            SELECT
            iuin,
            iclienttype,
            CASE WHEN iregdate > %(pre_14_date_str)s AND iregdate <= %(sDate)s THEN iuin ELSE NULL END AS idnuuin,
            CASE WHEN ilastactdate > %(pre_14_date_str)s AND ilastactdate <= %(sDate)s THEN iuin ELSE NULL END AS iactuin
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s 
            )t
            GROUP BY iclienttype
            
            
            
            UNION ALL 
            
            SELECT
            %(sDate)s AS dtstatdate,
            30 AS idayrange,
            iclienttype,
            COUNT(DISTINCT iactuin) AS iactuin,
            COUNT(DISTINCT idnuuin) AS idnuuin
            FROM 
            ( 
            SELECT
            iuin,
            iclienttype,
            CASE WHEN iregdate > %(pre_30_date_str)s AND iregdate <= %(sDate)s THEN iuin ELSE NULL END AS idnuuin,
            CASE WHEN ilastactdate > %(pre_30_date_str)s AND ilastactdate <= %(sDate)s THEN iuin ELSE NULL END AS iactuin
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s 
            )t
            GROUP BY iclienttype
            
            UNION ALL 
            
            SELECT
            %(sDate)s AS dtstatdate,
            1 AS idayrange,
            -100 AS iclienttype,
            COUNT(DISTINCT iactuin) AS iactuin,
            COUNT(DISTINCT idnuuin) AS idnuuin
            FROM 
            ( 
            SELECT
            iuin,
            iclienttype,
            CASE WHEN iregdate = %(sDate)s THEN iuin ELSE NULL END AS idnuuin,
            CASE WHEN ilastactdate = %(sDate)s THEN iuin ELSE NULL END AS iactuin
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s AND iclienttype IN (0,1)
            )t
                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
    tdw.WriteLog("== end OK ==")
    
    
    
    