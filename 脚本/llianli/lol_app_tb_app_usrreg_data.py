#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_tb_app_usrreg_data.py
# 功能描述:     掌盟掌火总注册用户数据统计
# 输入参数:     yyyymmdd    例如：20160309
# 目标表名:     ieg_qt_community_app.tb_app_original_data
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


# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20150111'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS tb_app_usrreg_data
            (
            dtstatdate INT COMMENT '统计日期',
            iclienttype INT COMMENT '客户端类型',
            ireguin BIGINT COMMENT '总注册用户'
            ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""DELETE FROM tb_app_usrreg_data WHERE dtstatdate = %s """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

   

    sql = """
            INSERT TABLE tb_app_usrreg_data
            SELECT
            %(sDate)s AS dtstatdate,
            iclienttype,
            COUNT(DISTINCT iuin) AS ireguin
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s
            GROUP BY iclienttype
            
            UNION ALL 
            
            
            SELECT
            %(sDate)s AS dtstatdate,
            -100 AS iclienttype,
            COUNT(DISTINCT iuin) AS ireguin
            FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s AND iclienttype IN (0,1)


                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
    tdw.WriteLog("== end OK ==")
    
    
    
    