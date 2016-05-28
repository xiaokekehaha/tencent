#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_dau_statics_tb_app_original_data.py
# 功能描述:     掌盟掌火活跃数据统计
# 输入参数:     yyyymmdd    例如：20160309
# 目标表名:     ieg_qt_community_app.tb_app_original_data
# 数据源表:     ieg_tdbank::qtalk_dsl_QTXReqStat_fht0
# 创建人名:     llianli
# 创建日期:     2016-03-09
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
            CREATE TABLE IF NOT EXISTS tb_app_original_data
            (
            dtstatdate INT COMMENT '活跃日期',
            iuin BIGINT COMMENT '用户UIN',
            iclienttype INT COMMENT '客户端类型，9：掌盟安卓，10：掌盟IOS，15：掌火安卓，16：掌火IOS'
            )PARTITION BY LIST (dtstatdate)
                (
                partition p_20151201  VALUES IN (20151201),
                partition p_20151202  VALUES IN (20151202)
                ) """
    res = tdw.execute(sql)

    sql="""ALTER TABLE tb_app_original_data DROP PARTITION (p_%s) """ % (sDate)
    res = tdw.execute(sql)

    sql="""ALTER TABLE tb_app_original_data ADD PARTITION p_%s VALUES IN (%s) """ % (sDate,sDate)
    res = tdw.execute(sql)
    

    sql = """
            INSERT TABLE tb_app_original_data 
            SELECT  
            %s AS dtstatdate,
            iuin,
            iclienttype 
            FROM ieg_tdbank::qtalk_dsl_QTXReqStat_fht0
            WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'  AND  ( iClientType NOT IN (9, 10) OR iCmd  NOT IN  (13314, 12545, 13081)) AND iClientType IN (9,10,15,16) 
            GROUP BY iuin,iclienttype

                    """ % (sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
    tdw.WriteLog("== end OK ==")
    
    
    
    