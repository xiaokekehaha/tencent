#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_tb_app_reg_uin_day.py
# 功能描述:     掌盟掌火新进用户表构建
# 输入参数:     yyyymmdd    例如：20160321
# 目标表名:     ieg_qt_community_app.tb_app_reg_uin_day
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
            CREATE TABLE IF NOT EXISTS tb_app_reg_uin_day
            (
            dtstatdate INT COMMENT '统计日期',
            iuin BIGINT COMMENT '每日注册UIN用户',
            iclienttype INT COMMENT '客户端类型0：掌盟整体，1：掌火总体'
            )PARTITION BY LIST (dtstatdate)
            (
             partition p_20160314  VALUES IN (20160314),
             partition p_20160315  VALUES IN (20160315)
            )"""
    res = tdw.execute(sql)

    sql="""ALTER TABLE tb_app_reg_uin_day DROP PARTITION (p_%s) """ % (sDate)
    res = tdw.execute(sql)

    sql="""ALTER TABLE tb_app_reg_uin_day ADD PARTITION p_%s VALUES IN (%s) """ % (sDate,sDate)
    res = tdw.execute(sql)
    

    sql = """
            INSERT  TABLE tb_app_reg_uin_day
            SELECT 
            DISTINCT 
            dtstatdate,
            iuin,
            iclienttype
            FROM tb_app_reg_account PARTITION (p_%s) a WHERE dtstatdate = %s AND iregdate = %s
                    """ % (sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
    tdw.WriteLog("== end OK ==")
    
    
    
    