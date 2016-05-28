#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     cfapp_growth_system_integration_client.py
# 功能描述:     掌火成长体系积分客户端点击数据
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::tb_cfapp_grouwth_system_integration_client
# 数据源表:     teg_mta_intf.ieg_lol 
# 创建人名:     llianli
# 创建日期:     2016-05-17
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
            CREATE TABLE IF NOT EXISTS tb_cfapp_grouwth_system_integration_client
            (
            dtstatdate INT COMMENT '统计日期',
            ei STRING COMMENT '积分相关点击事件上报',
            pv BIGINT COMMENT '点击PV',
            mac_uv BIGINT COMMENT '设备号计算UV',
            uin_uv BIGINT COMMENT 'qq号计算UV'
            )


    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""delete from tb_cfapp_grouwth_system_integration_client where dtstatdate=%s  """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    sql = """
            INSERT TABLE tb_cfapp_grouwth_system_integration_client
            SELECT
            %s AS dtstatdate,
            ei,
            COUNT(*) AS pv,
            COUNT(DISTINCT uin_mac) AS mac_uv,
            COUNT(DISTINCT uin) AS uin_uv
            FROM 
            (
            SELECT
            concat(ui,mc) AS uin_mac,
            get_json_object(kv,'$.uin') AS uin,
            CASE 
                WHEN ei = '积分中心点击次数' OR ei = '积分中心_点击积分中心' THEN '积分中心点击'
                WHEN ei = '积分明细点击次数' OR ei = '积分中心_点击积分明细' THEN '积分明细点击'
                WHEN ei = '我的成长点击次数' OR ei = '我的成长_点击我的成长入口' THEN '我的成长点击' 
                ELSE 'other' 
            END AS ei
            FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND id IN (1100679031,1200679031)
            )t
            WHERE ei != 'other'
            GROUP BY ei     
            """ % (sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
