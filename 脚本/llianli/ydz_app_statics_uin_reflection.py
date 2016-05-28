#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_staics_uin_reflection.py
# 功能描述:     油点赞数据统计——设备号与账号的映射关系
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_app_page_action_width_table
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-04-01
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
            CREATE TABLE IF NOT EXISTS tb_ydz_app_uin_mac_reflection
            (
            sdate INT COMMENT '统计日期',
            appid BIGINT COMMENT 'APPID',
            uin_mac STRING COMMENT '用户的设备信息',
            iaccountid BIGINT COMMENT '用户的账号信息'
            )PARTITION BY LIST (sdate)
            (
            PARTITION p_20160318 VALUES IN (20160318),
            PARTITION p_20160319 VALUES IN (20160319),
            PARTITION p_20160320 VALUES IN (20160320),
            PARTITION p_20160321 VALUES IN (20160321),
            PARTITION p_20160322 VALUES IN (20160322),
            PARTITION p_20160323 VALUES IN (20160323),
            PARTITION p_20160324 VALUES IN (20160324),
            PARTITION p_20160325 VALUES IN (20160325),
            PARTITION p_20160326 VALUES IN (20160326),
            PARTITION p_20160327 VALUES IN (20160327),
            PARTITION p_20160328 VALUES IN (20160328),
            PARTITION p_20160329 VALUES IN (20160329),
            PARTITION p_20160330 VALUES IN (20160330),
            PARTITION p_20160331 VALUES IN (20160331)
            ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql=""" ALTER TABLE tb_ydz_app_uin_mac_reflection DROP PARTITION (p_%s) """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ ALTER TABLE tb_ydz_app_uin_mac_reflection ADD PARTITION p_%s VALUES IN (%s) """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    



    sql = """
           INSERT TABLE tb_ydz_app_uin_mac_reflection
            SELECT
            DISTINCT 
            sdate,
            id,
            concat(ui,mc) AS uin_mac,
            get_json_object(kv,'$.account') AS accountid
            FROM
             teg_mta_intf::ieg_youxishengjing WHERE sdate =%s  AND id IN (1100679541,1200679541) AND et = 1000
             AND (ui != '000000000000000' OR mc != '000000000000') AND (ui != '-' OR mc != '-') AND (ui != '000000000000000' OR mc != '-')
             
                    """ % (sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
      

    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    