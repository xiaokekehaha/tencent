#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_uin_pageview_water.py
# 功能描述:     油点赞数据统计——页面曝光量
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_mta_pageview_action_width_table
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
    CREATE TABLE IF NOT EXISTS tb_ydz_mta_pageview_action_width_table
    (
    dtstatdate INT COMMENT '统计日期',
    appid INT COMMENT 'APPID',
    accounttype INT COMMENT '用户是否登录的标记',
    machineflag INT COMMENT '用户是否是机器登录的标记',
    uin_mac STRING COMMENT '用户设备信息',
    accountid BIGINT COMMENT '用户的账号ID',
    moduleid STRING COMMENT '页面ID',
    kv STRING COMMENT 'moduleid之后的参数,目前只对gameid 和tagid有效',
    du BIGINT COMMENT '时长信息'
    )
    PARTITION BY LIST (dtstatdate)
    (
        PARTITION p_20160413  VALUES IN (20160413),
        PARTITION p_20160414  VALUES IN (20160414)
    )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    ALTER TABLE tb_ydz_mta_pageview_action_width_table DROP PARTITION (p_%s)
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """ ALTER TABLE tb_ydz_mta_pageview_action_width_table ADD PARTITION p_%s VALUES IN (%s)"""%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
   INSERT TABLE tb_ydz_mta_pageview_action_width_table
    SELECT
    %s AS dtstatdate,
    t.appid AS appid,
    t.accounttype AS accounttype,
    CASE  WHEN t1.iaccountid IS NOT NULL THEN 1 ELSE 0 END AS machineflag,
    t.uin_mac AS uin_mac,
    t.accountid AS accountid,
    t.module_id AS module_id,
    t.kv AS kv,
    t.du AS time
    FROM 
    (
    SELECT
    id AS appid,
    CASE 
        WHEN ext4 = '-' THEN 0
        WHEN CAST(ext4 AS BIGINT) > 4200000000 OR  CAST(ext4 AS BIGINT) = 0 THEN 0 
        WHEN CAST(ext4 AS BIGINT) IS NULL THEN 0
    ELSE 1 
    END AS accounttype,
    
    concat(ui,mc) AS uin_mac,
    
    CASE 
        WHEN ext4 = '-' THEN -1
        WHEN CAST(ext4 AS BIGINT) > 4200000000 OR  CAST(ext4 AS BIGINT) = 0 THEN -1 
        WHEN CAST(ext4 AS BIGINT) IS NULL THEN -1
    ELSE CAST(ext4 AS BIGINT) 
    END AS accountid,
    
    CASE WHEN (pi LIKE '%%tag_sub_feeds%%' OR  pi LIKE '%%game_sub_feeds%%') AND INSTR(pi,'#') != 0 THEN split(pi,'#')[0]  ELSE pi END AS module_id,
    
    CASE WHEN (pi LIKE '%%tag_sub_feeds%%' OR  pi LIKE '%%game_sub_feeds%%') AND INSTR(pi,'#') != 0 THEN split(pi,'#')[1]  ELSE '-' END AS kv,
    du
    FROM teg_mta_intf::ieg_youxishengjing WHERE sdate = %s AND id IN (1100679541,1200679541) AND et = 1 AND du <= 30*60
    )t 
    LEFT OUTER JOIN
     tb_ydz_machine_account t1 
     ON(t.accountid = t1.iaccountid)

     """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    