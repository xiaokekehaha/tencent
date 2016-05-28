#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_statics_temp_huaxiang.py
# 功能描述:     掌盟临时数据统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_temp_lol_app_black_battle_info_20160302
# 目标表名:     ieg_qt_community_app.tb_temp_lol_app_normal_battle_info_20160302
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2014-10-29
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


    tdw.WriteLog("== temp = " + 'temp' + " ==")

    sDate = argv[0];
    ##sDate = '20141201'

    ##tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

##    sql = """set hive.inputfiles.splitbylinenum=true"""
##    res = tdw.execute(sql)
##    sql = """set hive.inputfiles.line_num_per_split=1000000"""
##    res = tdw.execute(sql)


    ##掌盟每小时内用户使用功能和使用环境
    sql = """
    INSERT  TABLE tb_lol_app_actionid_table_cn_temp
SELECT
sdate,
uin_mac,
appid,
ihour,
cn
FROM 
(
SELECT
sdate,
uin_mac,
appid,
ihour,
cn,
MAX(ts) 
FROM 
(
SELECT
sdate,
concat(ui,mc) AS uin_mac,
id AS appid,
concat(substr(from_unixtime(ts),1,4),substr(from_unixtime(ts),6,2),substr(from_unixtime(ts),9,2)) AS test_date,
substr(from_unixtime(ts),12,2) AS ihour,
cn,ts
FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND id IN (1100678382,1200678382) 
AND et = 2 AND (ui != '000000000000000' OR mc != '000000000000') AND (ui != '-' OR mc != '-') AND (ui != '000000000000000' OR mc != '-')
)t WHERE test_date = CAST(sdate AS STRING)
GROUP BY sdate,uin_mac,appid,ihour,cn
)t1 
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
   
   


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    