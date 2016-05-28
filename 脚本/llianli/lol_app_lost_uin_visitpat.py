#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_lost_uin_visit_path.py
# 功能描述:     掌盟周流失用户最后七天访问路径
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_app_visit_path
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-12-10
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
            INSERT TABLE tb_app_visit_path
SELECT 
t.id AS id,
t.uin_info AS uin_info,
t1.pi AS pi,
t1.rf AS rf
FROM 
(
SELECT 
id,
uin_info
FROM tb_app_lost_uin_info WHERE ilastday = %s
)t
JOIN
(
SELECT 
id,
concat(ui,mc) AS uin_info,
pi,
rf
FROM  teg_mta_intf::ieg_lol WHERE sdate > date_sub(%s,7) AND sdate <= %s
)t1
ON (t.id = t1.id AND t.uin_info = t1.uin_info)

                    """ % (sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    

    
    
    