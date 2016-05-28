#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_cf_nba_upload_vedio_num.py
# 功能描述:     掌萌掌火观看视频时长统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::lol_cf_nba_upload_vedio_num
# 数据源表:     ieg_tdbank::qtalk_dsl_herotimeinfo_fht0
# 创建人名:     yaoyaopeng
# 创建日期:     2016-02-17
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
def TDW_PL(tdw, argv=[]):

    print "===HELLO TDW=="
    tdw.WriteLog("===statistic upload vedio num===")
    
#接收日期参数
    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")
    sDate = argv[0];
    
    tdw.WriteLog("== sDate = " + sDate + " ==")

#连接tdw， ieg_qt_community_app数据库
    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)
    

#创建结果表   
    tdw.WriteLog("== create table lol_cf_nba_upload_vedio_num==")
    sql = """CREATE TABLE IF NOT EXISTS lol_cf_nba_upload_vedio_num(
        sdate bigint,
        game_id string,
        vedio_num bigint
        )"""
    tdw.WriteLog(sql)    
    res = tdw.execute(sql)
    tdw.WriteLog("== create table lol_cf_nba_upload_vedio_num success==")

#确认插入数据日期的唯一性    
    tdw.WriteLog("== check date==")
    sql="""delete from lol_cf_nba_upload_vedio_num where sdate=%s"""%(sDate)
    res = tdw.execute(sql)
    tdw.WriteLog("== check date success==")
    
#插入数据        
    tdw.WriteLog("== insert table==")
    sql="""INSERT TABLE  lol_cf_nba_upload_vedio_num 
    SELECT 
    %s,
    t.game_name,
    COUNT(DISTINCT(t.vid)) as vedio_num
    FROM 
    (SELECT 
    vid,
    CASE 
    WHEN game_id = 2100993 THEN 'nba'
    WHEN game_id = 2103041 THEN 'lol'
    WHEN game_id = 2104833 THEN 'cf'
    ELSE 'other'
    END AS game_name 
    FROM  ieg_tdbank::qtalk_dsl_herotimeinfo_fht0
    WHERE tdbank_imp_date between '%s00' AND '%s23')t 
    WHERE t.game_name != 'other'
    GROUP BY t.game_name 
    """%(sDate,sDate,sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
