#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_score_nps.py
# 功能描述:     油点赞数据统计——用户得分每日的NPS数据
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_score_nps
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-04-08
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module
import datetime

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
    today_str = sDate
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d") 

    sql = """
            CREATE TABLE IF NOT EXISTS tb_ydz_score_nps
            (
            dtstatdate INT COMMENT '统计时间',
            sostype STRING COMMENT '操作系统类型',
            machinetype INT COMMENT '是否是机器人  0 不是 1 是',
            iscore INT COMMENT '用户打分的分值',
            total_cnt BIGINT COMMENT '总用户数'
            )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ DELETE FROM tb_ydz_score_nps WHERE dtstatdate = %s"""%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    
    
    
    ##NPS数据计算
    sql = """
    INSERT TABLE tb_ydz_score_nps
    SELECT
    %s as dtstatdate,
    sostype,
    machinetype,
    CASE WHEN GROUPING(iscore) = 1 THEN -100 ELSE iscore END AS iscore,
    COUNT(DISTINCT account) AS total_cnt
    FROM  
    (
    SELECT
    dtstatdate,
    account,
    sostype,
    machinetype,
    iscore
    FROM  tb_ydz_user_score_data  PARTITION (p_%s) a
    )t
    GROUP BY sostype,machinetype,dtstatdate,cube(iscore)
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    