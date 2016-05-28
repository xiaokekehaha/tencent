#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_score.py
# 功能描述:     油点赞数据统计——用户得分累加数据获取
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_user_score_data
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
            CREATE TABLE IF NOT EXISTS tb_ydz_user_score_data
         (
         dtstatdate INT COMMENT '统计时间',
         account BIGINT COMMENT '用户账户信息',
         sostype STRING COMMENT '用户的操作系统类型',
         machinetype INT COMMENT '用户是否是机器人账户',
         iscore INT COMMENT '用户打分值',
         itimestamp INT COMMENT '用户打分的时间戳'
         )PARTITION BY LIST (dtstatdate)
            (
            PARTITION p_20160405 VALUES IN (20160405),
            PARTITION p_20160406 VALUES IN (20160406)
            )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ ALTER TABLE tb_ydz_user_score_data DROP PARTITION (p_%s)"""%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """ ALTER TABLE tb_ydz_user_score_data ADD PARTITION p_%s VALUES IN (%s)"""%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##建立一个临时表把每日数据写入
    sql = """
    CREATE TABLE IF NOT EXISTS tb_ydz_user_score_data_temp_%s
    (
    account BIGINT COMMENT '账号信息',
    vostype STRING COMMENT '操作系统类型',
    machinetype INT COMMENT '是否是机器人标志1：是，0：不是',
    iscore BIGINT COMMENT '用户得分',
    itimestamp BIGINT COMMENT '当前最大时间戳'
    )
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = """
    INSERT OVERWRITE TABLE tb_ydz_user_score_data_temp_%s

    SELECT
    account,
    sostype AS vostype,
    machinetype,
    iscore,
    itimestamp 
    FROM tb_ydz_user_score_data PARTITION (p_%s) a
    
    UNION ALL 
    SELECT
    account,
    CASE WHEN GROUPING(vostype) = 1 THEN '-100' ELSE vostype END AS vostype,
    CASE WHEN GROUPING(machinetype) = 1 THEN -100 ELSE machinetype END AS machinetype,
    iscore,
    itimestamp 
    FROM 
    (
    SELECT
    a.account AS account,
    a.sostype AS vostype,
    CASE WHEN b.iaccountid IS NULL THEN 0 ELSE 1 END AS machinetype,
    a.iscore AS iscore,
    a.itimestamp AS itimestamp
    FROM 
    (
        SELECT
        isybid AS account,
        vostype AS sostype,
        ik1 AS iscore,
        unix_timestamp(dteventtime) AS itimestamp 
        FROM ieg_tdbank::gqq_dsl_gamebible_day_task_bill_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23' AND  iactiontype = 4  AND iactionid = 1  AND isybid != 0
    )a
    LEFT OUTER JOIN 
    tb_ydz_machine_account b
    ON(a.account = b.iaccountid)
    )c 
    GROUP BY account,iscore,itimestamp,cube(vostype,machinetype) 
    """%(sDate,pre_date_str,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##临时表的数据写入最终表
    sql = """
    INSERT TABLE tb_ydz_user_score_data
    SELECT
    %s AS dtstatdate,
    a.account AS account,
    a.vostype AS vostype,
    a.machinetype AS machinetype,
    a.iscore AS iscore,
    a.itimestamp AS itimestamp
    FROM 
    (
    SELECT
    account,
    vostype,
    machinetype,
    iscore,
    itimestamp
    FROM 
    tb_ydz_user_score_data_temp_%s
    GROUP BY account,vostype,machinetype,iscore,itimestamp
    ) a
    JOIN
    (
    SELECT
    account,
    vostype,
    machinetype,
    MAX(itimestamp) AS imaxtimestamp
    FROM 
    tb_ydz_user_score_data_temp_%s
    GROUP BY account,vostype,machinetype
    )b
    on (a.account = b.account AND a.vostype = b.vostype AND a.itimestamp = b.imaxtimestamp AND a.machinetype = b.machinetype)

    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ drop table tb_ydz_user_score_data_temp_%s"""%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    