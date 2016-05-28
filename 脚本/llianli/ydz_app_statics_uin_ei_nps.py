#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_staics_uin_ei_nps.py
# 功能描述:     油点赞数据统计——页面ID的统计
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
            CREATE TABLE IF NOT EXISTS tb_ydz_app_click_action_avg_temp_%s
            (
            appid BIGINT COMMENT 'APPID',
            ei STRING COMMENT '操作信息',
            feed_id BIGINT COMMENT 'feedid',
            seg_6_pv BIGINT COMMENT '打分为6的所有用户访问量平均值',
            seg_6_time BIGINT COMMENT '打分为6的所有用户访问时长平均值',
            seg_9_pv BIGINT COMMENT '打分为9的所有用户访问量平均值',
            seg_9_time BIGINT COMMENT '打分为9的所有用户访问时长平均值'
            )
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    sql = """
    INSERT OVERWRITE TABLE tb_ydz_app_click_action_avg_temp_%s
    SELECT
    appid,
    ei,
    feed_id,
    avg(seg_6_pv) AS seg_6_pv,
    avg(seg_6_time) AS seg_6_pv,
    avg(seg_9_pv) AS seg_6_pv,
    avg(seg_6_time) AS seg_6_time
    FROM 
    (
    SELECT
    a.appid AS appid,
    b.ei AS ei,
    b.feed_id AS feed_id,
    CASE WHEN a.iscore = 6 THEN b.pv ELSE 0 END AS seg_6_pv,
    CASE WHEN a.iscore = 6 THEN b.time ELSE 0 END AS seg_6_time,
    CASE WHEN a.iscore = 9 THEN b.time ELSE 0 END AS seg_9_pv,
    CASE WHEN a.iscore = 9 THEN b.time ELSE 0 END AS seg_9_time
    FROM 
    (
    SELECT 
    CASE 
        WHEN lower(sostype) = 'android' THEN 1100679541
        WHEN lower(sostype) = 'ios' THEN 1200679541
        ELSE -100 
    END AS appid ,
    account,
    iscore
    FROM  tb_ydz_user_score_data PARTITION (p_%s) tmp 
    WHERE iscore IN (6,9)
    ) a
    
    JOIN 
    (
    SELECT
    appid,
    accountid,
    ei,
    feed_id,
    SUM(pv) AS pv,
    SUM(time) AS time
    FROM 
     tb_ydz_app_click_data_width_table 
     GROUP BY appid,accountid,ei,feed_id
     )b 
    ON (a.appid = b.appid AND a.account = b.accountid)
    )t
    GROUP BY appid,ei,feed_id
    
    
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = """
    CREATE TABLE IF NOT EXISTS tb_ydz_app_action_nps
    (
    dtstatdate INT COMMENT '统计日期',
    appid INT COMMENT '应用ID',
    ei STRING COMMENT '行为信息',
    feedid STRING COMMENT 'feedid',
    total_cnt BIGINT COMMENT '全量用户',
    low_pv_cnt BIGINT COMMENT 'pv小于6分平均值的用户量',
    mid_pv_cnt BIGINT COMMENT 'pv介于6-9分平均值的用户量',
    high_pv_cnt BIGINT COMMENT 'pv大于6分平均值的用户量',
    low_time_cnt BIGINT COMMENT 'time小于6分平均值的用户量',
    mid_time_cnt BIGINT COMMENT 'time介于6-9分平均值的用户量',
    high_time_cnt BIGINT COMMENT 'time大于6分平均值的用户量'
    )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = """
    DELETE FROM tb_ydz_app_action_nps WHERE  dtstatdate = %s 
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    INSERT TABLE tb_ydz_app_action_nps
    SELECT
    %s AS dtstatdate,
    appid,
    ei,
    CASE WHEN GROUPING(feed_id) = 1 THEN '-100' ELSE CAST(feed_id AS STRING) END AS feed_id,
    COUNT(DISTINCT accountid) AS total_cnt,
    COUNT(DISTINCT low_pv_account) AS low_pv_cnt,
    COUNT(DISTINCT mid_pv_account) AS mid_pv_cnt,
    COUNT(DISTINCT high_pv_account) AS high_pv_cnt,
    COUNT(DISTINCT low_time_account) AS low_time_cnt,
    COUNT(DISTINCT mid_time_account) AS mid_time_cnt,
    COUNT(DISTINCT high_time_account) AS high_time_cnt
    FROM 
    (
    SELECT
    a.appid AS appid,
    a.ei AS ei,
    a.feed_id AS feed_id,
    b.accountid AS accountid,
    CASE WHEN b.pv <= a.seg_6_pv THEN b.accountid ELSE NULL END AS low_pv_account,
    CASE WHEN b.pv > a.seg_6_pv AND b.pv < a.seg_9_pv THEN b.accountid ELSE NULL END AS mid_pv_account,
    CASE WHEN b.pv >= a.seg_9_pv THEN b.accountid ELSE NULL END AS high_pv_account,
    
    CASE WHEN b.time <= a.seg_6_time THEN b.accountid ELSE NULL END AS low_time_account,
    CASE WHEN b.time > a.seg_6_time AND b.time < a.seg_9_time THEN b.accountid ELSE NULL END AS mid_time_account,
    CASE WHEN b.time >= a.seg_9_time THEN b.accountid ELSE NULL END AS high_time_account
    
    FROM tb_ydz_app_click_action_avg_temp_%s a 
    JOIN
    (
    SELECT
    appid,
    accountid,
    ei,
    feed_id,
    SUM(pv) AS pv,
    SUM(time) AS time
    FROM 
     tb_ydz_app_click_data_width_table 
     GROUP BY appid,accountid,ei,feed_id
     ) b
    ON(a.appid = b.appid AND a.ei = b.ei AND a.feed_id = b.feed_id)
    )t
    GROUP BY appid,ei,cube(feed_id)
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ DROP TABLE tb_ydz_app_click_action_avg_temp_%s """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    