#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_uin_click_action_nps.py
# 功能描述:     油点赞数据统计——用户点击NPS数据统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_click_action_nps
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-04-18
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
    today_str = sDate
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d") 
    
    
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
    CREATE TABLE IF NOT EXISTS tb_ydz_app_click_action_avg_temp_%s
            (
            appid BIGINT COMMENT 'APPID',
            machineflag BIGINT COMMENT '是否是机器人',
            moduleid STRING COMMENT '页面信息',
            saction STRING COMMENT '用户行为',
            seg_6_pv FLOAT COMMENT '打分为6的所有用户访问量平均值',
            seg_6_time FLOAT COMMENT '打分为6的所有用户访问时长平均值',
            seg_9_pv FLOAT COMMENT '打分为9的所有用户访问量平均值',
            seg_9_time FLOAT COMMENT '打分为9的所有用户访问时长平均值'
            )
    
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##统计出来得分是6和得分是9的用户的人均值
    sql = """
    INSERT OVERWRITE TABLE tb_ydz_app_click_action_avg_temp_%s
    SELECT
    appid,
    machineflag,
    moduleid,
    saction,
    CASE WHEN seg_6_account = 0 THEN 0.0 ELSE seg_6_pv/seg_6_account END  AS seg_6_pv,
    CASE WHEN seg_6_account = 0 THEN 0.0 ELSE seg_6_time/seg_6_account END AS seg_6_time,
    CASE WHEN seg_9_account = 0 THEN 1000000000.0 ELSE seg_9_pv/seg_9_account END AS seg_9_pv,
    CASE WHEN seg_9_account = 0 THEN 1000000000.0 ELSE seg_9_time/seg_9_account END  AS seg_9_time
    FROM 
    (
    SELECT
    appid,
    machineflag,
    moduleid,
    saction,
    COUNT(DISTINCT seg_6_account) AS seg_6_account,
    COUNT(DISTINCT seg_9_account) AS seg_9_account,
    SUM(seg_6_pv) AS seg_6_pv,
    SUM(seg_6_time) AS seg_6_time,
    SUM(seg_9_pv) AS seg_9_pv,
    SUM(seg_9_time) AS seg_9_time
    FROM
    (
    SELECT
    a.appid AS appid,
    a.machinetype AS machineflag,
    b.moduleid AS moduleid,
    b.saction AS saction,
    CASE WHEN a.iscore = 6 THEN a.account ELSE NULL END AS seg_6_account,
    CASE WHEN a.iscore = 9 THEN a.account ELSE NULL END AS seg_9_account,
    CASE WHEN a.iscore = 6 THEN b.pv ELSE 0 END AS seg_6_pv,
    CASE WHEN a.iscore = 6 THEN b.du ELSE 0 END AS seg_6_time,
    CASE WHEN a.iscore = 9 THEN b.pv ELSE 0 END AS seg_9_pv,
    CASE WHEN a.iscore = 9 THEN b.du ELSE 0 END AS seg_9_time
    FROM 
    (
    SELECT 
    CASE 
        WHEN lower(sostype) = 'android' THEN 1100679541
        WHEN lower(sostype) = 'ios' THEN 1200679541
        ELSE -100 
    END AS appid ,
    machinetype,
    account,
    iscore
    FROM  tb_ydz_user_score_data PARTITION (p_%s) tmp 
    ) a
    
    JOIN 
     tb_ydz_click_action_accumulate  PARTITION (p_%s) b 
    ON (a.appid = b.appid AND a.account = b.accountid AND a.machinetype = b.machineflag )
    )t
    GROUP BY appid,    machineflag,    moduleid,    saction
    )t1
     """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##NPS值结果表
    sql = """
    CREATE TABLE IF NOT EXISTS tb_ydz_click_action_nps
    (
    dtstatdate INT COMMENT '统计日期',
    appid INT COMMENT '应用ID',
    machineflag INT COMMENT '是否是机器人账户 0 不是 1 是',
    moduleid STRING COMMENT '行为信息',
    
    saction STRING COMMENT '行为数据', 
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
    DELETE FROM tb_ydz_click_action_nps WHERE dtstatdate = %s
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    ##计算NPS结果值
    sql = """
    INSERT TABLE tb_ydz_click_action_nps
    SELECT
    %s  AS dtstatdate,
    appid,
    machineflag,
    moduleid,
    saction,
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
    a.moduleid AS moduleid,
    a.machineflag AS machineflag,
    a.saction AS saction,
    b.accountid AS accountid,
    CASE WHEN b.pv <= a.seg_6_pv THEN b.accountid ELSE NULL END AS low_pv_account,
    CASE WHEN b.pv > a.seg_6_pv AND b.pv < a.seg_9_pv THEN b.accountid ELSE NULL END AS mid_pv_account,
    CASE WHEN b.pv >= a.seg_9_pv THEN b.accountid ELSE NULL END AS high_pv_account,
    
    CASE WHEN b.du <= a.seg_6_time THEN b.accountid ELSE NULL END AS low_time_account,
    CASE WHEN b.du > a.seg_6_time AND b.du < a.seg_9_time THEN b.accountid ELSE NULL END AS mid_time_account,
    CASE WHEN b.du >= a.seg_9_time THEN b.accountid ELSE NULL END AS high_time_account
    
    FROM tb_ydz_app_click_action_avg_temp_%s a 
    JOIN
      tb_ydz_click_action_accumulate  PARTITION (p_%s) b
    ON(a.appid = b.appid AND a.moduleid = b.moduleid AND a.machineflag = b.machineflag AND a.saction = b.saction)
    )t
    GROUP BY appid,moduleid, machineflag,saction
     """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ DROP TABLE tb_ydz_app_click_action_avg_temp_%s"""%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    