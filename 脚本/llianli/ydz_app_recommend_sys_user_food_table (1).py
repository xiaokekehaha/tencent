#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_recommend_sys_user_food_table.py
# 功能描述:     游点赞推荐系统数据统计——用户使用行为表
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_app_recommend_sys_usr_food
# 数据源表:     teg_mta_intf.ieg_youxishengjing
# 创建人名:     llianli
# 创建日期:     2016-04-29
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
    CREATE TABLE IF NOT EXISTS tb_ydz_app_recommend_sys_usr_food
    (
    dtstatdate BIGINT COMMENT '统计时间：表会按照统计时间进行分区',
    accountid BIGINT COMMENT '用户账号ID',
    feed_id INT COMMENT '用户操作的feedid',
    read_flag INT COMMENT '用户是否阅读的标记',
    feed_read_time INT COMMENT '用户阅读时长，现在没有写-1，确认这里有数据之后再使用该字段',
    zan_flag INT COMMENT '用户是否点赞的标记',
    big_picture_flag INT COMMENT '用户是否点击大图的标记',
    comment_add_flag INT COMMENT '是否添加评论的标记',
    comment_view_flag INT COMMENT '评论是否被查看的标记',
    share_flag INT COMMENT 'feed是否被分享的标记',
    last_aciton_ts BIGINT COMMENT '最后一次操作的时间戳'
    )
    PARTITION BY LIST(dtstatdate)
    (
            PARTITION p_20160427  VALUES IN (20160427),
            PARTITION p_20160428  VALUES IN (20160428)
    )
    
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    ALTER TABLE tb_ydz_app_recommend_sys_usr_food DROP PARTITION (p_%(sDate)s)
     """%({"sDate":sDate})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    ALTER TABLE tb_ydz_app_recommend_sys_usr_food ADD PARTITION p_%(sDate)s VALUES IN (%(sDate)s)
     """%({"sDate":sDate})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##记录历史信息，这个作为备注不执行
    '''
    sql = """ INSERT TABLE tb_ydz_app_recommend_sys_usr_food
    SELECT 
    20160427 AS dtstatdate,
    accountid,
    feed_id,
    CASE WHEN SUM(read_flag) > 0 THEN 1 ELSE 0 END AS read_flag,
    SUM(feed_read_time)  AS feed_read_time,
    CASE WHEN SUM(zan_flag) > 0 THEN 1 ELSE 0 END AS zan_flag,
    CASE WHEN SUM(big_picture_flag) > 0 THEN 1 ELSE 0 END AS big_picture_flag,
    CASE WHEN SUM(comment_add_flag) > 0 THEN 1 ELSE 0 END AS comment_add_flag,
    CASE WHEN SUM(comment_view_flag) > 0 THEN 1 ELSE 0 END AS comment_view_flag,
    CASE WHEN SUM(share_flag) > 0 THEN 1 ELSE 0 END AS share_flag,
    last_action_ts
    FROM 
    (
    SELECT
    a.accountid AS accountid,
    a.feed_id AS feed_id,
    CASE WHEN a.ei = 'feed_read' THEN 1 ELSE 0 END AS read_flag,
    CASE WHEN a.ei = 'feed_read' THEN a.time ELSE 0 END AS feed_read_time,
    CASE WHEN a.ei = 'praise' THEN 1 ELSE 0 END AS zan_flag,
    CASE WHEN a.ei = 'big_picture' THEN 1 ELSE 0 END AS big_picture_flag,
    CASE WHEN a.ei = 'comment_add' THEN 1 ELSE 0 END AS comment_add_flag,
    CASE WHEN a.ei = 'comment_view' THEN 1 ELSE 0 END AS comment_view_flag,
    CASE WHEN a.ei = 'share' THEN 1 ELSE 0 END AS share_flag,
    b.last_action_ts AS last_action_ts
    FROM 
    (
    SELECT
    accountid,
    feed_id,
    ei,
    SUM(du) AS time
    FROM 
    (
    SELECT
    get_json_object(kv,'$.account') AS accountid,
    get_json_object(kv,'$.feed_id') AS feed_id,
    ei ,
    ts,
    du
    FROM teg_mta_intf::ieg_youxishengjing WHERE sdate >= 20160401 AND sdate <= 20160427 AND ei IN ( 'feed_read','praise','big_picture','comment_add','comment_view','share')
    )t
    WHERE accountid IS NOT NULL AND accountid != '0' AND feed_id IS NOT NULL AND feed_id != '0'
    GROUP BY accountid,feed_id,ei
    )a 
    
    JOIN
    
    (
    SELECT
    accountid,
    feed_id,
    MAX(ts) AS last_action_ts
    FROM 
    (
    SELECT
    get_json_object(kv,'$.account') AS accountid,
    get_json_object(kv,'$.feed_id') AS feed_id,
    ei AS saction,
    ts,
    du
    FROM teg_mta_intf::ieg_youxishengjing WHERE sdate >= 20160401 AND sdate <= 20160427 AND ei IN ( 'feed_read','praise','big_picture','comment_add','comment_view','share')
    )t
    WHERE accountid IS NOT NULL AND accountid != '0' AND feed_id IS NOT NULL AND feed_id != '0'
    GROUP BY accountid,feed_id
    )b
    ON(a.accountid = b.accountid AND a.feed_id = b.feed_id)
    )c 
    GROUP BY accountid,feed_id,last_action_ts """
    '''
    
    
    sql = """ DROP TABLE tb_ydz_app_recommend_sys_usr_food_temp_%(sDate)s """%({"sDate":sDate})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    CREATE TABLE IF NOT EXISTS tb_ydz_app_recommend_sys_usr_food_temp_%(sDate)s
    (
    accountid BIGINT COMMENT '用户账号ID',
    feed_id INT COMMENT '用户操作的feedid',
    read_flag INT COMMENT '用户是否阅读的标记',
    feed_read_time INT COMMENT '用户阅读时长，现在没有写-1，确认这里有数据之后再使用该字段',
    zan_flag INT COMMENT '用户是否点赞的标记',
    big_picture_flag INT COMMENT '用户是否点击大图的标记',
    comment_add_flag INT COMMENT '是否添加评论的标记',
    comment_view_flag INT COMMENT '评论是否被查看的标记',
    share_flag INT COMMENT 'feed是否被分享的标记',
    last_aciton_ts BIGINT COMMENT '最后一次操作的时间戳'
    ) 
    """%({"sDate":sDate})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ###临时数据写入
    sql = """
    INSERT OVERWRITE TABLE tb_ydz_app_recommend_sys_usr_food_temp_%(sDate)s
    SELECT 
    accountid,
    feed_id,
    CASE WHEN SUM(read_flag) > 0 THEN 1 ELSE 0 END AS read_flag,
    SUM(feed_read_time)  AS feed_read_time,
    CASE WHEN SUM(zan_flag) > 0 THEN 1 ELSE 0 END AS zan_flag,
    CASE WHEN SUM(big_picture_flag) > 0 THEN 1 ELSE 0 END AS big_picture_flag,
    CASE WHEN SUM(comment_add_flag) > 0 THEN 1 ELSE 0 END AS comment_add_flag,
    CASE WHEN SUM(comment_view_flag) > 0 THEN 1 ELSE 0 END AS comment_view_flag,
    CASE WHEN SUM(share_flag) > 0 THEN 1 ELSE 0 END AS share_flag,
    last_action_ts
    FROM 
    (
    SELECT
    a.accountid AS accountid,
    a.feed_id AS feed_id,
    CASE WHEN a.ei = 'feed_read' THEN 1 ELSE 0 END AS read_flag,
    CASE WHEN a.ei = 'feed_read' THEN a.time ELSE 0 END AS feed_read_time,
    CASE WHEN a.ei = 'praise' THEN 1 ELSE 0 END AS zan_flag,
    CASE WHEN a.ei = 'big_picture' THEN 1 ELSE 0 END AS big_picture_flag,
    CASE WHEN a.ei = 'comment_add' THEN 1 ELSE 0 END AS comment_add_flag,
    CASE WHEN a.ei = 'comment_view' THEN 1 ELSE 0 END AS comment_view_flag,
    CASE WHEN a.ei = 'share' THEN 1 ELSE 0 END AS share_flag,
    b.last_action_ts AS last_action_ts
    FROM 
    (
    SELECT
    accountid,
    feed_id,
    ei,
    SUM(du) AS time
    FROM 
    (
    SELECT
    get_json_object(kv,'$.account') AS accountid,
    get_json_object(kv,'$.feed_id') AS feed_id,
    ei ,
    ts,
    du
    FROM teg_mta_intf::ieg_youxishengjing WHERE sdate =  %(sDate)s AND ei IN ( 'feed_read','praise','big_picture','comment_add','comment_view','share')
    )t
    WHERE accountid IS NOT NULL AND accountid != '0' AND feed_id IS NOT NULL AND feed_id != '0'
    GROUP BY accountid,feed_id,ei
    )a 
    
    JOIN
    
    (
    SELECT
    accountid,
    feed_id,
    MAX(ts) AS last_action_ts
    FROM 
    (
    SELECT
    get_json_object(kv,'$.account') AS accountid,
    get_json_object(kv,'$.feed_id') AS feed_id,
    ei AS saction,
    ts,
    du
    FROM teg_mta_intf::ieg_youxishengjing WHERE sdate =  %(sDate)s AND ei IN ( 'feed_read','praise','big_picture','comment_add','comment_view','share')
    )t
    WHERE accountid IS NOT NULL AND accountid != '0' AND feed_id IS NOT NULL AND feed_id != '0'
    GROUP BY accountid,feed_id
    )b
    ON(a.accountid = b.accountid AND a.feed_id = b.feed_id)
    )c 
    GROUP BY accountid,feed_id,last_action_ts 
    """%({"sDate":sDate})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##求得最终结果
    sql = """
    INSERT TABLE tb_ydz_app_recommend_sys_usr_food 

    SELECT
    %(sDate)s AS dtstatdate,
    accountid,
    feed_id,
    CASE WHEN SUM(read_flag) > 0 THEN 1 ELSE 0 END AS read_flag,
    SUM(feed_read_time)  AS feed_read_time,
    CASE WHEN SUM(zan_flag) > 0 THEN 1 ELSE 0 END AS zan_flag,
    CASE WHEN SUM(big_picture_flag) > 0 THEN 1 ELSE 0 END AS big_picture_flag,
    CASE WHEN SUM(comment_add_flag) > 0 THEN 1 ELSE 0 END AS comment_add_flag,
    CASE WHEN SUM(comment_view_flag) > 0 THEN 1 ELSE 0 END AS comment_view_flag,
    CASE WHEN SUM(share_flag) > 0 THEN 1 ELSE 0 END AS share_flag,
    MAX(last_aciton_ts)
    FROM 
    (
    SELECT 
    accountid,
    feed_id,
    read_flag,
    feed_read_time,
    zan_flag,
    big_picture_flag,
    comment_add_flag,
    comment_view_flag,
    share_flag,
    last_aciton_ts
    FROM 
    tb_ydz_app_recommend_sys_usr_food_temp_%(sDate)s a
    
    UNION ALL
    
    
    SELECT
    accountid,
    feed_id,
    read_flag,
    feed_read_time,
    zan_flag,
    big_picture_flag,
    comment_add_flag,
    comment_view_flag,
    share_flag,
    last_aciton_ts
    FROM tb_ydz_app_recommend_sys_usr_food PARTITION (p_%(pre_date_str)s) b
    
    ) c
    
    GROUP BY accountid,feed_id
     """%({"sDate":sDate,"pre_date_str":pre_date_str})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ DROP TABLE tb_ydz_app_recommend_sys_usr_food_temp_%(sDate)s """%({"sDate":sDate})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    