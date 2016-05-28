#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statiics_feed_write_server_data.py
# 功能描述:     油点赞数据统计——server统计的feed发表数据
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_app_data_write_feed_server_data
# 数据源表:     ieg_qt_community_app.tb_ydz_feed_feature_witdh_table
# 创建人名:     llianli
# 创建日期:     2016-05-11
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
    CREATE TABLE IF NOT EXISTS tb_ydz_app_data_write_feed_server_data
    (
    dtstatdate INT COMMENT '统计时间',
    vostype STRING COMMENT '操作系统类型，android ios -100',
    machine_type INT COMMENT '是否是机器人账户 0 不是 1 是 -100全部',
    total_feed BIGINT COMMENT '当日发表的总的帖子数',
    big_character_poster_feed BIGINT COMMENT '含有大字报的帖子数',
    phiz_pkg_id_feed BIGINT COMMENT '带有表情包的帖子数',
    bubble_feed BIGINT COMMENT '带有气泡的帖子数',
    common_tag_feed BIGINT COMMENT '带有普通标签的帖子数',
    game_tag_feed BIGINT COMMENT '带有游戏标签的帖子数',
    has_pic_feed BIGINT COMMENT '带有图片的帖子数',
    outer_url_feed BIGINT COMMENT '外部链接的帖子数'
    )
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    DELETE FROM tb_ydz_app_data_write_feed_server_data WHERE dtstatdate = %s
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    INSERT TABLE tb_ydz_app_data_write_feed_server_data
    SELECT
    %s AS dtstatdate,
    CASE WHEN GROUPING(vostype) = 1 THEN '-100' ELSE vostype END AS vostype,
    CASE WHEN GROUPING(machine_type) = 1 THEN -100 ELSE machine_type END AS machine_type,
    COUNT(DISTINCT feed_id) AS total_feed,
    COUNT(DISTINCT big_character_poster_flag) AS big_character_poster_feed,
    COUNT(DISTINCT phiz_pkg_id_flag) AS phiz_pkg_id_feed,
    COUNT(DISTINCT bubble_flag) AS bubble_feed,
    COUNT(DISTINCT common_tag_flag) AS common_tag_feed,
    COUNT(DISTINCT game_tag_flag) AS game_tag_feed,
    COUNT(DISTINCT has_pic_flag) AS has_pic_feed,
    COUNT(DISTINCT outer_url_flag) AS outer_url_feed
   
    FROM
    (
    SELECT
    feed_id,
    vostype,
    machine_type ,
    CASE WHEN phiz_type = 1 THEN feed_id ELSE NULL  END AS big_character_poster_flag,
    CASE WHEN pkg_id > 0 THEN feed_id ELSE NULL END AS phiz_pkg_id_flag,
    CASE WHEN phiz_type = 3 THEN feed_id ELSE NULL END AS bubble_flag,
    CASE WHEN tag_gameid <= 0 THEN feed_id ELSE NULL END AS common_tag_flag,
    CASE WHEN tag_gameid > 0 THEN feed_id ELSE NULL END AS game_tag_flag,
    CASE WHEN has_pic_flag = 1 THEN feed_id ELSE NULL END AS has_pic_flag,
    CASE WHEN feed_type =1  THEN feed_id ELSE NULL END AS outer_url_flag
    FROM tb_ydz_feed_feature_witdh_table PARTITION (p_%s) a WHERE iret = 0
    )t
    GROUP BY cube(vostype,machine_type)
     """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    