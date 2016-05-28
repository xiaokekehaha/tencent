#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_statics_tag_feednum_data.py
# 功能描述:     油点赞数据统计——每种标签下新增帖子数量统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_app_tag_new_feed_data
# 数据源表:     ieg_qt_community_app.tb_ydz_feed_feature_witdh_table
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
    CREATE TABLE IF NOT EXISTS tb_ydz_app_tag_new_feed_data
    (
    dtstatdate INT COMMENT '统计日期',
    tag_id INT COMMENT '标签ID',
    feed_num BIGINT COMMENT '对应标签下新增了多少feed'
    )

    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    DELETE FROM tb_ydz_app_tag_new_feed_data WHERE dtstatdate = %s
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
   
    
    sql = """
    INSERT TABLE tb_ydz_app_tag_new_feed_data
    SELECT 
    %s AS dtstatdate,
    tag_id,
    COUNT(DISTINCT feed_id) feed_num 
    FROM tb_ydz_feed_feature_witdh_table WHERE dtstatdate = %s
    GROUP BY tag_id

     """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    