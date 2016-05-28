#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     ydz_app_recommend_sys_feed_width_table.py
# 功能描述:     游点赞推荐系统数据统计—帖子宽表建立
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_ydz_feed_feature_witdh_table
# 数据源表:     teg_mta_intf.ieg_youxishengjing
# 创建人名:     llianli
# 创建日期:     2016-05-10
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
    CREATE TABLE IF NOT EXISTS tb_ydz_feed_feature_witdh_table
    (
    dtstatdate INT COMMENT '统计日期，分区字段',
    isybid INT COMMENT '用户账户ID',
    iaccounttype INT COMMENT '用户登录账户类型',
    feed_id INT COMMENT '帖子信息',
    vostype STRING COMMENT '用户的操作系统类型',
    iret INT COMMENT '操作结果 0 成功 非 0 失败',
    dteventtime STRING COMMENT '发表时间',
    
    content STRING COMMENT '帖子内容',
    outer_url STRING COMMENT '外部链接',
    machine_type INT COMMENT '是否是机器人账户',
    feed_type INT COMMENT '帖子类型 0-图文  1-外链',
    has_pic_flag INT COMMENT '是否有图片 0-没有 1-有',
    pic_height STRING COMMENT '图片高度',
    pic_width STRING COMMENT '图片宽度',
    pic_url STRING COMMENT '图片链接',
    pic_outlint_pic_url STRING COMMENT '图片外部链接',
    
    has_tag_flag INT COMMENT '是否有标签',
    tag_id INT COMMENT '标签ID',
    tag_name STRING COMMENT '标签名称',
    tag_gameid STRING COMMENT '游戏标签ID',
    tag_source STRING COMMENT '标签来源 0-选择标签，1-选择游戏',
    
    
    has_phiz_flag INT COMMENT '是否有表情',
    phiz_content STRING COMMENT '表情内容',
    phiz_type INT COMMENT '表情类型',
    phiz_id INT COMMENT '表情ID',
    pkg_id INT COMMENT '表情包ID'
    )
    PARTITION BY LIST (dtstatdate)
    (
                PARTITION p_20160427  VALUES IN (20160427),
                PARTITION p_20160428  VALUES IN (20160428)
    )
    
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    ALTER TABLE tb_ydz_feed_feature_witdh_table DROP PARTITION (p_%(sDate)s)
     """%({"sDate":sDate})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    ALTER TABLE tb_ydz_feed_feature_witdh_table ADD PARTITION p_%(sDate)s VALUES IN (%(sDate)s)
     """%({"sDate":sDate})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    ##求得最终结果
    sql = """
    INSERT  TABLE tb_ydz_feed_feature_witdh_table

    SELECT
    %(sDate)s AS dtstatdate,
    isybid,
    iaccounttype,
    feed_id,
    vostype,
    iret,
    dteventtime,
    content,
    outer_url,
    machine_type,
    feed_type,
    CASE WHEN pic_info != '' THEN 1 ELSE 0 END AS has_pic_flag,
    get_json_object(pic_info,'$.height') AS pic_height,
    get_json_object(pic_info,'$.width') AS pic_width,
    get_json_object(pic_info,'$.url') AS pic_url,
    get_json_object(pic_info,'$.outlint_pic_url') AS pic_outlint_pic_url,
    
    CASE WHEN tag_info != '' THEN 1 ELSE 0 END AS has_tag_flag,
    get_json_object(tag_info,'$.id') AS tag_id,
    get_json_object(tag_info,'$.name') AS tag_name,
    get_json_object(tag_info,'$.gameid') AS tag_gameid,
    get_json_object(tag_info,'$.source') AS tag_source,
    
    
    CASE WHEN phiz_info != '' THEN 1 ELSE 0 END AS has_phiz_flag,
    get_json_object(phiz_info,'$.content') AS phiz_content,
    get_json_object(phiz_info,'$.phiz_type') AS phiz_type,
    get_json_object(phiz_info,'$.phiz_id') AS phiz_id,
    get_json_object(phiz_info,'$.pkg_id') AS pkg_id
    
    FROM 
    (
    SELECT
    isybid,
    iaccounttype,
    feed_id,
    vostype,
    iret,
    content,
    outer_url,
    machine_type,
    feed_type,
    dteventtime,
    pic_info,
    tag_info,
    phiz_info
    FROM
    (
    SELECT
    isybid,
    iaccounttype,
    feed_id,
    vostype,
    iret,
    content,
    outer_url,
    machine_type,
    feed_type,
    dteventtime,
    split(regexp_replace(pic_list,'\\\\},\\\\{','\\\\},,,,,\\\\{') ,',,,,,')   AS pic_list,
    split(regexp_replace(tag_list,'\\\\},\\\\{','\\\\},,,,,\\\\{') ,',,,,,')   AS tag_list,
    split(regexp_replace(phiz_list,'\\\\},\\\\{','\\\\},,,,,\\\\{') ,',,,,,')   AS phiz_list
    FROM
    (
    SELECT
    isybid,
    iaccounttype,
    feed_id,
    vostype,
    iret,
    content,
    outer_url,
    machine_type,
    feed_type,
    dteventtime,
    CASE WHEN pic_list IS NOT NULL AND regexp_instr(pic_list,'\\\\[') != 0 THEN regexp_replace(pic_list, '\\\\[|\\\\]', '') ELSE '' END AS pic_list,
    CASE WHEN tag_list IS NOT NULL AND regexp_instr(tag_list,'\\\\[') != 0 THEN regexp_replace(tag_list, '\\\\[|\\\\]', '') ELSE '' END AS tag_list,
    CASE WHEN phiz_list IS NOT NULL AND regexp_instr(phiz_list,'\\\\[') != 0 THEN regexp_replace(phiz_list, '\\\\[|\\\\]', '') ELSE '' END AS phiz_list
    FROM  
    (
    SELECT
    isybid,
    iaccounttype,
    ik1 AS feed_id,
    lower(vostype) AS vostype,
    iret,
    vv4 AS content,
    vv5 AS outer_url,
    ik2 AS machine_type,
    ik3 AS feed_type,
    dteventtime,
    get_json_object(vv1,'$.pic_list') AS pic_list,
    get_json_object(vv2,'$.tag') AS tag_list,
    get_json_object(vv3,'$.phiz_list') AS phiz_list
    FROM ieg_tdbank::gqq_dsl_gamebible_day_task_bill_fht0 
    WHERE tdbank_imp_date BETWEEN '%(sDate)s00' AND '%(sDate)s23' AND   iactiontype = 3 AND iactionid = 1
    )t
    )tmp
    )t1
    LATERAL VIEW  explode(pic_list) a AS pic_info
    LATERAL VIEW  explode(tag_list) b AS tag_info
    LATERAL VIEW  explode(phiz_list) b AS phiz_info
    )t2
     """%({"sDate":sDate,"pre_date_str":pre_date_str})
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    


    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    