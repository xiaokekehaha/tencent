#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     cf_app_function_stay_rate.py
# 功能描述:     掌火相关功能留存率数据统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     teg_mta_intf::ieg_lol 
# 创建人名:     llianli
# 创建日期:     2015-10-29
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime

def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_7_date = today_date - datetime.timedelta(days = 7)
    pre_7_date_str = pre_7_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，统计的原始表
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_cf_app_function_stay_uin_original_data
    (
    dtstatdate INT COMMENT '统计日期',
    sdate INT COMMENT '计算的数据日期',
    id INT COMMENT 'APPId',
    uin_info STRING COMMENT '用户信息',
    ut INT COMMENT '用户是否是新用户的标记',
    pi STRING COMMENT '功能描述'
    )PARTITION BY LIST (dtstatdate)
                (
                partition p_20151027  VALUES IN (20151027),
                partition p_20151028  VALUES IN (20151028)
                ) '''
            
    res = tdw.execute(sql)


    sql=''' ALTER TABLE   tb_cf_app_function_stay_uin_original_data DROP PARTITION (p_%s)''' % (today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' ALTER TABLE tb_cf_app_function_stay_uin_original_data ADD PARTITION p_%s VALUES IN (%s)'''%(today_str,today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##处理数据写入这个表中用以进行计算
    sql = ''' 
    INSERT TABLE tb_cf_app_function_stay_uin_original_data
        SELECT 
        %s AS dtstatdate,
        sdate,
        id,
        uin_info,
        ut,
        pi
        FROM 
        (
        SELECT 
        sdate,
        id,
        concat(ui,mc) AS uin_info,
        ut,
        case
                when (id = 1100679031 and ei  in ('情报站列表项点击','资讯广告点击','视频播放次数') ) or (id = 1200679031 and ei = '情报站列表项') then '情报站'
                when (id = 1100679031 and ei  in  ('查看评论点击次数' ,'写评论点击次数')) or (id = 1200679031 and ei in ( '进入评论页面','发表评论')) then '评论'
                
                when (id = 1100679031 and ei = '我模块点击次数' ) or (id = 1200679031 and ei = '情报站社区基地我TAB点击次数' and get_json_object(kv,'$.type') = '我') then '我-战绩'
                when (id = 1100679031 and ei = '我_战绩资产记录展示次数' and get_json_object(kv,'$.tab') = '装备') or (id = 1200679031 and ei = '战绩资产记录TAB点击次数' and get_json_object(kv,'$.type') = '资产') then '我-资产'
                when (id = 1100679031 and ei = '我_战绩资产记录展示次数' and get_json_object(kv,'$.tab') = '记录') or (id = 1200679031 and ei = '战绩资产记录TAB点击次数' and get_json_object(kv,'$.type') = '记录') then '我-记录'
                
                when (id = 1100679031 and ei = '客态资料' ) then '客态资料'
                
                when (id = 1100679031 and ei = '道聚城点击次数') or (id = 1200679031 and ei = '道具城点击') then '道聚城'
                when (id = 1100679031 and ei = '火线_视频点击次数') or (id = 1200679031 and ei = '火线时刻视频点击次数') then '火线时刻'
                
                when (id = 1100679031 and ei = '我的仓库点击' ) or (id = 1200679031 and ei = '我的仓库点击') then '我的仓库'
                
                when (id = 1100679031 and ei = '军火基地点击次' ) or (id = 1200679031 and ei = '军火基地点击次') then '军火基地'
                
                when (id = 1100679031 and ei= '基地WEB页面点击次数' and get_json_object(kv,'$.title') = '周边商城') then '周边商城'
                
                when (id = 1100679031 and ei = '竞猜大厅入口' ) or (id = 1200679031 and ei = '竞猜大厅入口点击次数') then '赛事竞猜'
                
                
                when (id = 1100679031 and ei = '火线百科点击次数' ) or (id = 1200679031 and ei = '火线百科点击') then '火线百科' 
                when (id = 1100679031 and ei = '火线助手点击次数' ) or (id = 1200679031 and ei = '火线助手') then '火线助手'
                
                when (id = 1100679031 and ei = '我的任务点击次数' ) or (id = 1200679031 and ei = '我的任务点击') then '我的任务'
                when (id = 1100679031 and ei = '地图点位模块点击次数' ) or (id = 1200679031 and ei = '地图点图') then '地图点位'
                when (id = 1100679031 and ei in ('每天用户发的消息' ,'每天用户发的消息')) then '聊天'
                when (id = 1100679031 and ei = '社区_CF论坛点击次数' ) or (id = 1200679031 and ei = 'CF论坛点击') then 'CF论坛'
                when (id = 1100679031 and ei = '社区_CF手游论坛点击次数' ) or (id = 1200679031 and ei = '点击CF手游论坛') then 'CF手游论坛'
                when (id = 1100679031 and ei = '社区_兴趣部落点击次数' ) or (id = 1200679031 and ei = 'CF兴趣部落') then '兴趣部落'

                else 'other'
            end as pi
        
        FROM  teg_mta_intf::ieg_lol WHERE sdate > %s AND sdate <= %s AND id in (1100679031,1200679031)
        )t WHERE pi IS NOT NULL and pi != 'other'
        GROUP BY sdate,id,uin_info,pi,ut
    '''%(sDate,pre_7_date_str,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##统计每日的活跃新增数据
    sql = '''CREATE TABLE IF NOT EXISTS tb_cf_app_function_useract
        (
        dtstatdate INT COMMENT '统计日期',
        id INT COMMENT '应用id',
        pi STRING COMMENT '功能',
        sflag STRING COMMENT '数据类型（act:活跃用户数，reg:注册用户数）',
        uin_num  INT COMMENT '用户数'
        ) '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = ''' DELETE FROM tb_cf_app_function_useract WHERE dtstatdate = %s'''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##将活跃新增数据写入表中
    sql = '''
    INSERT TABLE tb_cf_app_function_useract
      SELECT 
      %s AS dtstatdate,
      id,
      pi,
      sflag,
      COUNT(DISTINCT uin_info) AS uin_num
      FROM 
      (
      SELECT
      id,
      pi,
      CASE WHEN ut = 0 THEN 'reg' ELSE 'act' END AS sflag,
      uin_info
      FROM tb_cf_app_function_stay_uin_original_data WHERE dtstatdate = %s AND sdate = %s AND pi IS NOT NULL
      )t
      GROUP BY id,pi,sflag 
    '''%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##统计留存数据
    sql = ''' 
    CREATE TABLE IF NOT EXISTS tb_cf_app_function_stayscale
     (
     dtstatdate INT COMMENT '统计日期',
     deltadays INT COMMENT '相隔日期',
     sdate INT COMMENT '回溯的日期',
     id INT COMMENT '应用id',
     pi STRING COMMENT '功能',
     sflag STRING COMMENT '数据类型（act:活跃用户数，reg:注册用户数）',
     stay_uin_num INT COMMENT '留存用户数'
     )'''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = ''' DELETE FROM tb_cf_app_function_stayscale WHERE dtstatdate = %s'''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##将活跃留存跟踪数据写入表中
    sql = '''
          INSERT TABLE tb_cf_app_function_stayscale
      SELECT 
      %s AS dtstatdate,
      datediff(%s,t2.sdate) AS deladay,
      t2.sdate AS sdate,
      t1.id AS id,
      t1.pi AS pi,
      'act' AS sflag,
      COUNT(DISTINCT t1.uin_info) AS stay_uin
      FROM 
      (
      SELECT 
      DISTINCT
      id,
      pi,
      uin_info
      FROM tb_cf_app_function_stay_uin_original_data WHERE dtstatdate = %s AND sdate = %s AND pi is NOT NULL
      )t1
      JOIN
      (
      SELECT 
      DISTINCT
      sdate, 
      id,
      pi,
      uin_info
      FROM tb_cf_app_function_stay_uin_original_data WHERE dtstatdate = %s AND sdate < %s AND pi is NOT NULL
      )t2
      ON (t1.id = t2.id AND t1.pi = t2.pi AND t1.uin_info = t2.uin_info)
      GROUP BY t2.sdate,t1.id,t1.pi 
    '''%(sDate,sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##将新增留存跟踪数据写入表中
    sql = '''
      INSERT TABLE tb_cf_app_function_stayscale
      SELECT 
      %s AS dtstatdate,
      datediff(%s,t2.sdate) AS deladay,
      t2.sdate AS sdate,
      t1.id AS id,
      t1.pi AS pi,
      'reg' AS sflag,
      COUNT(DISTINCT t1.uin_info) AS stay_uin
      FROM 
      (
      SELECT 
      DISTINCT
      id,
      pi,
      uin_info
      FROM tb_cf_app_function_stay_uin_original_data WHERE dtstatdate = %s AND sdate = %s AND pi is NOT NULL
      )t1
      JOIN
      (
      SELECT 
      DISTINCT
      sdate, 
      id,
      pi,
      uin_info
      FROM tb_cf_app_function_stay_uin_original_data WHERE dtstatdate = %s AND sdate < %s AND ut = 0 AND pi is NOT NULL
      )t2
      ON (t1.id = t2.id AND t1.pi = t2.pi AND t1.uin_info = t2.uin_info)
      GROUP BY t2.sdate,t1.id,t1.pi
    '''%(sDate,sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##最后将七天之前的分区做一个删除
    sql = ''' alter table tb_cf_app_function_stay_uin_original_data DROP PARTITION (p_%s)  '''%(pre_7_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
     
    tdw.WriteLog("== end OK ==")
    
    
    
    