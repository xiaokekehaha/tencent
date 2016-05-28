#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     cfapp_ei.py
# 功能描述:     cfapp每日访问的事件n数目
# 输入参数:     yyyymmdd    例如：20151208
# 目标表名:     
# 数据源表:     teg_mta_intf::ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-12-08
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime
import time


def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表写数据
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_cf_app_ei
(
fdate INT,
id INT,
ei1 STRING,
ei2 STRING,
uin_mac STRING,
uin STRING,
pv BIGINT 
)
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql = '''  DELETE FROM tb_cf_app_ei WHERE  fdate = %s '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##将每日的数据配置写入表中
    sql = ''' 
     INSERT TABLE tb_cf_app_ei

SELECT 
%s AS fdate,
id,
ei1,
ei2,
uin_info,
uin,
COUNT(*) AS pv
FROM 
(
SELECT
id,

'all' AS ei1,
   
         
case
                when (id = 1100679031 and ei in ('情报站列表项点击')  and get_json_object(kv,'$.type') not in ('图片','手机','论坛','电脑','游戏'))   or 
                     (id = 1200679031 and ei in ('情报站列表项') and get_json_object(kv,'$.info_list') = '资讯列表项')
                then '情报站-资讯'
                
                
                
 
                when (id = 1100679031 and ( ei in ('视频播放次数')  or (ei = '资讯广告点击' and get_json_object(kv,'$.type') = '视频') ) )   or 
                     (id = 1200679031 and ei in ('情报站列表项') and get_json_object(kv,'$.info_list') = '视频列表项')
                then '情报站-视频'
                
                
 
                when (id = 1100679031 and ei in ('情报站列表项点击')  and get_json_object(kv,'$.type') ='图片')   or 
                     (id = 1200679031 and ei in ('情报站列表项') and get_json_object(kv,'$.info_list') = '图片列表项')
                then '情报站-图片'
                
                
                
 
                when (id = 1100679031 and ei in ('情报站列表项点击')  and get_json_object(kv,'$.type') in ('手机','电脑','论坛','游戏'))   or 
                     (id = 1200679031 and ei in ('情报站列表项') and get_json_object(kv,'$.info_list') = '活动列表项')
                then '情报站-活动'
                
                when (id = 1100679031 and ei = '我模块点击次数' ) or (id = 1200679031 and ei = '情报站社区基地我TAB点击次数' and get_json_object(kv,'$.type') = '我') then '我-战绩'
                when (id = 1100679031 and ei = '我_战绩资产记录展示次数' and get_json_object(kv,'$.tab') = '装备') or (id = 1200679031 and ei = '战绩资产记录TAB点击次数' and get_json_object(kv,'$.type') = '资产') then '我-资产'
                when (id = 1100679031 and ei = '我_战绩资产记录展示次数' and get_json_object(kv,'$.tab') = '记录') or (id = 1200679031 and ei = '战绩资产记录TAB点击次数' and get_json_object(kv,'$.type') = '记录') then '我-记录'
                
                
                when (id = 1100679031 and ei = '客态资料' ) then '客态资料'
                
                when (id = 1100679031 and ei = '道聚城点击次数') or (id = 1200679031 and ei = '道具城点击') then '基地-道聚城'
                when (id = 1100679031 and ei = '火线_视频点击次数') or (id = 1200679031 and ei = '火线时刻视频点击次数') then '基地-火线时刻'
                
                when (id = 1100679031 and ei = '我的仓库点击' ) or (id = 1200679031 and ei = '我的仓库点击') then '基地-我的仓库'
                
                when (id = 1100679031 and ei = '军火基地点击次' ) or (id = 1200679031 and ei = '军火基地点击次') then '基地-军火基地'
                
                when (id = 1100679031 and ei= '基地WEB页面点击次数' and get_json_object(kv,'$.title') = '周边商城') then '基地-周边商城'
                
                when (id = 1100679031 and ei = '竞猜大厅入口' ) or (id = 1200679031 and ei = '竞猜大厅入口点击次数') then '基地-赛事竞猜'
                
                
                when (id = 1100679031 and ei = '火线百科点击次数' ) or (id = 1200679031 and ei = '火线百科点击') then '基地-火线百科' 
                when (id = 1100679031 and ei = '火线助手点击次数' ) or (id = 1200679031 and ei = '火线助手') then '基地-火线助手'
                
                when (id = 1100679031 and ei = '我的任务点击次数' ) or (id = 1200679031 and ei = '我的任务点击') then '基地-我的任务'
                when (id = 1100679031 and ei = '地图点位模块点击次数' ) or (id = 1200679031 and ei = '地图点图') then '基地-地图点位'
                when (id = 1100679031 and ei in ('每天用户发的消息' ,'每天用户发的消息')) then '社区-聊天'
                when (id = 1100679031 and ei = '社区_CF论坛点击次数' ) or (id = 1200679031 and ei = 'CF论坛点击') then '社区-CF论坛'
                when (id = 1100679031 and ei = '社区_CF手游论坛点击次数' ) or (id = 1200679031 and ei = '点击CF手游论坛') then '社区-CF手游论坛'
                when (id = 1100679031 and ei = '社区_兴趣部落点击次数' ) or (id = 1200679031 and ei = 'CF兴趣部落') then '社区-兴趣部落'

            ELSE 'other'    
            end as ei2,
            
concat(ui,mc) AS uin_info,
get_json_object(kv,'$.uin') AS uin 
FROM  teg_mta_intf::ieg_lol WHERE sdate = %s  AND id in (1100679031,1200679031)
)t1 WHERE  ei1 != 'other' AND ei2 != 'other'
GROUP BY id,ei1,ei2,uin_info,uin                 

    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    