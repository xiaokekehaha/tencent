#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     cf_app_search_shike_statics.py
# 功能描述:     掌上穿越火线——资讯搜索、收藏、火线时刻模块数据统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_cf_app_search_fire_moment_statics
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-02-26
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
    ##sDate = '20150111'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS tb_cf_app_search_fire_moment_statics
            (
            sdate INT COMMENT '统计时间',
            id BIGINT COMMENT 'appid 安卓1100679031，ios 1200679031',
            ei STRING COMMENT '上报事件',
            pv BIGINT COMMENT '事件的PV',
            uv BIGINT COMMENT '事件的点击人数（mac地址统计）',
            uin_cnt BIGINT COMMENT '事件的点击人数（qq号统计）',
            time BIGINT COMMENT '事件总时长'
            )  """
    res = tdw.execute(sql)

    sql="""DELETE  FROM tb_cf_app_search_fire_moment_statics WHERE  sdate=%s  """ % (sDate)
    res = tdw.execute(sql)


    sql = """
            insert  table tb_cf_app_search_fire_moment_statics
            
            select 
            sdate,
            CASE WHEN GROUPING(id) = 1 THEN -100 ELSE id END AS id,
            ei,
            count(*) as pv,
            count(distinct uin_mac) as uv ,
            COUNT(DISTINCT uin) AS uin_cnt,
            SUM(time) AS time
            from 
            (
            SELECT
            sdate, 
            id,
            concat(ui,mc) as uin_mac,
            get_json_object(kv,'$.uin') AS uin,
            du AS time,
            
            case 
                when (id = 1100679031 and ei = ('火线_视频点击次数') )   or 
                     (id = 1200679031 and ei = ('火线时刻视频点击次数') )
                then '火线时刻-视频播放页访问'
                
                
                when (id = 1100679031 and ei = ('火线_视频播放页面时长') )   or 
                     (id = 1200679031 and ei = ('火线_视频播放页面时长') )
                then '火线时刻-视频播放'
                  
                
                
                when (id = 1100679031 and ei = ('火线_查看关注人视频次数') )   or 
                     (id = 1200679031 and ei = ('火线时刻查看关注作者视频次数')  )
                then '火线时刻-关注视频播放'
                
                
                
                when (id = 1100679031 and ei = ('火线_关注点击次数') )   or 
                     (id = 1200679031 and ei IN ('火线时刻关注量','火线时刻查看关注作者视频次数')  )
                then '火线时刻-关注行为'
                
                when (id = 1100679031 and ei = ('火线_取消关注点击次数') )   or 
                     (id = 1200679031 and ei = ('火线_取消关注点击次数')  )
                then '火线时刻-取消关注行为'
                

                
                
                when (id = 1100679031 and ei = ('资讯搜索_搜索入口点击')  )   or 
                     (id = 1200679031 and ei = ('资讯搜索_搜索入口点击') )
                then '资讯搜索-搜索入口点击'
                
                when (id = 1100679031 and ei = ('资讯搜索_搜索行为统计')  )   or 
                     (id = 1200679031 and ei = ('资讯搜索_搜索行为统计') )
                then '资讯搜索_搜索行为统计'
                
                when (id = 1100679031 and ei = ('资讯搜索_搜索结果点击')  )   or 
                     (id = 1200679031 and ei = ('资讯搜索_搜索结果点击') )
                then '资讯搜索_搜索结果点击'
                

                when (id = 1100679031 and ei = ('资讯收藏_收藏按钮点击') )   or 
                     (id = 1200679031 and ei = ('资讯收藏_收藏按钮点击') )
                then '资讯收藏-客户端收藏'
                
                
            END  AS ei
             
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031) AND du < 5*60*60

            ) where ei is not null
            group by sdate, cube(id), ei
            
            
            UNION ALL 
            
            
            
            
            select 
            sdate,
            CASE WHEN GROUPING(id) = 1 THEN -100 ELSE id END AS id,
            ei,
            count(*) as pv,
            count(distinct uin_mac) as uv ,
            COUNT(DISTINCT uin) AS uin_cnt,
            SUM(time) AS time
            from 
            (
            SELECT
            sdate, 
            id,
            concat(ui,mc) as uin_mac,
            get_json_object(kv,'$.uin') AS uin,
            du AS time,
            
            case 
             when (id = 1100679031 and ei = ('火线时刻视频点击次数')  AND get_json_object(kv,'$.from') = '推荐轮播图')   or 
                     (id = 1200679031 and ei = ('火线时刻视频点击次数') AND get_json_object(kv,'$.type') = '推荐轮播图' )
                then '火线时刻-播放渠道-轮播图'
                
                
                when (id = 1100679031 and ei = ('火线时刻视频点击次数')  AND get_json_object(kv,'$.from') = '推荐首页推荐')   or 
                     (id = 1200679031 and ei = ('火线时刻视频点击次数') AND get_json_object(kv,'$.type') = '推荐首页推荐' )
                then '火线时刻-播放渠道-首页推荐'
                
                
                when (id = 1100679031 and ei = ('火线时刻视频点击次数')  AND get_json_object(kv,'$.from') = '推荐综合推荐')   or 
                     (id = 1200679031 and ei = ('火线时刻视频点击次数') AND get_json_object(kv,'$.type') = '推荐综合推荐' )
                then '火线时刻-播放渠道-综合推荐'
                
                
                when (id = 1100679031 and ei = ('火线时刻视频点击次数')  AND get_json_object(kv,'$.from') = '好友')   or 
                     (id = 1200679031 and ei = ('火线时刻视频点击次数') AND get_json_object(kv,'$.type') = '好友' )
                then '火线时刻-播放渠道-好友'
                
                
                when (id = 1100679031 and ei = ('火线时刻视频点击次数')  AND get_json_object(kv,'$.from') = '我的客态')   or 
                     (id = 1200679031 and ei = ('火线时刻视频点击次数') AND get_json_object(kv,'$.type') = '我的客态' )
                then '火线时刻-播放渠道-我的客态'
                
                
                when (id = 1100679031 and ei = ('火线时刻视频点击次数')  AND get_json_object(kv,'$.from') = '我的主态')   or 
                     (id = 1200679031 and ei = ('火线时刻视频点击次数') AND get_json_object(kv,'$.type') = '我的主态' )
                then '火线时刻-播放渠道-我的主态'
                
                
                 when (id = 1100679031 and ei = ('资讯搜索_搜索行为统计') AND get_json_object(kv,'$.type') = '历史' )   or 
                     (id = 1200679031 and ei = ('资讯搜索_搜索行为统计') AND get_json_object(kv,'$.type') = '历史')
                then '资讯搜索-历史记录'
                
                
                when (id = 1100679031 and ei = ('资讯搜索_搜索行为统计') AND get_json_object(kv,'$.type') = '关联词' )   or 
                     (id = 1200679031 and ei = ('资讯搜索_搜索行为统计') AND get_json_object(kv,'$.type') = '关联词')
                then '资讯搜索-关联词'
                
                
                when (id = 1100679031 and ei = ('资讯搜索_搜索行为统计') AND get_json_object(kv,'$.type') = '热词' )   or 
                     (id = 1200679031 and ei = ('资讯搜索_搜索行为统计') AND get_json_object(kv,'$.type') = '热词')
                then '资讯搜索-热词'
                
                
                when (id = 1100679031 and ei = ('资讯搜索_搜索行为统计') AND get_json_object(kv,'$.type') = '输入' )   or 
                     (id = 1200679031 and ei = ('资讯搜索_搜索行为统计') AND get_json_object(kv,'$.type') = '输入')
                then '资讯搜索-手动输入'
                
                
                when (id = 1100679031 and ei = ('火线_关注点击次数') AND get_json_object(kv,'$.from') = '个人视频页' )   or 
                     (id = 1200679031 and ei = ('火线时刻个人视频页关注量') )
                then '火线时刻-个人视频页关注量'
                
                when (id = 1100679031 and ei = ('火线_关注点击次数') AND get_json_object(kv,'$.from') = '关注TAB' )   or 
                     (id = 1200679031 and ei = ('火线时刻关注量') )
                then '火线时刻-TAB关注量'
                
                
                
            END  AS ei
             
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100679031,1200679031) AND du < 5*60*60

            ) where ei is not null
            group by sdate, cube(id), ei
            
            
            """ % (sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    ##关键词搜索统计
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_cf_app_search_keyword_staics
    (
    dtstatdate INT COMMENT '统计时间',
    id INT COMMENT 'appid 安卓1100679031，ios 1200679031',
    skeyword STRING COMMENT '搜索关键词',
    pv BIGINT COMMENT '总搜索量',
    uv BIGINT COMMENT '搜索设备好数信息',
    uin_cnt BIGINT COMMENT '搜索的UIN数信息'
    ) '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = ''' DELETE FROM tb_cf_app_search_keyword_staics WHERE dtstatdate = %s ''' %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = ''' 
    INSERT TABLE tb_cf_app_search_keyword_staics 
    SELECT
    %s AS dtstatdate,
    -100 AS id,
    skeyword,
    COUNT(*) AS pv,
    COUNT(DISTINCT uin_mac) AS uv,
    COUNT(DISTINCT uin) AS uin_cnt
    FROM
    (
    SELECT 
    id,
    concat(ui,mc) AS uin_mac,
    get_json_object(kv,'$.uin') AS uin,
    get_json_object(kv,'$.word') AS skeyword
    FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND id IN (1100679031,1200679031)
    AND ei = '资讯搜索_搜索行为统计'
    )t
    GROUP BY skeyword
''' %(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    
    
    