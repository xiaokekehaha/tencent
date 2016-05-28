#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_content_data.py
# 功能描述:     手游宝内容运营数据统计
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     ieg_qt_community_app.tb_syb_content_data_new
# 数据源表:     teg_mta_intf::ieg_shouyoubao 
# 创建人名:     llianli
# 创建日期:     2015-04-30
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
    partition_date = today_date + datetime.timedelta(days = 1)
    partition_date_str = partition_date.strftime("%Y%m%d")
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，手游宝内容运营数据
    sql = '''
            CREATE TABLE IF NOT EXISTS tb_syb_content_data_new
            (
            fdate int,
            id  string,
            game_id bigint,
            ei string,
            news_id string,
            pv bigint,
            uv bigint
            ) PARTITION BY RANGE (fdate)
            (
            partition p_20150801  VALUES LESS THAN (20150802),
            partition p_20150802  VALUES LESS THAN (20150803),
            partition p_20150803  VALUES LESS THAN (20150804),
            partition p_20150804  VALUES LESS THAN (20150805),
            partition p_20150805  VALUES LESS THAN (20150806)
            ) '''
            
    res = tdw.execute(sql)


    sql=''' alter table  tb_syb_content_data_new DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' alter table tb_syb_content_data_new ADD PARTITION p_%s VALUES LESS THAN (%s) '''%(sDate,partition_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##计算视频、话题的数据不区分游戏,不区分客户端类型，这里只计算话题和视频的数据，资讯的数据通过点击流做计算
    sql = ''' 
    insert table tb_syb_content_data_new
        SELECT 
        sdate,
        id,
        CASE WHEN GROUPING(game_id) = 1 THEN '-100' ELSE game_id END AS game_id,
        ei ,
        CASE WHEN GROUPING(news_id) = 1 THEN '-100' ELSE news_id END AS news_id,
        COUNT(*) as pv,
        COUNT(DISTINCT uin) as uv 
        FROM 
        (
        SELECT  
        sdate,
        -100 as id,
        CASE 
            WHEN  ei = 'VIDEO_ITEM_CLICK' THEN get_json_object(kv,'$.gameID')
            WHEN  ei = 'MGC_VIDEO_ITEM_CLICK' THEN get_json_object(kv,'$.gameid')
            WHEN  ei = 'TOPIC_ACT_ENTRY' THEN get_json_object(kv,'$.GameID') 
            WHEN  ei = 'TOPIC_VIEW_LOAD' THEN get_json_object(kv,'$.gameid')
            ELSE NULL
        END AS game_id,

        CASE 
            WHEN id = 1100679302 THEN get_json_object(kv,'$.uin')
            WHEN id = 1200679337 THEN concat(ui,mc)
            ELSE NULL
        END AS uin,
        
        CASE 
            WHEN (id = 1100679302 AND ei = 'VIDEO_ITEM_CLICK' ) OR (id = 1200679337 AND ei = 'MGC_VIDEO_ITEM_CLICK')  THEN 'video_item'
            WHEN (id = 1100679302 AND ei = 'TOPIC_ACT_ENTRY' ) OR (id = 1200679337 AND ei = 'TOPIC_VIEW_LOAD')  THEN 'topic_item'
            ELSE NULL
        END AS ei,
        
        CASE 
            WHEN ei = 'VIDEO_ITEM_CLICK' THEN  get_json_object(kv,'$.vid')
            WHEN ei = 'TOPIC_ACT_ENTRY' THEN  get_json_object(kv,'$.topicid')
            
            WHEN ei = 'MGC_VIDEO_ITEM_CLICK' THEN  get_json_object(kv,'$.videoId')
            WHEN ei = 'TOPIC_VIEW_LOAD' THEN  get_json_object(kv,'$.topicid')
            ELSE NULL
        END AS news_id
        FROM teg_mta_intf::ieg_shouyoubao  WHERE sdate = %s and cdate = %s 
        AND ei in ('VIDEO_ITEM_CLICK','TOPIC_ACT_ENTRY',
        'MGC_VIDEO_ITEM_CLICK','TOPIC_VIEW_LOAD')
        )t 
        WHERE ei IS NOT NULL AND game_id IS NOT NULL AND news_id IS NOT NULL
        GROUP BY sdate,id, ei,CUBE(game_id,news_id)
    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##论坛数据内容数据写入
    sql = """
    insert table  tb_syb_content_data_new
    SELECT 
    daytime,
    CASE WHEN GROUPING(plat) = 1 THEN '-100' ELSE plat END AS plat,
    '-100' as game_id,
    'bbs_item' as ei,
    CASE WHEN GROUPING(ti) = 1 THEN -100 ELSE ti END AS ti,
    count(*) as pv,
    count(DISTINCT pvi) as uv 
    from 
    (
    SELECT
    daytime,
    case 
    when dm = 'AGAME.QQ.COM' AND url = '/bbs_joy/topic.shtml' AND instr(ua,'GAMEJOY') = 0 then 'H5'
    when dm = 'AGAME.QQ.COM' AND url = '/bbs_joy/topic.shtml' AND instr(ua,'GAMEJOY') != 0 then 'APP'
    WHEN dm = 'BBS.G.QQ.COM' THEN 'PC' 
    else NULL
    end as plat,
    ti,
    pvi 
    FROM
    dw_ta::ta_log_bbs_g_qq_com 
    WHERE daytime = %s  AND   md = 'viewthread'
    )t WHERE plat IS NOT NULL GROUP  BY daytime,CUBE(plat,ti)
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
   

    
    ##资讯内容写入，资讯内容包括两块内容，一块内容是通过点击流获取的点击数据，这里先获取点击流数据
    ##这里表示所有的文章，但是有的文章是帖子
    sql = """
    insert table  tb_syb_content_data_new    
    SELECT 
    f_date,
    '-100'  AS plat,
    '-100' as game_id,
    'news_item_tcss' as ei,
    news_id,
    count(*) as pv,
    count(DISTINCT f_pvid) as uv 
    from 
    (
    SELECT
    f_date,
    split(split(f_url,'/syb/article/')[1],'.shtml')[0] as news_id,
    f_pvid 
    FROM
    teg_dw_tcss::tcss_qt_qq_com 
    WHERE f_date = %s  AND   f_url like '%%/syb/article/%%'
    )t GROUP  BY f_date,news_id
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##这里创建临时表，对论坛的帖子数据打标签，每一行扩展为帖子ID，资讯ID，专栏ID，论坛的PV UV
    sql = """
            CREATE TABLE IF NOT EXISTS tb_syb_content_data_bbs_content_temp
            (
            ei string,
            tid bigint,
            articleid bigint,
            zhuanlanid bigint,
            pv bigint,
            uv bigint
            ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##把论坛帖子的数据和配置表做一个交叉，从而得到论坛内容和资讯对应关系以及专栏的ID
    sql = '''
    insert overwrite table  tb_syb_content_data_bbs_content_temp
    select 
    'bbs_article_cross',
    t1.news_id as itid,
    t2.iarticleid as iarticleid,
    t2.izhuanlanid as izhuanlanid,
    t1.pv as bbs_pv,
    t1.uv as bbs_uv
    from 
    (
    select 
    news_id,
    pv,
    uv 
    from tb_syb_content_data_new where fdate = %s and ei = 'bbs_item' and id = '-100' and game_id = '-100' and news_id != '-100'
    ) t1
    join 
    syb_article_tid_zhuanqu_cfg t2
    on (t1.news_id = t2.itid)
    '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
            
    
    ##这里把点击流的数据中与资讯发布到论坛中的数据合并到一起
    sql = '''
    insert table  tb_syb_content_data_new
    select 
    %s,
    '-100'  AS plat,
    '-100' as game_id,
    'news_item_total' as ei,
    CASE WHEN GROUPING(iarticleid) = 1 THEN -100 ELSE cast(iarticleid as BIGINT) END AS news_id,
    sum(pv),
    sum(uv)
    from 
    (
    select 
    tmp1.iarticleid as iarticleid,
    case 
        when  tmp2.iarticleid is null then tmp1.pv
        else tmp1.pv + tmp2.pv 
    end as pv,
    case 
        when  tmp2.iarticleid is null then tmp1.uv
        else tmp1.uv + tmp2.uv 
    end as uv
    from 
    (
    select 
    news_id as iarticleid,
    pv ,
    uv   
    from tb_syb_content_data_new where fdate = %s and ei = 'news_item_tcss' and  id = '-100' and game_id = '-100' and news_id != '-100'
    )
    tmp1
    
    left outer join 
    (
    select 
    articleid as iarticleid,
    pv,
    uv 
    from tb_syb_content_data_bbs_content_temp where ei = 'bbs_article_cross'
    )
    tmp2
    on (tmp1.iarticleid = tmp2.iarticleid)
    )t
    group by CUBE(iarticleid)
    '''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##这里计算热门板块的数据
    sql = """
    insert table  tb_syb_content_data_new

    SELECT
    %s as dtstatdate,
    '-100' AS plat,
    '-100'  AS game_id,
    'bbs_board_item' AS ei,
    t2.iboardid AS news_id,
    COUNT(t1.pvi) AS pv,
    COUNT(DISTINCT t1.pvi) AS uv
    FROM 
    (
    SELECT
    ti,
    pvi 
    FROM
    dw_ta::ta_log_bbs_g_qq_com 
    WHERE daytime = %s  AND   md = 'viewthread'
    )t1 
    JOIN 
    syb_article_tid_zhuanqu_cfg t2
    on(t1.ti = t2.itid)
    WHERE t2.iboardid  != 0
    GROUP BY t2.iboardid 
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    ##这里计算专栏的数据
    sql = '''
    insert table  tb_syb_content_data_new
     
    select 
    %s,
    '-100'  AS plat,
    CASE WHEN GROUPING(izhuanlanid) = 1 THEN -100 ELSE cast(izhuanlanid as BIGINT) END AS izhuanlanid,
    'zhuanlan_item' as ei,
    CASE WHEN GROUPING(iarticleid) = 1 THEN -100 ELSE cast(iarticleid as BIGINT) END AS iarticleid,
    sum(pv),
    sum(uv)
    from        
    (
    select
    t1.iarticleid as iarticleid,
    t2.izhuanlanid as izhuanlanid ,
    t1.pv as pv,
    t1.uv as uv 
    from 
    (
    select 
    news_id as iarticleid,
    pv ,
    uv   
    from tb_syb_content_data_new where fdate = %s and ei = 'news_item_tcss'
    )t1  
    join 
    syb_article_tid_zhuanqu_cfg t2
    on(t1.iarticleid = t2.iarticleid)
    
    
    union all
    
    select 
    articleid as iarticleid,
    zhuanlanid as izhuanlanid,
    pv,
    uv 
    from tb_syb_content_data_bbs_content_temp   
    )t where izhuanlanid != 0
    group by CUBE(izhuanlanid,iarticleid)

    '''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##数据出库临时表
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_syb_content_data_top_10
            (
            fdate int,
            id  string,
            game_id bigint,
            ei string,
            news_id string,
            pv bigint,
            uv bigint
            )
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = ''' delete from tb_syb_content_data_top_10 where fdate = %s'''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##将每种类型的输入写入表，同时算出排名前100的数据
    sql = '''
    insert table tb_syb_content_data_top_10
    SELECT 
    fdate ,
    id  ,
    game_id ,
    ei ,
    news_id ,
    pv ,
    uv
    FROM
    (
    SELECT 
    fdate ,
    id  ,
    game_id ,
    ei ,
    news_id ,
    pv ,
    uv   
    FROM ieg_qt_community_app::tb_syb_content_data_new WHERE fdate = %s  AND uv >= 500 and ei != 'news_item_tcss'
    
    
    UNION ALL 
    
    SELECT 
    fdate ,
    id  ,
    game_id ,
    '-100' AS ei ,
    news_id,
    pv,
    uv 
    FROM ieg_qt_community_app::tb_syb_content_data_new WHERE fdate = %s AND id = '-100' AND game_id = '-100' AND ei in ('bbs_item','news_item_total','video_item','topic_item') AND news_id != '-100'
    ORDER BY pv DESC LIMIT 100
    
    UNION ALL 
    
    SELECT 
    fdate ,
    id  ,
    game_id ,
    '-100' AS ei ,
    '-100' AS news_id,
    SUM(pv) AS pv,
    SUM(uv) AS uv 
    FROM ieg_qt_community_app::tb_syb_content_data_new WHERE fdate = %s AND id = '-100' AND game_id = '-100' AND ei in ('bbs_item','news_item_total','video_item','topic_item') AND news_id != '-100'
    GROUP BY fdate,id,game_id

    )t
    '''%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##视频后台数据统计（点赞，评论相关数据）
    sql = """
            CREATE TABLE IF NOT EXISTS tb_syb_content_comment_data
            (
            fdate int,
            vostype  string,
            game_id bigint,
            ei string,
            video_id string,
            pv bigint,
            uv bigint
            ) PARTITION BY RANGE (fdate)
            (
            partition p_20150801  VALUES LESS THAN (20150802),
            partition p_20150802  VALUES LESS THAN (20150803),
            partition p_20150803  VALUES LESS THAN (20150804),
            partition p_20150804  VALUES LESS THAN (20150805),
            partition p_20150805  VALUES LESS THAN (20150806)
            )"""
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql=""" alter table  tb_syb_content_comment_data DROP PARTITION (p_%s)""" % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = """ alter table tb_syb_content_comment_data ADD PARTITION p_%s VALUES LESS THAN (%s) """%(sDate,partition_date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##将视频的评论和点赞数据计算完成后写入到对应表里
    sql="""insert table  tb_syb_content_comment_data
    SELECT 
    %s,
     '-100' as vostype,
     CASE WHEN GROUPING(game_id) = 1 then -100 else cast(game_id as bigint) end as game_id,
    'video_comment' as ei,
    CASE WHEN GROUPING(video_id) = 1 THEN '-100' ELSE video_id end as video_id,
    COUNT(*) as pv,
    COUNT(DISTINCT isybid) as uv 
    FROM 
    (
     SELECT 
     vv2 as isybid,
     vv1 as video_id  ,
     vv3 as game_id
     from ieg_tdbank::gqq_dsl_day_task_bill_fht0  
     WHERE tdbank_imp_date >= '%s00' and tdbank_imp_date <= '%s23' AND iactiontype = 20 and iactionid = 51
     )t 
     GROUP BY CUBE(video_id,game_id)
     """ % (sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    