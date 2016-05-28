#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_h5_app_page_click.py
# 功能描述:     手游宝H5APP页卡点击统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_syb_app_page_action
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


    #手游宝app每个页卡点击数据
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_syb_app_page_action
        (
        dtstatdate INT COMMENT '统计时间',
        av STRING COMMENT '版本号',
        iappid BIGINT COMMENT 'APPID',
        ipageid INT COMMENT '页卡ID',
        ipageposid INT COMMENT '页卡槽位ID',
        iactionid INT COMMENT '操作ID',
        ilocpos STRING  COMMENT '素材ID',
        pv BIGINT  COMMENT '点击次数',
        uv BIGINT COMMENT '点击人数，用设备号计算',
        account_cnt BIGINT COMMENT '点击人数，用手游宝id计算'
        )
    '''
    res = tdw.execute(sql)

    sql="""DELETE FROM  tb_syb_app_page_action where dtstatdate=%s  """ % (sDate)
    res = tdw.execute(sql)



    ##计算连接相关的pv uv
    sql = """
    INSERT TABLE tb_syb_app_page_action
     SELECT
        %s AS dtstatdate,
        '-100',
        id,
        page_id,
        CASE WHEN GROUPING(page_pos_id) = 1 THEN '-100' ELSE page_pos_id END AS page_pos_id,
        CASE WHEN GROUPING(action_id) = 1 THEN '-100' ELSE action_id END AS action_id,
        CASE WHEN GROUPING(logo_pos) = 1 THEN '-100' ELSE logo_pos END AS logo_pos,
        COUNT(*) AS pv,
        COUNT(DISTINCT uin_info) AS uv,
        COUNT(DISTINCT sybid) AS account_cnt 
        FROM 
        (
        SELECT
        id,
        concat(ui,mc) AS uin_info,
        get_json_object(kv,'$.sybId') AS sybid,
        split(ei,'_')[0] AS page_id,
        split(ei,'_')[1] AS page_pos_id,
        split(ei,'_')[2] AS action_id ,
        case when ( cast(get_json_object(kv,'$.loc_id') as bigint) is null ) or (  cast(get_json_object(kv,'$.loc_id') as bigint) > 1000) then '-1' else get_json_object(kv,'$.loc_id') end  AS logo_pos
        FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND cdate = %s AND id IN (1100679302,1200679337)
        AND size(split(ei,'_')) >= 3 AND CAST(split(ei,'_')[0] AS INT) > 0 AND CAST(split(ei,'_')[0] AS INT) <= 10000000 
        )t 
        GROUP BY 
        id,page_id,ROLLUP(page_pos_id,action_id,logo_pos)
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = """
    INSERT TABLE tb_syb_app_page_action
     SELECT
        %s AS dtstatdate,
        av,
        id,
        page_id,
        CASE WHEN GROUPING(page_pos_id) = 1 THEN '-100' ELSE page_pos_id END AS page_pos_id,
        CASE WHEN GROUPING(action_id) = 1 THEN '-100' ELSE action_id END AS action_id,
        CASE WHEN GROUPING(logo_pos) = 1 THEN '-100' ELSE logo_pos END AS logo_pos,
        COUNT(*) AS pv,
        COUNT(DISTINCT uin_info) AS uv,
        COUNT(DISTINCT sybid) AS account_cnt 
        FROM 
        (
        SELECT
        case 
        when size(split(av,'\\\\.')) = 1 then concat (av,'.0.0')
        when size(split(av,'\\\\.')) = 2 then concat (av,'.0')
        when size(split(av,'\\\\.')) = 3 then av
        else concat (split(av,'\\\\.')[0],'.',split(av,'\\\\.')[1],'.',split(av,'\\\\.')[2]) 
        end as av,
        id,
        concat(ui,mc) AS uin_info,
        get_json_object(kv,'$.sybId') AS sybid,
        split(ei,'_')[0] AS page_id,
        split(ei,'_')[1] AS page_pos_id,
        split(ei,'_')[2] AS action_id ,
        case when ( cast(get_json_object(kv,'$.loc_id') as bigint) is null ) or (  cast(get_json_object(kv,'$.loc_id') as bigint) > 1000)   then '-1' else get_json_object(kv,'$.loc_id') end  AS logo_pos
        FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND cdate = %s AND id IN (1100679302,1200679337)
        AND size(split(ei,'_')) >= 3 AND CAST(split(ei,'_')[0] AS INT) > 0 AND CAST(split(ei,'_')[0] AS INT) <= 10000000 
        )t 
        GROUP BY 
        av,id,page_id,ROLLUP(page_pos_id,action_id,logo_pos)
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    


    
    
    ##用户显示或者点击了多少款游戏统计
    sql = '''
          CREATE TABLE IF NOT EXISTS tb_syb_app_page_show_game_num
      (
      dtstatdate INT COMMENT '统计时间',
      ilogintype INT COMMENT '登陆类型',
      splatform STRING COMMENT '平台类型',
      sactionflag STRING COMMENT '行为',
      sgamenum STRING COMMENT '显示游戏分布区间',
      view_cnt INT COMMENT '显示对应游戏个数的次数',
      uin_cnt INT COMMENT '显示对应游戏个数的人数',
      syb_cnt INT COMMENT '手游宝ID去重总数'
      ) 
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''DELETE FROM  tb_syb_app_page_show_game_num WHERE dtstatdate = %s AND sactionflag = 'show' ''' %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''
    INSERT TABLE tb_syb_app_page_show_game_num
    SELECT 
    %s AS dtstatdate,
    CASE WHEN GROUPING(ilogintype) = 1 THEN -100 ELSE ilogintype END AS ilogintype,
    CASE WHEN GROUPING(splatform) = 1 THEN '-100' ELSE splatform END AS splatform,
    'show',
    sgamenum,
    COUNT(*) AS pv,
    0 AS uv,
    COUNT(DISTINCT isybid)  AS syb_cnt 
    FROM 
    (
      SELECT 
      
      ilogintype,
      splatform,
      CASE 
          WHEN igamenum = 0 THEN '0' 
          WHEN igamenum = 1 THEN '1' 
          WHEN igamenum = 2 THEN '2'
          WHEN igamenum >= 3 AND igamenum <= 5 THEN '3-5'
          WHEN igamenum >= 6 AND igamenum <= 10 THEN '6-10'
          WHEN igamenum >= 11  THEN '11+'
          ELSE 'other' 
      END AS sgamenum,
      isybid
      FROM
      (
      SELECT 
      isybid,
      ilogintype,
      vv9 AS splatform,
      CAST(vv10 AS BIGINT)  AS igamenum 
      FROM
       ieg_tdbank :: gqq_dsl_day_task_bill_fht0 WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23' AND iactiontype = 19 AND iactionid = 50 
      )t
    )t1 GROUP BY sgamenum,cube(ilogintype,splatform)
    '''%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##显示用户点击了多少游戏
    sql = '''DELETE FROM  tb_syb_app_page_show_game_num WHERE dtstatdate = %s AND sactionflag = 'click' ''' %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''
    INSERT TABLE tb_syb_app_page_show_game_num
    SELECT 
    %s AS dtstatdate,
    -100 AS ilogintype,
    CASE WHEN GROUPING(splatform) = 1 THEN '-100' ELSE splatform END AS splatform,
    'click',
    sgamenum,
    COUNT(*) AS pv,
    COUNT(DISTINCT uin_info) AS uv, 
    COUNT(DISTINCT sybid) AS syb_cnt 
    FROM 
    (
      SELECT 
      
      splatform,
      CASE 
          WHEN igamenum = 0 THEN '0' 
          WHEN igamenum = 1 THEN '1' 
          WHEN igamenum = 2 THEN '2'
          WHEN igamenum >= 3 AND igamenum <= 5 THEN '3-5'
          WHEN igamenum >= 6 AND igamenum <= 10 THEN '6-10'
          WHEN igamenum >= 11  THEN '11+'
          ELSE 'other' 
      END AS sgamenum,
      sybid,
      uin_info
      
      FROM
      (
      SELECT 
      splatform,
      uin_info,
      sybid,
      COUNT(DISTINCT logo_pos) AS igamenum
      FROM 
      (
      SELECT
        cast(id as string) AS splatform,
        concat(ui,mc) AS uin_info,
        get_json_object(kv,'$.sybId') AS sybid,
        get_json_object(kv,'$.loc_id') AS logo_pos
        FROM teg_mta_intf::ieg_shouyoubao WHERE sdate = %s AND cdate = %s AND id IN (1100679302,1200679337) 
        AND ei = '3000_4_200'
      )tmp
      GROUP BY splatform, uin_info, sybid
      )t
    )t1 GROUP BY sgamenum,cube(splatform)
    '''%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    