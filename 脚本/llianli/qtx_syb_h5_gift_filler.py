#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_h5_gift_filler.py
# 功能描述:     手游宝H5礼包漏斗模型点击PVUV统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_syb_h5_activity_data
# 数据源表:     teg_dw_tcss::tcss_qt_qq_com
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


    ##漏斗模型的统计思路是：下一步的账号一定是基于上一步的账号，是上一步账号的子集
    ##基于这个原因，需要每日有一个临时表来做统计
    ##礼包漏洞模型统计
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_syb_h5_activity_data
    (
    dtstatdate  STRING COMMENT '统计时间',
    iactiveid   INT COMMENT '活动ID',
    sactivityname STRING COMMENT '活动名称',
    sactivityurl STRING COMMENT '活动链接',
    sactiveactioin STRING COMMENT '步骤名称',
    pv BIGINT COMMENT '过程对应pv',
    uv BIGINT COMMENT '过程对应uv'
    ) 
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##漏斗模型统计,计算之前删除，计算之前删除原有数据
    sql = ''' DELETE FROM tb_syb_h5_activity_data WHERE dtstatdate = %s'''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##创建临时表，用来存储对应的每一步的pvid
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_syb_h5_activity_original_data
    (
    dtstatdate  STRING COMMENT '统计时间',
    iactiveid   INT COMMENT '活动ID',
    sactivityname STRING COMMENT '活动名称',
    sactivityurl STRING COMMENT '活动链接',
    sactiveactioin STRING COMMENT '步骤名称',
    pvid STRING COMMENT '用户pvid'
    ) 
    PARTITION BY LIST (dtstatdate)
                    (
                    PARTITION p_20150917  VALUES IN (20150917),
                    PARTITION p_20150918  VALUES IN (20150918),
                    PARTITION p_20150919  VALUES IN (20150919),
                    PARTITION p_20150920  VALUES IN (20150920)
                    ) 
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    sql=''' alter table  tb_syb_h5_activity_original_data DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' alter table tb_syb_h5_activity_original_data ADD PARTITION p_%s VALUES IN (%s) '''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##统计第一步：进入礼包页面数据 ##进入礼包页面一定是从qt域进入，这里匹配QT域
    sql = '''
        INSERT TABLE tb_syb_h5_activity_original_data
            SELECT 
        %s AS dtstatdate,
        t1.avtivity_id AS avtivity_id,
        t1.activity_name AS activity_name,
        t1.activity_url AS activity_url,
        'enter' AS sactiveactioin,
        t.f_pvid as f_pvid 
        FROM 
        (
        SELECT 
        
        f_pvid,
        concat('http://qt.qq.com',f_url) AS f_url 
        FROM teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s AND f_dm = 'QT.QQ.COM' AND instr(f_user_agent,'GAMEJOY') = 0 
   
        )t
        JOIN 
        
        ieg_qt_community_app::tb_syb_h5_webactivitycfg t1
        
        ON (t.f_url = t1.activity_url) 
    '''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##第二步进入游戏专区数据，使用 furl 来匹配，全部从 qt.qq.com域名中获取
    sql = '''
    INSERT TABLE  tb_syb_h5_activity_original_data
     SELECT 
   t2.dtstatdate,
        t2.avtivity_id AS avtivity_id,
        t2.activity_name AS activity_name,
        t2.activity_url AS activity_url,
        'enter_zone' AS sactiveactioin,
        t2.f_pvid as f_pvid
        FROM 
       (
            SELECT 
        %s AS dtstatdate,
        t1.avtivity_id AS avtivity_id,
        t1.activity_name AS activity_name,
        t1.activity_url AS activity_url,
        t.f_pvid as f_pvid
        FROM 
        
        (
        SELECT 
        f_pvid,
        concat('http://qt.qq.com',f_url) AS f_url 
        
        FROM  teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s AND f_dm = 'QT.QQ.COM.HOT' AND lower(f_hottag) like lower('%%activity.goWebAppGetGift')
        
        )t
        JOIN 
        
        ieg_qt_community_app::tb_syb_h5_webactivitycfg t1
        
        ON (t.f_url = t1.activity_url) 
        )t2
        
        JOIN 
        (
        SELECT
         dtstatdate,
        iactiveid as avtivity_id,
        sactivityname as activity_name,
        sactivityurl as activity_url,
        pvid as f_pvid
        FROM tb_syb_h5_activity_original_data WHERE dtstatdate = %s AND sactiveactioin = 'enter'
        GROUP BY    dtstatdate,
        iactiveid,
        sactivityname,
        sactivityurl,
        pvid     
        )t3 
        ON (t2.dtstatdate = t3.dtstatdate 
        and t2.avtivity_id = t3.avtivity_id 
        and t2.activity_name = t3.activity_name 
        and t2.activity_url = t3.activity_url 
        and t2.f_pvid = t3.f_pvid )
    '''%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##第三步点击确定按钮领取礼包
    
    
    #第四步领取成功数据
    
    
    ##第五步领取失败统计
    
    
    sql = '''
    INSERT TABLE  tb_syb_h5_activity_original_data
    SELECT 
    t2.dtstatdate,
        t2.avtivity_id AS avtivity_id,
        t2.activity_name AS activity_name,
        t2.activity_url AS activity_url,
        t2.ei AS ei,
        t2.f_pvid as f_pvid
        FROM 
        
        (
            SELECT
        %s AS dtstatdate,
        t1.avtivity_id AS avtivity_id,
        t1.activity_name AS activity_name,
        t1.activity_url AS activity_url,
        t.ei AS ei,
        t.f_pvid as f_pvid
        FROM 
        (
        SELECT
        f_pvid,
        split(split(f_arg,'%%26')[0],'%%3D')[1] AS game_id,
        CASE 
            WHEN lower(f_hottag) LIKE lower('webapp.index.startGetGift%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.startGetGift%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.startBatchGetActivityGift%%')  
            THEN 'start_get_gift'
            
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.getGiftSucess%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftSucess%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftSucess%%')
            THEN 'success_get_gift'
            
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.getGiftFail%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftFail%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftFail%%') 
            THEN 'fail_get_gift'
            
            ELSE 'unknow'
        END AS ei 
            
        FROM teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s   
        AND f_dm = 'QT.QQ.COM.HOT' 
        AND  
            (
            lower(f_hottag) LIKE lower('webapp.index.startGetGift%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.startGetGift%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.startBatchGetActivityGift%%')  
            
            OR lower(f_hottag) LIKE lower('webapp.index.getGiftSucess%%' )
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftSucess%%' )
            OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftSucess%%')
            
            OR lower(f_hottag) LIKE lower('webapp.index.getGiftFail%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftFail%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftFail%%')
            
            )  
            
            
            
            
            UNION ALL 
            
            SELECT
        f_pvid,
        split(split(f_arg,'%%26')[0],'%%3D')[1] AS game_id,
        CASE 
            WHEN lower(f_hottag) LIKE lower('webapp.index.startGetGift%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.startGetGift%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.startBatchGetActivityGift%%')  
            THEN 'start_get_gift'
            
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.getGiftSucess%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftSucess%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftSucess%%')
            THEN 'success_get_gift'
            
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.getGiftFail%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftFail%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftFail%%') 
            THEN 'fail_get_gift'
            
            ELSE 'unknow'
        END AS ei 
            
        FROM teg_dw_tcss::tcss_bao_qq_com WHERE  f_date = %s   
        AND f_dm = 'BAO.QQ.COM.HOT' 
        AND  
            (
            lower(f_hottag) LIKE lower('webapp.index.startGetGift%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.startGetGift%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.startBatchGetActivityGift%%')  
            
            OR lower(f_hottag) LIKE lower('webapp.index.getGiftSucess%%' )
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftSucess%%' )
            OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftSucess%%')
            
            OR lower(f_hottag) LIKE lower('webapp.index.getGiftFail%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftFail%%')
            OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftFail%%')
            )  
            
            
            
        )t
        JOIN
        ieg_qt_community_app::tb_syb_h5_webactivitycfg t1
        on (t.game_id = t1.game_id)
        
        )t2
        
        JOIN 
        (
        SELECT
         dtstatdate,
        iactiveid as avtivity_id,
        sactivityname as activity_name,
        sactivityurl as activity_url,
        pvid as f_pvid
        FROM tb_syb_h5_activity_original_data WHERE dtstatdate = %s AND sactiveactioin = 'enter_zone'
        GROUP BY    dtstatdate,
        iactiveid,
        sactivityname,
        sactivityurl,
        pvid          
        )t3 
        ON (t2.dtstatdate = t3.dtstatdate 
        and t2.avtivity_id = t3.avtivity_id 
        and t2.activity_name = t3.activity_name 
        and t2.activity_url = t3.activity_url 
        and t2.f_pvid = t3.f_pvid )
        
    '''%(sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
   
   
    sql = '''
    insert table tb_syb_h5_activity_data
    
    select 
    dtstatdate,
    iactiveid ,
    sactivityname ,
    sactivityurl ,
    sactiveactioin ,
    count(*) as pv,
    count(distinct pvid) as uv 
    from tb_syb_h5_activity_original_data where dtstatdate = %s
    group by dtstatdate,
    iactiveid ,
    sactivityname ,
    sactivityurl ,
    sactiveactioin 
    
     '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    tdw.WriteLog("== end OK ==")
    
    
    
    