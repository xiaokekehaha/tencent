#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_h5_pv_uv.py
# 功能描述:     手游宝H5点击PVUV统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_syb_h5_click_action
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


    ##H5页面总体点击PVUV
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_syb_h5_pv_uv
        (
        dtstatdate INT COMMENT '统计时间',
        surlflag STRING COMMENT 'URL标记',
        sflag STRING  COMMENT '渠道 -100 整体，first_page 首页 special_zone 专区',
        ssubflag STRING  COMMENT '子渠道 -100 所有，其他gameid 对应每个游戏',
        splatfrom STRING COMMENT '平台：-100 总体，android 安卓 ， ios IOS，app_embed :app内嵌',
        pv BIGINT COMMENT '点击PV',
        uv BIGINT COMMENT '点击UV'
        ) 
    '''
    res = tdw.execute(sql)

    sql="""delete from tb_syb_h5_pv_uv where dtstatdate=%s  """ % (sDate)
    res = tdw.execute(sql)



    ##计算连接相关的pv uv
    sql = """
    INSERT TABLE tb_syb_h5_pv_uv
        SELECT 
        %s AS dtstatdate,
        '-100' AS surlflag,
        CASE WHEN GROUPING(sflag) = 1 THEN '-100' ELSE sflag  END AS sflag,
        CASE WHEN GROUPING(ssubflag) = 1 THEN '-100' ELSE ssubflag END AS ssubflag ,
        CASE WHEN GROUPING(splatform) = 1 THEN  '-100' ELSE splatform END AS splatform,
        COUNT(*) AS pv,
        COUNT(DISTINCT f_pvid) AS uv
        FROM 
        (
        SELECT 
        CASE 
               WHEN f_url = '/syb/webapp/html/index.shtml' THEN 'first_page'
               WHEN f_url = '/syb/webapp/html/zone.shtml' or f_url = '/zone.shtml' THEN 'special_zone'
               ELSE 'unknow_url'
        END AS sflag,
        
        CASE 
               WHEN f_url = '/syb/webapp/html/index.shtml' THEN '0'
               WHEN ( f_url = '/syb/webapp/html/zone.shtml' or f_url = '/zone.shtml' ) AND f_arg LIKE 'game_id%%' THEN split(cast(split(f_arg,'%%26')[0] as string),'%%3D')[1]
               ELSE 'unknow_flag'
        END AS ssubflag,
        
        CASE 
             WHEN INSTR(lower(f_user_agent),'android') != 0  THEN 'android'
             WHEN INSTR(lower(f_user_agent),'android') = 0 AND (INSTR(lower(f_user_agent),'iphone') != 0 OR INSTR(lower(f_user_agent),'ipad') != 0 ) THEN 'ios'
             WHEN INSTR(lower(f_user_agent),'gamejoy') != 0  THEN 'app_embed' 
             ELSE 'unknow_plat'
        END AS splatform,
        
        f_pvid 
        FROM teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s AND f_dm = 'QT.QQ.COM' AND f_url in ('/syb/webapp/html/index.shtml','/syb/webapp/html/zone.shtml','/zone.shtml')
        
        
        
        UNION ALL
        
        SELECT 
        CASE 
               WHEN f_url = '/' THEN 'first_page'
               WHEN f_url = '/zone.shtml' THEN 'special_zone'
               ELSE 'unknow_url'
        END AS sflag,
        
        CASE 
               WHEN f_url = '/' THEN '0'
               WHEN (f_url = '/zone.shtml' ) AND f_arg LIKE 'game_id%%'  THEN split(cast(split(f_arg,'%%26')[0] as string),'%%3D')[1]
               ELSE 'unknow_flag'
        END AS ssubflag,
        
        CASE 
             WHEN INSTR(lower(f_user_agent),'android') != 0  THEN 'android'
             WHEN INSTR(lower(f_user_agent),'android') = 0 AND (INSTR(lower(f_user_agent),'iphone') != 0 OR INSTR(lower(f_user_agent),'ipad') != 0 ) THEN 'ios'
             WHEN INSTR(lower(f_user_agent),'gamejoy') != 0  THEN 'app_embed' 
             ELSE 'unknow_plat'
        END AS splatform,
        
        f_pvid 
        FROM teg_dw_tcss::tcss_bao_qq_com WHERE  f_date = %s AND f_dm = 'BAO.QQ.COM' AND f_url in ('/','/zone.shtml')
        
        
        )t 
        GROUP BY cube(sflag,ssubflag,splatform)
 
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    ##计算每个点击行为总体功能的点击
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_syb_h5_click_action
        (
        dtstatdate INT COMMENT '统计时间',
        click_action STRING COMMENT '点击的功能点',
        pv BIGINT COMMENT '点击的PV',
        uv BIGINT COMMENT '点击的UV'
        ) 
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = ''' DELETE FROM tb_syb_h5_click_action WHERE dtstatdate = %s '''%(sDate) 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    sql = """
    INSERT TABLE tb_syb_h5_click_action
        SELECT 
        %s AS dtstatdate,
        click_action,
        COUNT(*) AS pv,
        COUNT(DISTINCT f_pvid) AS uv 
        FROM 
        (
        
        SELECT 
        CASE 
            WHEN f_hottag = 'webapp.index.login' THEN  'btn_login'
            WHEN f_hottag = 'webapp.index.logout' THEN 'btn_logout'
            WHEN f_hottag = 'webapp.index.played' THEN 'btn_played'
            WHEN f_hottag = 'webapp.index.recommend' THEN 'btn_recommend'
            WHEN f_hottag LIKE 'webapp.index.banner.pos%%' THEN 'btn_banner'
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.gift.startGetGift.%%') OR lower(f_hottag) LIKE lower('webapp.index.gift.startBatchGetActivityGift%%') 
            OR  lower(f_hottag) LIKE lower('webapp.index.startGetGift.%%') OR lower(f_hottag) LIKE lower('webapp.index.startBatchGetActivityGift%%') 
            THEN 'btn_firstpage_lq'
            
            WHEN lower(f_hottag) LIKE lower('webapp.zone.gift.startGetGift.%%') OR lower(f_hottag) LIKE lower('webapp.zone.gift.startBatchGetActivityGift%%')
            OR  lower(f_hottag) LIKE lower('webapp.zone.startGetGift.%%') OR lower(f_hottag) LIKE lower('webapp.zone.startBatchGetActivityGift%%')
            THEN 'btn_zone_lq'
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.gift.getGiftSucess.%%') OR lower(f_hottag) LIKE lower('webapp.index.gift.batchGetActivityGiftSucess%%')
            OR  lower(f_hottag) LIKE lower('webapp.index.getGiftSucess.%%') OR lower(f_hottag) LIKE lower('webapp.index.batchGetActivityGiftSucess%%') 
            THEN 'btn_firstpage_lq_success'
            
            WHEN lower(f_hottag) LIKE lower('webapp.zone.gift.getGiftSucess.%%') OR lower(f_hottag) LIKE lower('webapp.zone.gift.batchGetActivityGiftSucess%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftSucess.%%') OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftSucess%%') 
            THEN 'btn_zone_lq_success'
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.gift.getGiftFail.%%') OR lower(f_hottag) LIKE lower('webapp.index.gift.batchGetActivityGiftFail%%')
            OR  lower(f_hottag) LIKE lower('webapp.index.getGiftFail.%%') OR lower(f_hottag) LIKE lower('webapp.index.batchGetActivityGiftFail%%')
            THEN 'btn_firstpage_lq_fail'
            
            WHEN lower(f_hottag) LIKE lower('webapp.zone.gift.getGiftFail.%%') OR lower(f_hottag) LIKE lower('webapp.zone.gift.batchGetActivityGiftFail%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftFail.%%') OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftFail%%')
            THEN 'btn_zone_lq_fail'
            
            
            WHEN f_hottag = 'webapp.index.gift.moreGift' THEN 'btn_gift_more'
            
            WHEN f_hottag = 'webapp.index.safarilayer.show' THEN 'btn_safarilayer_show'
            WHEN f_hottag = 'webapp.index.safarilayer.close' THEN 'btn_safarilayerlayer_close'
            WHEN f_hottag = 'webapp.index.syblayer.show' THEN 'btn_syblayer_show'
            WHEN f_hottag = 'webapp.index.syblayer.close' THEN 'btn_syblayer_close'
            WHEN f_hottag = 'webapp.index.syblayer.download' THEN 'btn_syblayer_download'
            
            WHEN f_hottag = 'webapp.index.group.shenkanzu' THEN 'btn_shenkanzu'
            WHEN f_hottag = 'webapp.index.group.airuanmei' THEN 'btn_airuanmei'
            
            ELSE 'unknow_hottag'
        END AS click_action,
        f_pvid
        FROM teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s AND f_dm = 'QT.QQ.COM.HOT' AND f_hottag LIKE '%%webapp.%%' 
        
        UNION ALL
        
        SELECT 
        CASE 
            WHEN f_hottag = 'webapp.index.login' THEN  'btn_login'
            WHEN f_hottag = 'webapp.index.logout' THEN 'btn_logout'
            WHEN f_hottag = 'webapp.index.played' THEN 'btn_played'
            WHEN f_hottag = 'webapp.index.recommend' THEN 'btn_recommend'
            WHEN f_hottag LIKE 'webapp.index.banner.pos%%' THEN 'btn_banner'
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.gift.startGetGift.%%') OR lower(f_hottag) LIKE lower('webapp.index.gift.startBatchGetActivityGift%%') 
            OR  lower(f_hottag) LIKE lower('webapp.index.startGetGift.%%') OR lower(f_hottag) LIKE lower('webapp.index.startBatchGetActivityGift%%') 
            THEN 'btn_firstpage_lq'
            
            WHEN lower(f_hottag) LIKE lower('webapp.zone.gift.startGetGift.%%') OR lower(f_hottag) LIKE lower('webapp.zone.gift.startBatchGetActivityGift%%')
            OR  lower(f_hottag) LIKE lower('webapp.zone.startGetGift.%%') OR lower(f_hottag) LIKE lower('webapp.zone.startBatchGetActivityGift%%')
            THEN 'btn_zone_lq'
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.gift.getGiftSucess.%%') OR lower(f_hottag) LIKE lower('webapp.index.gift.batchGetActivityGiftSucess%%')
            OR  lower(f_hottag) LIKE lower('webapp.index.getGiftSucess.%%') OR lower(f_hottag) LIKE lower('webapp.index.batchGetActivityGiftSucess%%') 
            THEN 'btn_firstpage_lq_success'
            
            WHEN lower(f_hottag) LIKE lower('webapp.zone.gift.getGiftSucess.%%') OR lower(f_hottag) LIKE lower('webapp.zone.gift.batchGetActivityGiftSucess%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftSucess.%%') OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftSucess%%') 
            THEN 'btn_zone_lq_success'
            
            WHEN lower(f_hottag) LIKE lower('webapp.index.gift.getGiftFail.%%') OR lower(f_hottag) LIKE lower('webapp.index.gift.batchGetActivityGiftFail%%')
            OR  lower(f_hottag) LIKE lower('webapp.index.getGiftFail.%%') OR lower(f_hottag) LIKE lower('webapp.index.batchGetActivityGiftFail%%')
            THEN 'btn_firstpage_lq_fail'
            
            WHEN lower(f_hottag) LIKE lower('webapp.zone.gift.getGiftFail.%%') OR lower(f_hottag) LIKE lower('webapp.zone.gift.batchGetActivityGiftFail%%') 
            OR lower(f_hottag) LIKE lower('webapp.zone.getGiftFail.%%') OR lower(f_hottag) LIKE lower('webapp.zone.batchGetActivityGiftFail%%')
            THEN 'btn_zone_lq_fail'
            
            
            WHEN f_hottag = 'webapp.index.gift.moreGift' THEN 'btn_gift_more'
            
            WHEN f_hottag = 'webapp.index.safarilayer.show' THEN 'btn_safarilayer_show'
            WHEN f_hottag = 'webapp.index.safarilayer.close' THEN 'btn_safarilayerlayer_close'
            WHEN f_hottag = 'webapp.index.syblayer.show' THEN 'btn_syblayer_show'
            WHEN f_hottag = 'webapp.index.syblayer.close' THEN 'btn_syblayer_close'
            WHEN f_hottag = 'webapp.index.syblayer.download' THEN 'btn_syblayer_download'
            
            WHEN f_hottag = 'webapp.index.group.shenkanzu' THEN 'btn_shenkanzu'
            WHEN f_hottag = 'webapp.index.group.airuanmei' THEN 'btn_airuanmei'
            
            ELSE 'unknow_hottag'
        END AS click_action,
        f_pvid
        FROM teg_dw_tcss::tcss_bao_qq_com WHERE  f_date = %s AND f_dm = 'BAO.QQ.COM.HOT' AND f_hottag LIKE '%%webapp.%%'
        
        
        )t
        GROUP BY click_action
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    ##banner点击区分每个位置
    sql = """
    INSERT TABLE tb_syb_h5_click_action
        SELECT 
        %s AS dtstatdate,
        click_action,
        COUNT(*) AS pv,
        COUNT(DISTINCT f_pvid) AS uv 
        FROM 
        (
        
        SELECT 
        concat('btn_banner_',split(split(f_hottag,'\\\\.')[3],'pos')[1],'_',split(split(f_hottag,'\\\\.')[4],'id')[1])
         AS click_action,
        f_pvid
        FROM teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s AND f_dm = 'QT.QQ.COM.HOT' AND f_hottag LIKE 'webapp.index.banner.pos%%'
        
        
        UNION ALL 
        
        SELECT 
        concat('btn_banner_',split(split(f_hottag,'\\\\.')[3],'pos')[1],'_',split(split(f_hottag,'\\\\.')[4],'id')[1])
         AS click_action,
        f_pvid
        FROM teg_dw_tcss::tcss_bao_qq_com WHERE  f_date = %s AND f_dm = 'BAO.QQ.COM.HOT' AND f_hottag LIKE 'webapp.index.banner.pos%%'
        
        )t
        GROUP BY click_action
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##热门攻略，手游头条数据上报
    sql = """
    INSERT TABLE tb_syb_h5_click_action
        SELECT 
        %s AS dtstatdate,
        click_action,
        COUNT(*) AS pv,
        COUNT(DISTINCT f_pvid) AS uv 
        FROM 
        (
        
        SELECT 
        CASE 
            WHEN f_hottag LIKE '%%webapp.index.strategy.label.%%'  THEN 'frist_page_strategy'
            WHEN  f_hottag LIKE '%%webapp.index.group.shenkanzu%%' 
                   OR f_hottag LIKE '%%webapp.index.group.airuanmei%%' 
                   OR f_hottag LIKE '%%webapp.index.headline.article%%' 
                   OR f_hottag LIKE '%%webapp.index.headline.more%%'   
            THEN 'frist_page_headline'
            
            ELSE 'unknow_hottag_2'
        END AS click_action,
        f_pvid
        FROM teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s AND f_dm = 'QT.QQ.COM.HOT' AND 
        ( f_hottag LIKE '%%webapp.index.strategy.label.%%' 
        OR f_hottag LIKE '%%webapp.index.group.shenkanzu%%' 
        OR f_hottag LIKE '%%webapp.index.group.airuanmei%%' 
        OR f_hottag LIKE '%%webapp.index.headline.article%%' 
        OR f_hottag LIKE '%%webapp.index.headline.more%%' 
        )
        
        UNION ALL
        
        SELECT 
        CASE 
            WHEN f_hottag LIKE '%%webapp.index.strategy.label.%%'  THEN 'frist_page_strategy'
            WHEN  f_hottag LIKE '%%webapp.index.group.shenkanzu%%' 
                   OR f_hottag LIKE '%%webapp.index.group.airuanmei%%' 
                   OR f_hottag LIKE '%%webapp.index.headline.article%%' 
                   OR f_hottag LIKE '%%webapp.index.headline.more%%'   
            THEN 'frist_page_headline'
            
            ELSE 'unknow_hottag_2'
        END AS click_action,
        f_pvid
        FROM teg_dw_tcss::tcss_bao_qq_com WHERE  f_date = %s AND f_dm = 'BAO.QQ.COM.HOT' AND 
        ( f_hottag LIKE '%%webapp.index.strategy.label.%%' 
        OR f_hottag LIKE '%%webapp.index.group.shenkanzu%%' 
        OR f_hottag LIKE '%%webapp.index.group.airuanmei%%' 
        OR f_hottag LIKE '%%webapp.index.headline.article%%' 
        OR f_hottag LIKE '%%webapp.index.headline.more%%' 
        )
        
        
        )t
        GROUP BY click_action
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    