#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_h5_source_pv_uv.py
# 功能描述:     手游宝H5来源统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.qtx_syb_h5_source_pv_uv
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


    ##H5来源数据统计
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_syb_h5_source
        (
        dtstatdate INT COMMENT '统计时间',
        surlflag STRING COMMENT '进入的链接，为单个链接进入的数据做统计',
        sflag STRING  COMMENT '渠道 -100 整体，first_page 首页 special_zone 专区',
        ssubflag STRING  COMMENT '子渠道 -100 所有，其他gameid 对应每个游戏',
        ssourcebig STRING COMMENT '外部来源：浏览器跳转，APP进入。。。',
        ssourcesmall STRING COMMENT '每种外部来源的子来源',
        pv BIGINT COMMENT '点击PV',
        uv BIGINT COMMENT '点击UV'
        ) 
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    ##H5整体来源统计
    sql="""delete from tb_syb_h5_source where dtstatdate=%s  """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    ##计算各种来源去向的PV UV
    sql = """
    INSERT TABLE tb_syb_h5_source
        SELECT 
        %s AS dtstatdate,
        '-100' AS surlflag,
        CASE WHEN GROUPING(sflag) = 1 THEN '-100' ELSE sflag  END AS sflag,
        CASE WHEN GROUPING(ssubflag) = 1 THEN '-100' ELSE ssubflag END AS ssubflag ,
        CASE WHEN GROUPING(ssourcebig) = 1  THEN '-100' ELSE ssourcebig END AS ssourcebig,
        CASE WHEN GROUPING(ssourcesmall) = 1 THEN '-100' ELSE ssourcesmall END AS ssourcesmall,
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
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('MQQBrowser')) != 0  
            OR (INSTR(lower(f_user_agent),lower('iPhone')) != 0 AND INSTR(lower(f_user_agent),lower('Android')) = 0 AND INSTR(lower(f_user_agent),lower('MQQBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('UCBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('MicroMessenger')) = 0)
            OR  INSTR(lower(f_user_agent),lower('UCBrowser')) != 0 
            )
            
            THEN 'enter_direct'
            
            WHEN  INSTR(lower(f_user_agent),lower('GAMEJOY')) !=  0 THEN 'app'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sybmp.push%%'
            OR   f_rdm_rurl LIKE 'ADTAGwx.sybmp%%'
            THEN 'wx_public'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sharefriend%%' 
            OR   f_rdm_rurl LIKE 'ADTAGwx.timeline%%' 
            OR   f_rdm_rurl LIKE 'ADTAGqq.sharefriend%%'  
            OR   f_rdm_rurl LIKE 'ADTAGqq.qzone%%'  
            THEN 'share'
        ELSE 'other' 
        END AS ssourcebig,

        CASE 
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('MQQBrowser')) != 0 ) 
            THEN 'enter_direct_qqbrowser'
            
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('iPhone')) != 0 AND INSTR(lower(f_user_agent),lower('Android')) = 0 AND INSTR(lower(f_user_agent),lower('MQQBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('UCBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('MicroMessenger')) = 0 ) 
            THEN 'enter_direct_safari'
            
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('UCBrowser')) != 0 ) 
            THEN 'enter_direct_ucbrowser'
                     
            WHEN  INSTR(lower(f_user_agent),lower('GAMEJOY')) !=  0 THEN 'app'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sybmp.push'
            THEN 'wx_public_pic_article_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sybmp'
            THEN 'wx_public_bottom_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sharefriend%%' 
            THEN 'wx_share_friend_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.timeline%%' 
            THEN 'wx_share_cycle_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGqq.sharefriend%%' 
            THEN 'qq_share_friend_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGqq.qzone%%' 
            THEN 'qq_zone_click'
        ELSE 'other' 
        END AS ssourcesmall,
             
        
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
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('MQQBrowser')) != 0  
            OR (INSTR(lower(f_user_agent),lower('iPhone')) != 0 AND INSTR(lower(f_user_agent),lower('Android')) = 0 AND INSTR(lower(f_user_agent),lower('MQQBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('UCBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('MicroMessenger')) = 0)
            OR  INSTR(lower(f_user_agent),lower('UCBrowser')) != 0 
            )
            
            THEN 'enter_direct'
            
            WHEN  INSTR(lower(f_user_agent),lower('GAMEJOY')) !=  0 THEN 'app'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sybmp.push%%'
            OR   f_rdm_rurl LIKE 'ADTAGwx.sybmp%%'
            THEN 'wx_public'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sharefriend%%' 
            OR   f_rdm_rurl LIKE 'ADTAGwx.timeline%%' 
            OR   f_rdm_rurl LIKE 'ADTAGqq.sharefriend%%'  
            OR   f_rdm_rurl LIKE 'ADTAGqq.qzone%%'  
            THEN 'share'
        ELSE 'other' 
        END AS ssourcebig,

        CASE 
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('MQQBrowser')) != 0 ) 
            THEN 'enter_direct_qqbrowser'
            
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('iPhone')) != 0 AND INSTR(lower(f_user_agent),lower('Android')) = 0 AND INSTR(lower(f_user_agent),lower('MQQBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('UCBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('MicroMessenger')) = 0 ) 
            THEN 'enter_direct_safari'
            
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('UCBrowser')) != 0 ) 
            THEN 'enter_direct_ucbrowser'
                     
            WHEN  INSTR(lower(f_user_agent),lower('GAMEJOY')) !=  0 THEN 'app'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sybmp.push'
            THEN 'wx_public_pic_article_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sybmp'
            THEN 'wx_public_bottom_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sharefriend%%' 
            THEN 'wx_share_friend_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.timeline%%' 
            THEN 'wx_share_cycle_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGqq.sharefriend%%' 
            THEN 'qq_share_friend_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGqq.qzone%%' 
            THEN 'qq_zone_click'
        ELSE 'other' 
        END AS ssourcesmall,
        
        
        
        f_pvid 
        FROM teg_dw_tcss::tcss_bao_qq_com WHERE  f_date = %s AND f_dm = 'BAO.QQ.COM' AND f_url in ('/','/zone.shtml')
        
        
        )t 
        GROUP BY cube(sflag,ssubflag),rollup(ssourcebig,ssourcesmall)
 
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    ##统计微信，qq的数据来源的pv UV
    sql = """
    INSERT TABLE tb_syb_h5_source
        SELECT 
        %s AS dtstatdate,
        '-100' AS surlflag,
        CASE WHEN GROUPING(sflag) = 1 THEN '-100' ELSE sflag  END AS sflag,
        CASE WHEN GROUPING(ssubflag) = 1 THEN '-100' ELSE ssubflag END AS ssubflag ,
        ssourcebig,
        CASE WHEN GROUPING(ssourcesmall) = 1 THEN '-100' ELSE ssourcesmall END AS ssourcesmall,
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
        
        
        'qq_wx' AS ssourcebig,

        CASE 
             WHEN INSTR(lower(f_user_agent),'micromessenger') != 0  THEN 'wx'
             WHEN regexp_instr(f_user_agent,'[/QQ\/(\d+\.(\d+)\.(\d+)\.(\d+))/i) || ua.match(/V1_AND_SQ_([\d\.]+)/]') != 0  THEN 'qq'
             ELSE 'unknow_plat'
        END AS ssourcesmall,
             
        
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
        
        
        'qq_wx' AS ssourcebig,

        CASE 
             WHEN INSTR(lower(f_user_agent),'micromessenger') != 0  THEN 'wx'
             WHEN regexp_instr(f_user_agent,'[/QQ\/(\d+\.(\d+)\.(\d+)\.(\d+))/i) || ua.match(/V1_AND_SQ_([\d\.]+)/]') != 0  THEN 'qq'
             ELSE 'unknow_plat'
        END AS ssourcesmall,
        
        
        
        f_pvid 
        FROM teg_dw_tcss::tcss_bao_qq_com WHERE  f_date = %s AND f_dm = 'BAO.QQ.COM' AND f_url in ('/','/zone.shtml')
        
        
        )t 
        GROUP BY ssourcebig,cube(sflag,ssubflag,ssourcesmall)
 
    """%(sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##一批内容文章的链接对应内容的来源
    sql = """
    INSERT TABLE tb_syb_h5_source
        SELECT 
        %s AS dtstatdate,
        surlflag,
        '-100' AS sflag,        
        '-100' AS ssubflag,
        CASE WHEN GROUPING(ssourcebig) = 1  THEN '-100' ELSE ssourcebig END AS ssourcebig,
        CASE WHEN GROUPING(ssourcesmall) = 1 THEN '-100' ELSE ssourcesmall END AS ssourcesmall,
        COUNT(*) AS pv,
        COUNT(DISTINCT f_pvid) AS uv
        FROM 
        (
        SELECT 
        f_url AS surlflag,

        CASE 
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('MQQBrowser')) != 0  
            OR (INSTR(lower(f_user_agent),lower('iPhone')) != 0 AND INSTR(lower(f_user_agent),lower('Android')) = 0 AND INSTR(lower(f_user_agent),lower('MQQBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('UCBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('MicroMessenger')) = 0)
            OR  INSTR(lower(f_user_agent),lower('UCBrowser')) != 0 
            )
            
            THEN 'enter_direct'
            
            WHEN  INSTR(lower(f_user_agent),lower('GAMEJOY')) !=  0 THEN 'app'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sybmp.push%%'
            OR   f_rdm_rurl LIKE 'ADTAGwx.sybmp%%'
            THEN 'wx_public'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sharefriend%%' 
            OR   f_rdm_rurl LIKE 'ADTAGwx.timeline%%' 
            OR   f_rdm_rurl LIKE 'ADTAGqq.sharefriend%%'  
            OR   f_rdm_rurl LIKE 'ADTAGqq.qzone%%'  
            THEN 'share'
        ELSE 'other' 
        END AS ssourcebig,

        CASE 
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('MQQBrowser')) != 0 ) 
            THEN 'enter_direct_qqbrowser'
            
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('iPhone')) != 0 AND INSTR(lower(f_user_agent),lower('Android')) = 0 AND INSTR(lower(f_user_agent),lower('MQQBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('UCBrowser')) = 0 AND INSTR(lower(f_user_agent),lower('MicroMessenger')) = 0 ) 
            THEN 'enter_direct_safari'
            
            WHEN f_rdm_rurl = '--' AND INSTR(lower(f_user_agent),lower('GAMEJOY')) = 0
            AND ( INSTR(lower(f_user_agent),lower('UCBrowser')) != 0 ) 
            THEN 'enter_direct_ucbrowser'
                     
            WHEN  INSTR(lower(f_user_agent),lower('GAMEJOY')) !=  0 THEN 'app'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sybmp.push'
            THEN 'wx_public_pic_article_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sybmp'
            THEN 'wx_public_bottom_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.sharefriend%%' 
            THEN 'wx_share_friend_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGwx.timeline%%' 
            THEN 'wx_share_cycle_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGqq.sharefriend%%' 
            THEN 'qq_share_friend_click'
            
            WHEN f_rdm_rurl LIKE 'ADTAGqq.qzone%%' 
            THEN 'qq_zone_click'
        ELSE 'other' 
        END AS ssourcesmall,

        f_pvid 
        FROM teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s AND f_dm = 'QT.QQ.COM' AND  f_url like '/syb/article/%%'
        )t 
        GROUP BY surlflag,rollup(ssourcebig,ssourcesmall)
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    ##一批内容文章的qq,微信来源
    sql = """
    INSERT TABLE tb_syb_h5_source
        SELECT 
        %s AS dtstatdate,
        surlflag,
        '-100' AS sflag,        
        '-100' AS ssubflag,
        ssourcebig,
        CASE WHEN GROUPING(ssourcesmall) = 1 THEN '-100' ELSE ssourcesmall END AS ssourcesmall,
        COUNT(*) AS pv,
        COUNT(DISTINCT f_pvid) AS uv
        FROM 
        (
        SELECT 
        f_url AS surlflag,

        'qq_wx' AS ssourcebig,

        CASE 
             WHEN INSTR(lower(f_user_agent),'micromessenger') != 0  THEN 'wx'
             WHEN regexp_instr(f_user_agent,'[/QQ\/(\d+\.(\d+)\.(\d+)\.(\d+))/i) || ua.match(/V1_AND_SQ_([\d\.]+)/]') != 0  THEN 'qq'
             ELSE 'unknow_plat'
        END AS ssourcesmall,

        f_pvid 
        FROM teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s AND f_dm = 'QT.QQ.COM' AND  f_url like '/syb/article/%%'
        )t 
        GROUP BY surlflag,ssourcebig,cube(ssourcesmall)
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    