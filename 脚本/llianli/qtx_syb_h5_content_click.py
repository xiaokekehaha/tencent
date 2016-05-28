#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_syb_h5_content_click.py
# 功能描述:     手游宝H5每个内容点击统计
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
    CREATE TABLE IF NOT EXISTS tb_syb_h5_content_click_action
        (
        dtstatdate INT COMMENT '统计时间',
        surlflag STRING COMMENT '进入的链接，为单个链接进入的数据做统计',
        btn_name_1 STRING COMMENT '一层按钮点击',
        btn_name_2 STRING COMMENT '二层按钮点击',
        pv BIGINT COMMENT '点击PV',
        uv BIGINT COMMENT '点击UV'
        ) 
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    ##H5每个内容的点击行为统计
    sql="""delete from tb_syb_h5_content_click_action where dtstatdate=%s  """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    ##H5每个内容的点击行为统计V
    sql = """
    INSERT TABLE tb_syb_h5_content_click_action
        SELECT
        %s AS dtstatdate,
        f_url,
        btn_name_1,
        CASE WHEN GROUPING(btn_name_2)  = 1 THEN '-100' ELSE btn_name_2 END AS btn_name_2,
        COUNT(*) AS pv,
        COUNT(DISTINCT f_pvid) AS uv 
        FROM 
        (
        SELECT 
        f_url,
        CASE 
            WHEN f_hottag LIKE '%%webapp.article.share.wx.toFriend%%' 
            OR f_hottag LIKE '%%webapp.article.share.wx.toTimeline%%' 
            OR f_hottag LIKE '%%webapp.article.share.qq.toFriend%%' 
            OR f_hottag LIKE '%%webapp.article.share.qq.toQZone%%' 
            THEN 'btn_share'
            
            WHEN f_hottag LIKE '%%webapp.article.goback%%' 
            OR f_hottag LIKE '%%webapp.article.authorIcon%%' 
            OR f_hottag LIKE '%%webapp.article.like.article%%'  OR f_hottag LIKE '%%webapp.article.like.comment%%'   
            OR f_hottag LIKE '%%webapp.article.comment.article%%'  OR f_hottag LIKE '%%webapp.article.comment.reply%%'   
            OR f_hottag LIKE '%%webapp.article.report%%' 
            OR f_hottag LIKE '%%webapp.article.gototop%%' 
            THEN 'btn_function'
            
            ELSE 'btn_other'
        END AS btn_name_1,
        
        CASE 
            WHEN f_hottag LIKE '%%webapp.article.share.wx.toFriend%%' THEN 'share_wx_friend'
            WHEN f_hottag LIKE '%%webapp.article.share.wx.toTimeline%%' THEN 'share_wx_cycle'
            WHEN f_hottag LIKE '%%webapp.article.share.qq.toFriend%%' THEN 'share_qq_friend'
            WHEN f_hottag LIKE '%%webapp.article.share.qq.toQZone%%' THEN 'share_qq_zone'
            
            WHEN f_hottag LIKE '%%webapp.article.goback%%' THEN 'btn_return'
            WHEN f_hottag LIKE '%%webapp.article.authorIcon%%' THEN 'btn_top_icon'
            WHEN f_hottag LIKE '%%webapp.article.like.article%%'  OR f_hottag LIKE '%%webapp.article.like.comment%%'   THEN 'btn_zan'
            WHEN f_hottag LIKE '%%webapp.article.comment.article%%'  OR f_hottag LIKE '%%webapp.article.comment.reply%%'   THEN 'btn_comment'
            WHEN f_hottag LIKE '%%webapp.article.report%%' THEN 'btn_report'
            WHEN f_hottag LIKE '%%webapp.article.gototop%%' THEN 'btn_return_top'
            
            ELSE 'btn_other'
        END AS btn_name_2,
        f_pvid
        FROM teg_dw_tcss::tcss_qt_qq_com WHERE  f_date = %s AND f_dm = 'QT.QQ.COM.HOT' AND  f_url like '/syb/article/%%'
        )t GROUP BY f_url,btn_name_1,rollup(btn_name_2)
 
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
   
    
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    