#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on 2014-12-24

@author: jakegong
'''

#import system module

time = __import__('time')
datetime = __import__('datetime')
string = __import__('string')

# main entry
def TDW_PL(tdw, argv=[]):
    
    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20150111'
    ##对日期做统一处理
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")

    sDate = argv[0];
    ##sDate = '20150111'
    ##对日期做统一处理
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")    
    
    pre_date = today_date - datetime.timedelta(days = 6)
    pre_6_date_str = pre_date.strftime("%Y%m%d")       

    tdw.WriteLog("== sDate = " + sDate + " ==")

    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qqtalk_mtgp_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)

    ##-----创建活跃表
#    sql = """
#            CREATE TABLE IF NOT EXISTS tb_gpcd_active_user
#            (
#            iuin BIGINT COMMENT '用户UIN',
#            business STRING COMMENT '业务类型',
#            dtstatdate INT COMMENT '统计日期'
#            )PARTITION BY LIST (dtstatdate)
#            (
#                partition p_20160314  VALUES IN (20160314),
#                partition p_20160315  VALUES IN (20160315)
#            ) """
#    res = tdw.execute(sql)
    
    sql=""" DELETE FROM tb_gpcd_active_user PARTITION(p_%s)a WHERE dtstatdate=%s AND business like 'mtgp_prefecture_%%' """ %(sDate,sDate)
    res = tdw.execute(sql)
    
    ##--导入活跃CF专区数据
    sql="""
        INSERT TABLE tb_gpcd_active_user
        SELECT
        u.uin ,
        'mtgp_prefecture_cf',
        a.sdate
        FROM 
            (
            SELECT 
            m.uin_mac as uin_mac,
            m.sdate as sdate 
            FROM 
                (SELECT 
                DISTINCT concat(ui,mc) as uin_mac,
                sdate,
                pi
                FROM teg_mta_intf::ieg_tgp
                WHERE sdate=%s AND id in (1100679521, 1200679521)
                )m
                JOIN 
                (SELECT /*+ MAPJOIN(tgp_pi_list) */   
                pi, title                       
                FROM ieg_qqtalk_mtgp_app::tgp_pi_list WHERE (title like 'CF%%') )n 
                ON m.pi = n.pi 
            UNION 
            SELECT 
            t.uin_mac as uin_mac ,
            t.sdate as sdate
            FROM 
                (SELECT 
                DISTINCT concat(ui,mc) as uin_mac,
                sdate,
                ei
                FROM teg_mta_intf::ieg_tgp
                WHERE sdate=%s AND id in (1100679521, 1200679521)
                )t 
                JOIN 
                (SELECT /*+ MAPJOIN(tgp_ei_list) */   
                ei, title                       
                FROM ieg_qqtalk_mtgp_app::tgp_ei_list WHERE (title like 'CF%%') )r 
                ON t.ei = r.ei 
                )a
            JOIN 
            (
            SELECT uin_mac,
            uin FROM ieg_qqtalk_mtgp_app::t_mtgp_uininfo
            )u 
            ON a.uin_mac = u.uin_mac       
        """ %(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== CF prefecture end OK ==")
    
    ##--导入活跃LOL专区数据
    sql="""
        INSERT TABLE tb_gpcd_active_user
        SELECT
        u.uin ,
        'mtgp_prefecture_lol',
        a.sdate
        FROM 
            (
            SELECT 
            m.uin_mac as uin_mac,
            m.sdate as sdate 
            FROM 
                (SELECT 
                DISTINCT concat(ui,mc) as uin_mac,
                sdate,
                pi
                FROM teg_mta_intf::ieg_tgp
                WHERE sdate=%s AND id in (1100679521, 1200679521)
                )m
                JOIN 
                (SELECT /*+ MAPJOIN(tgp_pi_list) */   
                pi, title                       
                FROM ieg_qqtalk_mtgp_app::tgp_pi_list WHERE (title like '666_%%') 
                OR ( title like '战绩_%%')
                OR ( title like '一起玩_%%'))n 
                ON m.pi = n.pi 
            UNION 
            SELECT 
            t.uin_mac as uin_mac ,
            t.sdate as sdate
            FROM 
                (SELECT 
                DISTINCT concat(ui,mc) as uin_mac,
                sdate,
                ei
                FROM teg_mta_intf::ieg_tgp
                WHERE sdate=%s AND id in (1100679521, 1200679521)
                )t 
                JOIN 
                (SELECT /*+ MAPJOIN(tgp_ei_list) */   
                ei, title                       
                FROM ieg_qqtalk_mtgp_app::tgp_ei_list WHERE (title like '666_%%') 
                OR ( title like '战绩_%%')
                OR ( title like '一起玩_%%'))r 
                ON t.ei = r.ei 
                )a
            JOIN 
            (
            SELECT uin_mac,
            uin FROM ieg_qqtalk_mtgp_app::t_mtgp_uininfo
            )u 
            ON a.uin_mac = u.uin_mac       
        """ %(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== lol prefecture end OK ==")
    
    ##--导入活跃DNF专区数据
    sql = """
    INSERT TABLE tb_gpcd_active_user
    SELECT
        u.uin ,
        'mtgp_prefecture_dnf',
        a.sdate
        FROM 
            (
            SELECT 
            m.uin_mac as uin_mac,
            m.sdate as sdate 
            FROM 
                (SELECT 
                DISTINCT concat(ui,mc) as uin_mac,
                sdate,
                pi
                FROM teg_mta_intf::ieg_tgp
                WHERE sdate=%s AND id in (1100679521, 1200679521)
                )m
                JOIN 
                (SELECT /*+ MAPJOIN(tgp_pi_list) */   
                pi, title                       
                FROM ieg_qqtalk_mtgp_app::tgp_pi_list WHERE (title like 'DNF%%') )n 
                ON m.pi = n.pi 
            UNION 
            SELECT 
            t.uin_mac as uin_mac ,
            t.sdate as sdate
            FROM 
                (SELECT 
                DISTINCT concat(ui,mc) as uin_mac,
                sdate,
                ei
                FROM teg_mta_intf::ieg_tgp
                WHERE sdate=%s AND id in (1100679521, 1200679521)
                )t 
                JOIN 
                (SELECT /*+ MAPJOIN(tgp_ei_list) */   
                ei, title                       
                FROM ieg_qqtalk_mtgp_app::tgp_ei_list WHERE (title like 'DNF%%') )r 
                ON t.ei = r.ei 
                )a
            JOIN 
            (
            SELECT uin_mac,
            uin FROM ieg_qqtalk_mtgp_app::t_mtgp_uininfo
            )u 
            ON a.uin_mac = u.uin_mac   
    """%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== dnf prefecture end OK ==")
    
    sql = """
    INSERT TABLE tb_gpcd_active_user
    SELECT 
    t3.uin , 
    'mtgp_prefecture_lol_dnf',
    %s 
    FROM 
    (SELECT 
    DISTINCT t1.iuin as uin 
    FROM 
    (SELECT iuin FROM ieg_qqtalk_mtgp_app::tb_gpcd_active_user WHERE dtstatdate =%s AND business = 'mtgp_prefecture_lol')t1
    JOIN 
    (SELECT iuin FROM ieg_qqtalk_mtgp_app::tb_gpcd_active_user WHERE dtstatdate=%s AND business = 'mtgp_prefecture_dnf')t2
    ON t1.iuin = t2.iuin )t3 
    """%(sDate,sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== dnf & lol prefecture end OK ==")
    
    #导入聊天页面的用户号码包
    sql = """
    INSERT TABLE tb_gpcd_active_user
    SELECT 
    t2.uin,
    'mtgp_chat_page',
    %s
    FROM 
        (SELECT 
        DISTINCT concat(ui,mc) as uin_mac
        FROM 
        teg_mta_intf::ieg_tgp
        WHERE sdate=%s AND id in (1100679521, 1200679521) AND 
        (pi = 'com.tencent.tgp.im.activity.IMNormalGroupChatActivity' OR
        pi = 'com.tencent.tgp.im.activity.IMDicussionGroupChatActivity' OR 
        pi = 'com.tencent.tgp.games.lol.chat.LOLChatTeamActivity' OR 
        pi = 'com.tencent.tgp.im.activity.IMSingleChatActivity' OR 
        pi = 'com.tencent.tgp.im.activity.IMFirstWinGroupActivity' ))t1
    JOIN 
        (SELECT uin_mac,uin FROM ieg_qqtalk_mtgp_app::t_mtgp_uininfo)t2 
        ON t1.uin_mac = t2.uin_mac 
    """%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    #导入LOL普通群组的用户号码包
    sql = """
    INSERT TABLE tb_gpcd_active_user
    SELECT 
    t2.uin,
    'mtgp_lol_normal_group',
    %s
    FROM 
        (SELECT 
        DISTINCT concat(ui,mc) as uin_mac
        FROM 
        teg_mta_intf::ieg_tgp
        WHERE sdate=%s AND id in (1100679521, 1200679521) AND 
        pi = 'com.tencent.tgp.im.activity.IMNormalGroupChatActivity' )t1
    JOIN 
        (SELECT uin_mac,uin FROM ieg_qqtalk_mtgp_app::t_mtgp_uininfo)t2 
        ON t1.uin_mac = t2.uin_mac 
    """%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    #导入组队房间的用户号码包
    sql = """
    INSERT TABLE tb_gpcd_active_user
    SELECT 
    t2.uin,
    'mtgp_make_team_room',
    %s
    FROM 
        (SELECT 
        DISTINCT concat(ui,mc) as uin_mac
        FROM 
        teg_mta_intf::ieg_tgp
        WHERE sdate=%s AND id in (1100679521, 1200679521) AND 
        pi = 'com.tencent.tgp.games.lol.chat.LOLChatTeamActivity' )t1
    JOIN 
        (SELECT uin_mac,uin FROM ieg_qqtalk_mtgp_app::t_mtgp_uininfo)t2 
        ON t1.uin_mac = t2.uin_mac 
    """%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    #导入首胜群组的用户号码包
    sql = """
    INSERT TABLE tb_gpcd_active_user
    SELECT 
    t2.uin,
    'mtgp_first_win_group',
    %s
    FROM 
        (SELECT 
        DISTINCT concat(ui,mc) as uin_mac
        FROM 
        teg_mta_intf::ieg_tgp
        WHERE sdate=%s AND id in (1100679521, 1200679521) AND 
        pi = 'com.tencent.tgp.im.activity.IMFirstWinGroupActivity' )t1
    JOIN 
        (SELECT uin_mac,uin FROM ieg_qqtalk_mtgp_app::t_mtgp_uininfo)t2 
        ON t1.uin_mac = t2.uin_mac 
    """%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)