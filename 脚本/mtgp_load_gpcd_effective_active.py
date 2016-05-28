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
    
    sql=""" DELETE FROM tb_gpcd_active_user PARTITION(p_%s)a WHERE dtstatdate=%s AND business like 'mtgp_effective_active_%%' """ %(sDate,sDate)
    res = tdw.execute(sql)

    ##--导入整体有效活跃用户,有效页面是所有的pi事件中除掉
    #com.tencent.tgp.login.ImageCodeVerifyActivity
    #com.tencent.tgp.login.TGPKickOffDialog
    #com.tencent.tgp.login.LoginActivity
    #com.tencent.tgp.login.QuickLoginActivity
    #com.tencent.tgp.login.GuideActivity
    #com.tencent.tgp.login.LaunchActivity
    sql="""
        INSERT TABLE tb_gpcd_active_user
        SELECT
        u.uin ,
        'mtgp_effective_active',
        a.sdate
        FROM 
            (SELECT t.uin_mac as uin_mac,
            t.sdate as sdate
            FROM 
            (
            SELECT 
            m.uin_mac as uin_mac,
            m.sdate as sdate,
            COUNT(m.pi) as pi_num 
            FROM 
                (SELECT 
                concat(ui,mc) as uin_mac,
                sdate,
                pi 
                FROM teg_mta_intf::ieg_tgp
                WHERE sdate=%s AND id in (1100679521, 1200679521) 
                AND pi != '-'
                AND pi != 'com.tencent.tgp.login.ImageCodeVerifyActivity' 
                AND pi != 'com.tencent.tgp.login.TGPKickOffDialog' 
                AND pi != 'com.tencent.tgp.login.LoginActivity' 
                AND pi != 'com.tencent.tgp.login.QuickLoginActivity' 
                AND pi != 'com.tencent.tgp.login.GuideActivity' 
                AND pi != 'com.tencent.tgp.login.LaunchActivity'
                )m
            GROUP BY m.uin_mac,m.sdate
            )t 
            WHERE t.pi_num >= 3)a
            JOIN 
            (
            SELECT uin_mac,
            uin FROM ieg_qqtalk_mtgp_app::t_mtgp_uininfo
            )u 
            ON a.uin_mac = u.uin_mac         
        """ %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)    

    tdw.WriteLog("== mtgp_effective_active end OK ==")
    
    
    