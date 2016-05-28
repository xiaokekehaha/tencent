#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on 2016-04-06

@author: xiaokezhou
'''
#import system module

time = __import__('time')
datetime = __import__('datetime')
string = __import__('string')

# main entry
def TDW_PL(tdw, argv=[]):
    
    tdw.WriteLog("== begin ==")

    #tdw.WriteLog("== argv[0] = " + argv[0] + " ==")
    sDate = '20160525';
    ##sDate = '20150111'
    ##对日期做统一处理
    today_str = sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")

    #sDate = argv[0];
    ##sDate = '20150111'
    ##对日期做统一处理
    today_str = sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days=1)
    pre_date_str = pre_date.strftime("%Y%m%d")    
    
    pre_date = today_date - datetime.timedelta(days=6)
    pre_6_date_str = pre_date.strftime("%Y%m%d")       

    pre_date = today_date - datetime.timedelta(days=7)
    pre_7_date_str = pre_date.strftime("%Y%m%d")   
   
    pre_date = today_date - datetime.timedelta(days=13)
    pre_13_date_str = pre_date.strftime("%Y%m%d")   
     
    tdw.WriteLog("== pre_6_date_str = " + pre_6_date_str + " ==")
    tdw.WriteLog("== pre_14_date_str = " + pre_13_date_str + " ==")

    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qqtalk_mtgp_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)

  #统计错误码 101 103 301错误数量
    sql = """
            CREATE TABLE IF NOT EXISTS wangzherongyao_login_ratio_validated_user
            (
            sdate int,
            system  string comment '系统 qq 微信',
            validated_user  string comment '登录比有效用户',
            remain_user  string comment '登录比留存用户',
            login_ratio float 
            ) """
    res = tdw.execute(sql)

    sql = """delete from wangzherongyao_login_ratio_validated_user where sdate=%s""" % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    tdw.WriteLog("== start count num ==")


    ##--导入活跃表全体数据
    sql = """
       INSERT TABLE wangzherongyao_login_ratio_validated_user
       SELECT
       %(sDate)s,
       'ALL',
        COUNT(CASE when activedatas>=2 THEN iuin ELSE null end ) as validated_user,
        COUNT(CASE when activedatas>=3 THEN iuin ELSE null end ) as remain_user,
        COUNT(CASE when activedatas>=3 THEN iuin ELSE null end ) /COUNT(CASE when activedatas>=2 THEN iuin ELSE null end ) as login_ratio
     from 
     (
      SELECT 
         iuin,
         SUM(activebitmap) as activedatas
       from
         ieg_qqtalk_mtgp_app::tb_gpcd_reg_account lateral view explode(split(cbitmap,'')) cbitmap as activebitmap   
        WHERE  business='WZRY_ALL' and iregdate=%(pre_6_date_str)s 
         GROUP BY
           iuin
       )
    t 
                      
        """ % {'sDate':sDate, 'pre_6_date_str':pre_6_date_str}
##    tdw.WriteLog("%s" % sql)
    res = tdw.execute(sql)  
#    
 
  #统计错误码2次登录比
    sql = """
            CREATE TABLE IF NOT EXISTS wangzherongyao_2login_ratio_validated_user
            (
            sdate int,
            system  string comment '系统 qq 微信',
            validated_user  string comment '登录比有效用户',
            remain_user  string comment '登录比留存用户',
            login_ratio float 
            ) """
    res = tdw.execute(sql)

    sql = """delete from wangzherongyao_2login_ratio_validated_user where sdate=%s""" % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    tdw.WriteLog("== start count num ==")


    ##--导入活跃表全体数据
    sql = """
       INSERT TABLE wangzherongyao_2login_ratio_validated_user
       SELECT
     %(sDate)s,
     'ALL',
       c.validated_user as validated_user,
       d.remain_user as remain_user, 
       c.validated_user/d.remain_user as login_ratio
        from          
       (select  
        COUNT(CASE when a.activedatas>=3 THEN iuin ELSE null end ) as validated_user
       from 
      ( SELECT 
         iuin,
         SUM(activebitmap) as activedatas
       from
         ieg_qqtalk_mtgp_app::tb_gpcd_reg_account lateral view explode(split(cbitmap,'')) cbitmap as activebitmap   
        WHERE  business='WZRY_ALL' and iregdate=%(pre_13_date_str)s  and  dtstatdate BETWEEN %(pre_13_date_str)s  and %(pre_7_date_str)s 
         GROUP BY
           iuin
       ) a)
       c
       join        
       (select  
        COUNT(CASE when activedatas>=1 THEN iuin ELSE null end ) as remain_user
       from 
      ( SELECT 
         iuin,
         SUM(activebitmap) as activedatas
       from
         ieg_qqtalk_mtgp_app::tb_gpcd_reg_account lateral view explode(split(cbitmap,'')) cbitmap as activebitmap   
        WHERE 
           business='WZRY_ALL' and iregdate=%(pre_13_date_str)s  and   dtstatdate BETWEEN %(pre_7_date_str)s and %(sDate)s
         GROUP BY
           iuin
       )b)
       d                          
        """ % {'pre_7_date_str':pre_7_date_str, 'pre_13_date_str':pre_13_date_str, 'sDate':sDate}
        
            
    res = tdw.execute(sql)
  
    tdw.WriteLog("== end OK ==")
    
    
