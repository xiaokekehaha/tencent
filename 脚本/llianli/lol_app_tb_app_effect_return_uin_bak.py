#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_tb_app_return_uin_bak.py
# 功能描述:     掌盟掌火有效回流数据统计——临时补录脚本
# 输入参数:     yyyymmdd    例如：20160309
# 目标表名:     ieg_qt_community_app.tb_app_return_uin
# 数据源表:     ieg_qt_community_app::tb_app_reg_account
# 创建人名:     llianli
# 创建日期:     2016-04-12
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ********************************q**********************************************


#import system module
import datetime

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
    
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")
    
    pre_30_date = today_date - datetime.timedelta(days = 30)
    pre_30_date_str = pre_30_date.strftime("%Y%m%d")
    
    pre_60_date = today_date - datetime.timedelta(days = 60)
    pre_60_date_str = pre_60_date.strftime("%Y%m%d")

    pre_90_date = today_date - datetime.timedelta(days = 90)
    pre_90_date_str = pre_90_date.strftime("%Y%m%d")
    

    sql = """
           CREATE TABLE IF NOT EXISTS tb_app_return_uin
            (
            dtstatdate INT COMMENT '统计日期',
            sfalg STRING COMMENT '数据标记：活跃回流 or 有效回流',
            idaysflag INT COMMENT '统计周期',
            iclienttype INT COMMENT '客户端类型',
            itotaluin BIGINT COMMENT '总活跃用户',
            ireturnuin BIGINT COMMENT '回流用户'
            ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""DELETE FROM tb_app_return_uin WHERE dtstatdate = %s and sfalg = 'effect' """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    ##统计结果写入，不同客户端类型
    sql = """
    INSERT TABLE tb_app_return_uin
            SELECT
            %(today_str)s AS dtstatdate,
            'effect' as sflag,
            30 as idaysflag,
            tt1.iclienttype AS iclienttype,
            tt1.total_uin AS total_uin,
            tt2.ireturnuin AS ireturnuin
            FROM
            (
             SELECT
             iclienttype,
             COUNT(DISTINCT iuin) AS total_uin
             FROM tb_app_original_data WHERE dtstatdate > %(pre_90_date_str)s AND dtstatdate <= %(pre_60_date_str)s 
             GROUP BY iclienttype
            )tt1
            JOIN
            (
             SELECT
             t3.iclienttype as iclienttype,
             COUNT(DISTINCT t3.iuin) AS ireturnuin
             FROM
             (
              SELECT
              t1.iclienttype AS iclienttype,
              t1.iuin as iuin
              FROM
              (
               SELECT
               DISTINCT iclienttype,iuin
               FROM tb_app_original_data  WHERE dtstatdate > %(pre_90_date_str)s AND dtstatdate <= %(pre_60_date_str)s 
              )t1
              LEFT  outer JOIN
              (
               SELECT
               DISTINCT iclienttype,iuin
               FROM tb_app_original_data  WHERE dtstatdate > %(pre_60_date_str)s AND dtstatdate <= %(pre_30_date_str)s 
              )t2
              on(t1.iclienttype = t2.iclienttype AND t1.iuin = t2.iuin)
              WHERE t2.iuin IS NULL
             )t3
             
             JOIN
             
             (
              select 
              iclienttype,iuin 
              from 
              (
               SELECT
               iclienttype,iuin,count(distinct dtstatdate) as loginday
               FROM  tb_app_original_data  WHERE dtstatdate > %(pre_30_date_str)s AND dtstatdate <= %(today_str)s 
               group by iclienttype,iuin
              )tmp
              where loginday > 2
             )t4
             on(t3.iclienttype = t4.iclienttype AND t3.iuin = t4.iuin)
             GROUP BY t3.iclienttype
            )tt2
            ON(tt1.iclienttype = tt2.iclienttype)


                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    
    sql = """
    INSERT TABLE tb_app_return_uin
            SELECT
            %(today_str)s AS dtstatdate,
            'effect' as sflag,
            30 as idaysflag,
            tt1.iclienttype AS iclienttype,
            tt1.total_uin AS total_uin,
            tt2.ireturnuin AS ireturnuin
            FROM
            (
             SELECT
             iclienttype,
             COUNT(DISTINCT iuin) AS total_uin
             FROM (
             select
             case when iclienttype in (9,10) then 0 else 1 end as iclienttype,
             iuin
            from 
                tb_app_original_data WHERE dtstatdate > %(pre_90_date_str)s AND dtstatdate <= %(pre_60_date_str)s and iclienttype in (9,10,15,16)
                )tmp  
             GROUP BY iclienttype
            )tt1
            JOIN
            (
             SELECT
             t3.iclienttype as iclienttype,
             COUNT(DISTINCT t3.iuin) AS ireturnuin
             FROM
             (
              SELECT
              t1.iclienttype AS iclienttype,
              t1.iuin as iuin
              FROM
              (
               SELECT
               DISTINCT iclienttype,iuin
               FROM 
               (
             select
             case when iclienttype in (9,10) then 0 else 1 end as iclienttype,
             iuin
            from 
                tb_app_original_data WHERE dtstatdate > %(pre_90_date_str)s AND dtstatdate <= %(pre_60_date_str)s and iclienttype in (9,10,15,16)
                )tmp  
                
              )t1
              LEFT  outer JOIN
              (
               SELECT
               DISTINCT iclienttype,iuin
               FROM (
             select
             case when iclienttype in (9,10) then 0 else 1 end as iclienttype,
             iuin
            from 
                tb_app_original_data WHERE dtstatdate > %(pre_60_date_str)s AND dtstatdate <= %(pre_30_date_str)s and iclienttype in (9,10,15,16)
                )tmp   
              )t2
              on(t1.iclienttype = t2.iclienttype AND t1.iuin = t2.iuin)
              WHERE t2.iuin IS NULL
             )t3
             
             JOIN
             
             (
              select 
              iclienttype,iuin 
              from 
              (
               SELECT
               iclienttype,iuin,count(distinct dtstatdate) as loginday
               FROM  
               (
             select
             dtstatdate,
             case when iclienttype in (9,10) then 0 else 1 end as iclienttype,
             iuin
            from 
                tb_app_original_data WHERE dtstatdate > %(pre_30_date_str)s AND dtstatdate <= %(today_str)s and iclienttype in (9,10,15,16)
                )tmp   
                
               group by iclienttype,iuin
              )tmp
              where loginday > 2
             )t4
             on(t3.iclienttype = t4.iclienttype AND t3.iuin = t4.iuin)
             GROUP BY t3.iclienttype
            )tt2
            ON(tt1.iclienttype = tt2.iclienttype)


                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
    tdw.WriteLog("== end OK ==")
    
    
    
    