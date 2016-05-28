#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_tb_app_stay_uin.py
# 功能描述:     掌盟掌火留存数据统计
# 输入参数:     yyyymmdd    例如：20160309
# 目标表名:     ieg_qt_community_app.tb_app_stay_uin
# 数据源表:     ieg_qt_community_app::tb_app_reg_account
# 创建人名:     llianli
# 创建日期:     2016-03-21
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
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_date = today_date - datetime.timedelta(days = 1)
    pre_date_str = pre_date.strftime("%Y%m%d")
    
   
    pre_3_date = today_date - datetime.timedelta(days = 3)
    pre_3_date_str = pre_3_date.strftime("%Y%m%d")
    
    
    pre_6_date = today_date - datetime.timedelta(days = 6)
    pre_6_date_str = pre_6_date.strftime("%Y%m%d")
    
    pre_7_date = today_date - datetime.timedelta(days = 7)
    pre_7_date_str = pre_7_date.strftime("%Y%m%d")
    
    pre_14_date = today_date - datetime.timedelta(days = 14)
    pre_14_date_str = pre_14_date.strftime("%Y%m%d")
    
    pre_28_date = today_date - datetime.timedelta(days = 28)
    pre_28_date_str = pre_28_date.strftime("%Y%m%d")
    
    
    pre_30_date = today_date - datetime.timedelta(days = 30)
    pre_30_date_str = pre_30_date.strftime("%Y%m%d")
    
    pre_60_date = today_date - datetime.timedelta(days = 60)
    pre_60_date_str = pre_60_date.strftime("%Y%m%d")

    pre_90_date = today_date - datetime.timedelta(days = 90)
    pre_90_date_str = pre_90_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = """
    CREATE TABLE IF NOT EXISTS tb_app_stay_uin_original_temp_%s
    (
    dtstatdate INT COMMENT '统计日期',
    iclienttype INT COMMENT '客户端类型',
    iuin BIGINT COMMENT 'UIN',
    iregdate INT COMMENT '用户注册日期',
    
    iday1actdays INT COMMENT '日活跃第一周期',
    iday2actdays INT COMMENT '日活跃第二周期',
    
    
    i3day1actdays INT COMMENT '三日活跃第一周期',
    i3day2actdays INT COMMENT '三日活跃第二周期',
    
    
    iweek1actdays INT COMMENT '周活跃第一周期',
    iweek2actdays INT COMMENT '周活跃第二周期',
    
    idweek1actdays INT COMMENT '双周活跃第一周期',
    idweek2actdays INT COMMENT '双周活跃第二周期',
    
    imonth1actdays INT COMMENT '月活跃第一周期',
    imonth2actdays INT COMMENT '月活跃第二周期'
    
    )
     """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##临时数据写入
    sql = """
    INSERT OVERWRITE TABLE tb_app_stay_uin_original_temp_%(sDate)s
    SELECT
    %(sDate)s AS dtstatdate,
    iclienttype,
    iuin,
    iregdate,
    length(regexp_replace(substr(cbitmap,2,1),'0','')) AS iday1actdays,
    length(regexp_replace(substr(cbitmap,1,1),'0','')) AS iday2actdays,
    
    length(regexp_replace(substr(cbitmap,4,3),'0','')) AS i3day1actdays,
    length(regexp_replace(substr(cbitmap,1,3),'0','')) AS i3day2actdays,
    
    
    length(regexp_replace(substr(cbitmap,8,7),'0','')) AS iweek1actdays,
    length(regexp_replace(substr(cbitmap,1,7),'0','')) AS iweek2actdays,
    
    
    length(regexp_replace(substr(cbitmap,15,14),'0','')) AS idweek1actdays,
    length(regexp_replace(substr(cbitmap,1,14),'0','')) AS idweek2actdays,
    
    
    
    length(regexp_replace(substr(cbitmap,31,30),'0','')) AS imonth1actdays,
    length(regexp_replace(substr(cbitmap,1,30),'0','')) AS imonth2actdays
    
     
    FROM tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE dtstatdate = %(sDate)s 
    """%(locals())
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
            CREATE TABLE IF NOT EXISTS tb_app_stay_uin
            (
            dtstatdate INT COMMENT '统计日期',
            sflag STRING COMMENT '数据标记：活跃或新进',
            idaysflag INT COMMENT '统计周期',
            iclienttype INT COMMENT '客户端类型',
            itotaluin BIGINT COMMENT '总活跃用户',
            istayuin BIGINT COMMENT '留存用户'
            ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""DELETE FROM tb_app_stay_uin WHERE dtstatdate = %s AND sflag = 'act' """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    ##写入活跃用户留存率
    sql = """
            INSERT TABLE tb_app_stay_uin
            SELECT
            %(sDate)s AS dtstatdate,
            'act' AS sflag,
            idayflag,
            iclienttype,
            SUM(iactuin) AS iactuin,
            SUM(istayuin) AS istayuin
            
            FROM
            (
            SELECT
            1 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN iday1actdays !=0 THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN iday1actdays != 0 AND iday2actdays != 0  THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            UNION ALL 
            
            SELECT
            3 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN i3day1actdays != 0 THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN i3day1actdays != 0 AND i3day2actdays != 0  THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            UNION ALL 
            
            SELECT
            7 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN iweek1actdays !=0 THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN iweek1actdays != 0 AND iweek2actdays != 0  THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            
            UNION ALL 
            
            SELECT
            14 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN idweek1actdays != 0 THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN idweek1actdays != 0 AND idweek2actdays != 0 THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            
            UNION ALL 
            
            SELECT
            30 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN imonth1actdays != 0 THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN imonth1actdays != 0 AND imonth2actdays != 0 THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            )t
            GROUP BY idayflag,iclienttype


                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ DELETE FROM tb_app_stay_uin WHERE dtstatdate = %s  AND sflag = 'reg' """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##写入新进用户的留存率
    sql = """INSERT TABLE tb_app_stay_uin
            SELECT
            %(sDate)s AS dtstatdate,
            'reg' AS sflag,
            idayflag,
            iclienttype,
            SUM(iactuin) AS iactuin,
            SUM(istayuin) AS istayuin
            
            FROM
            (
            SELECT
            1 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN iday1actdays != 0 AND iregdate = %(pre_date_str)s THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN iday1actdays != 0 AND iregdate = %(pre_date_str)s  AND iday2actdays != 0  THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            UNION ALL 
            
            SELECT
            3 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN i3day1actdays != 0 AND iregdate > %(pre_6_date_str)s AND iregdate <= %(pre_3_date_str)s THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN i3day1actdays != 0 AND iregdate > %(pre_6_date_str)s AND iregdate <= %(pre_3_date_str)s AND i3day2actdays != 0  THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            UNION ALL 
            
            SELECT
            7 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN iweek1actdays != 0 AND iregdate > %(pre_14_date_str)s AND iregdate <= %(pre_7_date_str)s THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN iweek1actdays != 0 AND iregdate > %(pre_14_date_str)s AND iregdate <= %(pre_7_date_str)s AND iweek2actdays != 0  THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            
            UNION ALL 
            
            SELECT
            14 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN idweek1actdays != 0 AND iregdate > %(pre_28_date_str)s AND iregdate <= %(pre_14_date_str)s THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN idweek1actdays != 0 AND iregdate > %(pre_28_date_str)s AND iregdate <= %(pre_14_date_str)s AND idweek2actdays != 0 THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            
            UNION ALL 
            
            SELECT
            30 AS idayflag,
            iclienttype,
            iuin,
            CASE WHEN imonth1actdays != 0 AND iregdate > %(pre_60_date_str)s AND iregdate <= %(pre_30_date_str)s THEN 1 ELSE 0  END AS iactuin,
            CASE WHEN imonth1actdays != 0 AND iregdate > %(pre_60_date_str)s AND iregdate <= %(pre_30_date_str)s AND imonth2actdays != 0 THEN 1 ELSE 0 END AS istayuin
            FROM tb_app_stay_uin_original_temp_%(sDate)s
            
            )t
            GROUP BY idayflag,iclienttype 
    """%(locals())
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ DROP TABLE tb_app_stay_uin_original_temp_%s"""%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    

    
    tdw.WriteLog("== end OK ==")
    
    
    
    