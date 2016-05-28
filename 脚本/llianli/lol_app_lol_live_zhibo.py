#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_live_zhibo.py
# 功能描述:     手机赛事直播数据
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_app_visit_path
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-12-10
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime

def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    
    live_start_time = '2015-12-14 06:30:00'
    live_end_time = '2015-12-14 13:40:00'
    
    relive_start_time = '2015-12-14 13:40:00'
    relive_end_time = '2015-12-14 23:59:59'

    
#    date_time = datetime.datetime(sDate[0:4],sDate[4:6],sDate[6:8])
#    today_str = date_time.strftime('%Y-%m-%d')
    ##sDate = '20150111'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = '''
    CREATE TABLE IF NOT EXISTS lol_app_live_data_total
(
sname STRING,
sstarttime STRING,
sendtime STRING ,
id BIGINT,
pv BIGINT,
uv BIGINT
)

     
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    



    sql = """
            INSERT TABLE lol_app_live_data_total
SELECT
'全明星赛-lol_app-直播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
CASE WHEN GROUPING(id) = 1 THEN -100 ELSE id END AS id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv
FROM 
(
SELECT 
id,
get_json_object(kv,'$.uin') AS uin 
FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND ei = 'LivePlaying'
)t

GROUP BY cube(id)


                    """ %(sDate,live_start_time,live_end_time,sDate,live_start_time,live_end_time)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    sql = """
            INSERT TABLE lol_app_live_data_total
SELECT
'全明星赛-lol_app-重播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
CASE WHEN GROUPING(id) = 1 THEN -100 ELSE id END AS id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv
FROM 
(
SELECT 
id,
get_json_object(kv,'$.uin') AS uin 
FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND ei = 'LivePlaying'
)t

GROUP BY cube(id)


                    """ %(sDate,relive_start_time,relive_end_time,sDate,relive_start_time,relive_end_time)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)



    sql = '''
    CREATE TABLE IF NOT EXISTS lol_app_live_original_data
(
dtstatdate INT,
id INT ,
uin STRING ,
time BIGINT,
ts BIGINT
)

     
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    



    sql = """
          INSERT OVERWRITE TABLE lol_app_live_original_data
SELECT 
%s,
id,
get_json_object(kv,'$.uin') AS uin,
du AS time,
ts
FROM teg_mta_intf::ieg_lol WHERE sdate = %s  AND ei = 'LiveTime'


                    """ % (sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)





    sql = '''
    CREATE TABLE IF NOT EXISTS lol_app_live_data_time
(
sname STRING,
sstarttime STRING,
sendtime STRING ,
sflag STRING,
id BIGINT,
pv BIGINT,
uv BIGINT,
time BIGINT
)

     
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = '''
    INSERT TABLE lol_app_live_data_time
SELECT
'全明星赛-lol_app-直播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
sflag,
-100 as id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv,
SUM(time) AS time
FROM 
(
SELECT 
id,
uin,
time,
'total' as sflag
FROM lol_app_live_original_data WHERE dtstatdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND time <=  7200 
)t WHERE sflag != 'other'
GROUP BY sflag

     
    '''%(sDate,live_start_time,live_end_time,sDate,live_start_time,live_end_time)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    #
    
    
    sql = '''
    INSERT TABLE lol_app_live_data_time
SELECT
'全明星赛-lol_app-直播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
sflag,
-100 as id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv,
SUM(time) AS time
FROM 
(
SELECT 
id,
uin,
time,
CASE WHEN time >= 10 THEN '10s'  ELSE 'other' END as sflag
FROM lol_app_live_original_data WHERE dtstatdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND time <=  7200 
)t WHERE sflag != 'other'
GROUP BY sflag

     
    '''%(sDate,live_start_time,live_end_time,sDate,live_start_time,live_end_time)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = '''
    INSERT TABLE lol_app_live_data_time
SELECT
'全明星赛-lol_app-直播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
sflag,
-100 as id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv,
SUM(time) AS time
FROM 
(
SELECT 
id,
uin,
time,
CASE WHEN time >= 60 THEN '1min'  ELSE 'other' END as sflag
FROM lol_app_live_original_data WHERE dtstatdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND time <=  7200 
)t WHERE sflag != 'other'
GROUP BY sflag

     
    '''%(sDate,live_start_time,live_end_time,sDate,live_start_time,live_end_time)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    sql = '''
    INSERT TABLE lol_app_live_data_time
SELECT
'全明星赛-lol_app-直播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
sflag,
-100 as id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv,
SUM(time) AS time
FROM 
(
SELECT 
id,
uin,
time,
CASE WHEN time >= 300 THEN '5min'  ELSE 'other' END as sflag
FROM lol_app_live_original_data WHERE dtstatdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND time <=  7200 
)t WHERE sflag != 'other'
GROUP BY sflag

     
    '''%(sDate,live_start_time,live_end_time,sDate,live_start_time,live_end_time)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    sql = '''
    INSERT TABLE lol_app_live_data_time
SELECT
'全明星赛-lol_app-重播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
sflag,
-100 as id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv,
SUM(time) AS time
FROM 
(
SELECT 
id,
uin,
time,
'total' as sflag
FROM lol_app_live_original_data WHERE dtstatdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND time <=  7200 
)t WHERE sflag != 'other'
GROUP BY sflag

     
    '''%(sDate,relive_start_time,relive_end_time,sDate,relive_start_time,relive_end_time)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = '''
    INSERT TABLE lol_app_live_data_time
SELECT
'全明星赛-lol_app-重播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
sflag,
-100 as id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv,
SUM(time) AS time
FROM 
(
SELECT 
id,
uin,
time,
CASE WHEN time >= 10 THEN '10s'  ELSE 'other' END as sflag
FROM lol_app_live_original_data WHERE dtstatdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND time <=  7200 
)t WHERE sflag != 'other'
GROUP BY sflag

     
    '''%(sDate,relive_start_time,relive_end_time,sDate,relive_start_time,relive_end_time)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = '''
    INSERT TABLE lol_app_live_data_time
SELECT
'全明星赛-lol_app-重播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
sflag,
-100 as id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv,
SUM(time) AS time
FROM 
(
SELECT 
id,
uin,
time,
CASE WHEN time >= 60 THEN '1min'  ELSE 'other' END as sflag
FROM lol_app_live_original_data WHERE dtstatdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND time <=  7200 
)t WHERE sflag != 'other'
GROUP BY sflag

     
    '''%(sDate,relive_start_time,relive_end_time,sDate,relive_start_time,relive_end_time)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    sql = '''
    INSERT TABLE lol_app_live_data_time
SELECT
'全明星赛-lol_app-重播-%s' AS sname,
'%s' AS starttime,
'%s' AS endtime,
sflag,
-100 as id,
COUNT(*) AS pv,
COUNT(DISTINCT uin) AS uv,
SUM(time) AS time
FROM 
(
SELECT 
id,
uin,
time,
CASE WHEN time >= 300 THEN '5min'  ELSE 'other' END as sflag
FROM lol_app_live_original_data WHERE dtstatdate = %s AND ts >= unix_timestamp('%s') AND ts <= unix_timestamp('%s') AND time <=  7200 
)t WHERE sflag != 'other'
GROUP BY sflag

     
    '''%(sDate,relive_start_time,relive_end_time,sDate,relive_start_time,relive_end_time)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    tdw.WriteLog("== end OK ==")
    
    
    

    
    
    