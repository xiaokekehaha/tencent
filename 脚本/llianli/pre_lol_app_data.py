#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     pre_lol_app_data.py
# 功能描述:     LOL_APP直播预处理数据
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::tb_lol_app_live_original_data

# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-01-14
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

    ##这个是要算呢个数据的时间
    sDate = argv[0]
    


    date_time = datetime.datetime(int(sDate[0:4]),int(sDate[4:6]),int(sDate[6:8]))
    print date_time
    pre_date_time = date_time - datetime.timedelta(days = 1)
    
    date_str = date_time.strftime('%Y-%m-%d')
    pre_date_str = pre_date_time.strftime('%Y-%m-%d')
    
    date_str1 = date_time.strftime('%Y%m%d')
    pre_date_str1 = pre_date_time.strftime('%Y%m%d')
    
    print pre_date_str
    print pre_date_str1


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    ##这里处理下掌盟的数据
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_lol_app_live_original_data
(
dtstatdate INT,
id INT ,
uin STRING ,
time BIGINT,
ts BIGINT
)PARTITION BY LIST (dtstatdate)
                (
                partition p_20160114  VALUES IN (20160114),
                partition p_20160115  VALUES IN (20160115)
                )

     
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = ''' ALTER TABLE tb_lol_app_live_original_data drop partition (p_%s)'''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = ''' ALTER TABLE tb_lol_app_live_original_data ADD PARTITION p_%s VALUES IN (%s)'''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    



    sql = """
          INSERT  TABLE tb_lol_app_live_original_data
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
    

    tdw.WriteLog("== end OK ==")
    
    