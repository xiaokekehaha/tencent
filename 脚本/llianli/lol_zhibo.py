#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_zhibo.py
# 功能描述:     直播数据汇总
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_lol_video_live_all_data
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
    
    

    
    date_time = datetime.datetime(int(sDate[0:4]),int(sDate[4:6]),int(sDate[6:8]))
    print date_time
    pre_date_time = date_time - datetime.timedelta(days = 1)
    
    date_str = date_time.strftime('%Y-%m-%d')
    pre_date_str = pre_date_time.strftime('%Y-%m-%d')
    
    date_str1 = date_time.strftime('%Y%m%d')
    pre_date_str1 = pre_date_time.strftime('%Y%m%d')
    
    partitin_date_str = sDate
    #partitin_date_str = '20160107'
    
    print pre_date_str
    print pre_date_str1

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = '''
    CREATE TABLE IF NOT EXISTS tb_lol_video_live_all_data (
    dtstatdate int,
  name string,
  start_time string,
  end_time string,
  web_pcu int,
  web_pcu_time string,
  web_acu float,
  web_width int,
  web_pv int,
  web_uv int,
  web_time float, 
  web_pv_10s bigint,
  web_uv_10s bigint,
  web_pv_1min bigint,
  web_uv_1min bigint,
  web_pv_5min bigint,
  web_uv_5min bigint
  )

     
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    sql = ''' delete from tb_lol_video_live_all_data where dtstatdate = %s'''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ##先将数据预处理一次，得到一个在对应时间范围内的数据
    
    ##先处理电视台的数据
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_lol_tv_zhibo_data_temp
    (
    idx int,
    smatchid string,
    imatchsubid int,
    sname string,
    sstarttime string,
    sendtime string,
    iuin int,
    ionlinetime bigint
    )
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''
    INSERT OVERWRITE TABLE tb_lol_tv_zhibo_data_temp
    SELECT
                                        idx,
                                        smatchid
                                        ,imatchsubid
                                        ,sname
                                        ,sstarttime
                                        ,sendtime
                                        ,iuin
                                        ,CASE
                                            WHEN timestamp_leave <= UNIX_TIMESTAMP(sstarttime) THEN 0
                                            WHEN timestamp_enter >= UNIX_TIMESTAMP(sendtime) THEN 0
                                            WHEN timestamp_enter <= UNIX_TIMESTAMP(sstarttime)
                                                  AND timestamp_leave <= UNIX_TIMESTAMP(sendtime) 
                                            THEN timestamp_leave - UNIX_TIMESTAMP(sstarttime)
                                            
                                            WHEN timestamp_enter <= UNIX_TIMESTAMP(sstarttime)
                                                     AND timestamp_leave > UNIX_TIMESTAMP(sendtime) 
                                            THEN UNIX_TIMESTAMP(sendtime) - UNIX_TIMESTAMP(sstarttime)
                                            
                                            WHEN timestamp_enter > UNIX_TIMESTAMP(sstarttime)
                                                AND timestamp_leave <= UNIX_TIMESTAMP(sendtime) 
                                            THEN ionlinetime
                                            
                                            WHEN timestamp_enter > UNIX_TIMESTAMP(sstarttime)
                                                AND timestamp_leave > UNIX_TIMESTAMP(sendtime) 
                                            THEN UNIX_TIMESTAMP(sendtime) - timestamp_enter
                                            ELSE 0
                                        END AS ionlinetime
                                    FROM
                                        (
                                            SELECT
                                            tmp2.idx AS idx,
                                            tmp2.smatchid AS smatchid,
                                            tmp2.imatchsubid AS  imatchsubid,
                                            tmp2.sname AS sname,
                                            tmp2.sstarttime AS sstarttime,
                                            tmp2.sendtime AS sendtime,
                                            tmp1.iuin AS iuin,
                                            tmp1.timestamp_leave AS timestamp_leave,
                                            tmp1.timestamp_enter AS timestamp_enter,
                                            tmp1.ionlinetime AS ionlinetime
                                            FROM 
                                            (
                                             SELECT
                                                    iuin
                                                    ,UNIX_TIMESTAMP(dteventtime) AS timestamp_leave
                                                    ,UNIX_TIMESTAMP(dteventtime) - ionlinetime AS timestamp_enter
                                                    ,ionlinetime
                                                FROM
                                                    ieg_tdbank :: qtalk_dsl_leaveroom_v2_fht0
                                                WHERE
                                                    tdbank_imp_date BETWEEN '%s00' AND '%s23'
                                                    AND iroomid1 = 94961178
                                                    AND isourcetype = 1999
                                             )tmp1 
                                             JOIN
                                             (
                                                 SELECT
                                                 idx, 
                                                 smatchid,
                                                 imatchsubid,
                                                 sname,
                                                 sstarttime,
                                                 sendtime
                                                 FROM hy::t_dw_mkt_oss_tblollivecfg WHERE statis_date = %s AND sendtime LIKE '%%%s%%' and sname not like '%%重播%%'
                                             )tmp2
                                        ) t where timestamp_leave >=  UNIX_TIMESTAMP(sstarttime) and 
                                        timestamp_enter <= UNIX_TIMESTAMP(sendtime)
                                        
                                        
    '''%(pre_date_str1,date_str1,partitin_date_str,date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    sql = """
    insert table tb_lol_video_live_all_data
    select 
    %s,
    t2.sname as sname,
    t2.sstarttime as sstarttime,
    t2.sendtime as sendtime,
    t3.pcu as web_pcu ,
      t3.pcu_time as web_pcu_time ,
      t3.acu as web_acu ,
      t3.cdn_width as web_width ,
      t2.web_pv ,
      t2.web_uv ,
      t2.web_time , 
      t2.web_pv_10s ,
      t2.web_uv_10s ,
      t2.web_pv_1min ,
      t2.web_uv_1min ,
      t2.web_pv_5min ,
      t2.web_uv_5min 
  from 
    (
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    sum(web_pv) as web_pv,
    sum(web_uv) as web_uv,
    sum(web_pv_10s) as web_pv_10s,
    sum(web_uv_10s) as web_uv_10s,
    sum(web_pv_1min) as web_pv_1min,
    sum(web_uv_1min) as web_uv_1min,
    sum(web_pv_5min) as web_pv_5min,
    sum(web_uv_5min) as web_uv_5min,
    sum(avg_time) as web_time
    from 
    (
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    case  when  sflag = 'total' then web_pv else 0 end as web_pv,
    case  when  sflag = 'total' then web_uv else 0 end as web_uv,
    case  when  sflag = '10s' then web_pv else 0 end as web_pv_10s,
    case  when  sflag = '10s' then web_uv else 0 end as web_uv_10s,
    case  when  sflag = '1min' then web_pv else 0 end as web_pv_1min,
    case  when  sflag = '1min' then web_uv else 0 end as web_uv_1min,
    case  when  sflag = '5min' then web_pv else 0 end as web_pv_5min,
    case  when  sflag = '5min' then web_uv else 0 end as web_uv_5min,
    
    case  when  sflag = 'time' then web_pv/60/web_uv else 0.0 end as avg_time
    from 
    (
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    count(*) as web_pv,
    count(distinct iuin) as web_uv ,
    'total' as sflag
    from tb_lol_tv_zhibo_data_temp
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
    
    
    union all 
    
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    count(*) as web_pv,
    count(distinct iuin) as web_uv ,
    '10s' as sflag
    from tb_lol_tv_zhibo_data_temp where ionlinetime >= 10
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
  
    union all 
    
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    count(*) as web_pv,
    count(distinct iuin) as web_uv ,
    '1min' as sflag
    from tb_lol_tv_zhibo_data_temp where ionlinetime >= 60
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
  
   union all 
    
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    count(*) as web_pv,
    count(distinct iuin) as web_uv ,
    '5min' as sflag
    from tb_lol_tv_zhibo_data_temp where ionlinetime >= 300
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
  
    union all 
    
    
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    sum(ionlinetime) as web_pv,
    count(distinct iuin) as web_uv ,
    'time' as sflag
    from tb_lol_tv_zhibo_data_temp 
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
  
  
    )t
    )t1
    group by idx,smatchid,imatchsubid,sname,sstarttime,sendtime
    )t2
    join 
    (
    select 
    idx,
    smatchid,
    sname,
    sstarttime,
    sendtime,
    pcu,
    pcu_time,
    cdn_width,
    acu from  hy::t_dw_mkt_qt_lol_live_pcu where statis_date = %s and ichannelid = 0 and ivideodefinition  = -100
    )t3
    on (t2.smatchid = t3.smatchid and t2.sname = t3.sname and t2.sstarttime = t3.sstarttime and t2.sendtime = t3.sendtime)
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    #处理手机数据
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_lol_cellphone_zhibo_data_temp
    (
    idx int,
    smatchid string,
    imatchsubid int,
    sname string,
    sstarttime string,
    sendtime string,
    iuin int,
    ionlinetime bigint
    )
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''
    INSERT OVERWRITE TABLE tb_lol_cellphone_zhibo_data_temp
    SELECT
                                        idx,
                                        smatchid
                                        ,imatchsubid
                                        ,sname
                                        ,sstarttime
                                        ,sendtime
                                        ,iuin
                                        ,CASE
                                            WHEN timestamp_leave <= UNIX_TIMESTAMP(sstarttime) THEN 0
                                            WHEN timestamp_enter >= UNIX_TIMESTAMP(sendtime) THEN 0
                                            WHEN timestamp_enter <= UNIX_TIMESTAMP(sstarttime)
                                                  AND timestamp_leave <= UNIX_TIMESTAMP(sendtime) 
                                            THEN timestamp_leave - UNIX_TIMESTAMP(sstarttime)
                                            
                                            WHEN timestamp_enter <= UNIX_TIMESTAMP(sstarttime)
                                                     AND timestamp_leave > UNIX_TIMESTAMP(sendtime) 
                                            THEN UNIX_TIMESTAMP(sendtime) - UNIX_TIMESTAMP(sstarttime)
                                            
                                            WHEN timestamp_enter > UNIX_TIMESTAMP(sstarttime)
                                                AND timestamp_leave <= UNIX_TIMESTAMP(sendtime) 
                                            THEN ionlinetime
                                            
                                            WHEN timestamp_enter > UNIX_TIMESTAMP(sstarttime)
                                                AND timestamp_leave > UNIX_TIMESTAMP(sendtime) 
                                            THEN UNIX_TIMESTAMP(sendtime) - timestamp_enter
                                            ELSE 0
                                        END AS ionlinetime
                                    FROM
                                        (
                                            SELECT
                                            tmp2.idx AS idx,
                                            tmp2.smatchid AS smatchid,
                                            tmp2.imatchsubid AS  imatchsubid,
                                            tmp2.sname AS sname,
                                            tmp2.sstarttime AS sstarttime,
                                            tmp2.sendtime AS sendtime,
                                            tmp1.iuin AS iuin,
                                            tmp1.timestamp_leave AS timestamp_leave,
                                            tmp1.timestamp_enter AS timestamp_enter,
                                            tmp1.ionlinetime AS ionlinetime
                                            FROM 
                                            (
                                             SELECT
                                                    cast(uin as bigint) as iuin
                                                    ,ts AS timestamp_leave
                                                    ,ts - time AS timestamp_enter
                                                    ,time as ionlinetime
                                                FROM
                                                    ieg_qt_community_app::tb_lol_app_live_original_data
                                                WHERE
                                                    dtstatdate BETWEEN %s AND %s
                                                    AND time < 3*3600
                                             )tmp1 
                                             JOIN
                                             (
                                                 SELECT
                                                 idx, 
                                                 smatchid,
                                                 imatchsubid,
                                                 sname,
                                                 sstarttime,
                                                 sendtime
                                                 FROM hy::t_dw_mkt_oss_tblollivecfg WHERE statis_date = %s AND sendtime LIKE '%%%s%%' and sname not like '%%重播%%'
                                             )tmp2
                                        ) t where timestamp_leave >=  UNIX_TIMESTAMP(sstarttime) and 
                                        timestamp_enter <= UNIX_TIMESTAMP(sendtime)
                                        
                                        
    '''%(pre_date_str1,date_str1,partitin_date_str,date_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    sql = """
    insert table tb_lol_video_live_all_data
    select 
    %s,
    concat('lol_app_',t2.sname) as sname,
    t2.sstarttime as sstarttime,
    t2.sendtime as sendtime,
    t3.pcu as web_pcu ,
      t3.pcu_time as web_pcu_time ,
      t3.acu as web_acu ,
      t3.cdn_width as web_width ,
      t2.web_pv ,
      t2.web_uv ,
      t2.web_time , 
      t2.web_pv_10s ,
      t2.web_uv_10s ,
      t2.web_pv_1min ,
      t2.web_uv_1min ,
      t2.web_pv_5min ,
      t2.web_uv_5min 
  from 
    (
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    sum(web_pv) as web_pv,
    sum(web_uv) as web_uv,
    sum(web_pv_10s) as web_pv_10s,
    sum(web_uv_10s) as web_uv_10s,
    sum(web_pv_1min) as web_pv_1min,
    sum(web_uv_1min) as web_uv_1min,
    sum(web_pv_5min) as web_pv_5min,
    sum(web_uv_5min) as web_uv_5min,
    sum(avg_time) as web_time
    from 
    (
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    case  when  sflag = 'total' then web_pv else 0 end as web_pv,
    case  when  sflag = 'total' then web_uv else 0 end as web_uv,
    case  when  sflag = '10s' then web_pv else 0 end as web_pv_10s,
    case  when  sflag = '10s' then web_uv else 0 end as web_uv_10s,
    case  when  sflag = '1min' then web_pv else 0 end as web_pv_1min,
    case  when  sflag = '1min' then web_uv else 0 end as web_uv_1min,
    case  when  sflag = '5min' then web_pv else 0 end as web_pv_5min,
    case  when  sflag = '5min' then web_uv else 0 end as web_uv_5min,
    
    case  when  sflag = 'time' then web_pv/60/web_uv else 0.0 end as avg_time
    from 
    (
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    count(*) as web_pv,
    count(distinct iuin) as web_uv ,
    'total' as sflag
    from tb_lol_cellphone_zhibo_data_temp
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
    
    
    union all 
    
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    count(*) as web_pv,
    count(distinct iuin) as web_uv ,
    '10s' as sflag
    from tb_lol_cellphone_zhibo_data_temp where ionlinetime >= 10
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
  
    union all 
    
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    count(*) as web_pv,
    count(distinct iuin) as web_uv ,
    '1min' as sflag
    from tb_lol_cellphone_zhibo_data_temp where ionlinetime >= 60
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
  
   union all 
    
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    count(*) as web_pv,
    count(distinct iuin) as web_uv ,
    '5min' as sflag
    from tb_lol_cellphone_zhibo_data_temp where ionlinetime >= 300
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
  
    union all 
    
    
    select 
    idx,
    smatchid,
    imatchsubid,
    sname,
    sstarttime,
    sendtime,
    sum(ionlinetime) as web_pv,
    count(distinct iuin) as web_uv ,
    'time' as sflag
    from tb_lol_cellphone_zhibo_data_temp 
    group by idx,    smatchid,    imatchsubid,    sname,    sstarttime,    sendtime
  
  
    )t
    )t1
    group by idx,smatchid,imatchsubid,sname,sstarttime,sendtime
    )t2
    join 
    (
    select 
    idx,
    smatchid,
    sname,
    sstarttime,
    sendtime,
    pcu,
    pcu_time,
    cdn_width,
    acu from  hy::t_dw_mkt_qt_lol_live_pcu where statis_date = %s and ichannelid = 1 and ivideodefinition  = -100
    )t3
    on (t2.smatchid = t3.smatchid and t2.sname = t3.sname and t2.sstarttime = t3.sstarttime and t2.sendtime = t3.sendtime)
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

  


    tdw.WriteLog("== end OK ==")
    
    
    

    
    
    