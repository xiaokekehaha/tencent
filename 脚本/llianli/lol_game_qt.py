#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_game_qt.py
# 功能描述:     LOL赛事直播数据写入
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app::tb_lol_match_pv_uv
# 目标表名:     ieg_qt_community_app::tb_lol_match_time_range
# 目标表名:     ieg_qt_community_app::tb_lol_match_pcu
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-12-31
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
    
    ##分区的时间暂时指定为1月6日
    partitin_date_str = sDate

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


    
    sql = """ delete from tb_lol_match_pv_uv where statis_date = %s"""%(date_str1)
    tdw.WriteLog(sql)
    tdw.execute(sql)

    sql = """
        INSERT  TABLE tb_lol_match_pv_uv 
        SELECT
         idx
        ,smatchid
        ,0 
        ,sname
        ,''
        ,sstarttime
        ,sendtime
        ,-100
        ,SUM(view_times) AS pv 
        ,COUNT(DISTINCT iuin) AS uv
        ,SUM(ionlinetime) AS total_time
        ,%s
        ,COUNT(DISTINCT effect_uin) AS effect_uin
        ,SUM(effect_time) AS effect_time
        ,imatchsubid
        FROM 
        (
        SELECT
                             idx,
                            smatchid
                            ,imatchsubid
                            ,sname
                            ,sstarttime
                            ,sendtime
                            ,iuin
                            ,view_times
                            ,ionlinetime
                            ,CASE WHEN ionlinetime >= 60 THEN iuin ELSE NULL END AS effect_uin
                            ,CASE WHEN ionlinetime >= 60 THEN ionlinetime ELSE 0 END AS effect_time 
            FROM
                (
                    SELECT
                            idx,
                            smatchid
                            ,imatchsubid
                            ,sname
                            ,sstarttime
                            ,sendtime
                            ,iuin
                            ,COUNT(*) AS view_times
                            ,SUM(ionlinetime) AS ionlinetime
                        FROM
                            (
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
                            ) t1
                        GROUP BY
                            idx,smatchid,imatchsubid ,sname ,sstarttime  ,sendtime,iuin
                ) t2
                
        )t3
        GROUP BY  idx,smatchid
        ,imatchsubid
        ,sname
        ,sstarttime
        ,sendtime
                    """ %(date_str1,pre_date_str1,date_str1,partitin_date_str,date_str)
    
    tdw.WriteLog(sql)
    tdw.execute(sql)
    


    
    
    ##算掌盟的数据 掌盟数据的原始表：ieg_qt_community_app::tb_lol_app_live_original_data
    sql = """
        INSERT  TABLE tb_lol_match_pv_uv 
        SELECT
         idx
        ,smatchid
        ,1 
        ,sname
        ,''
        ,sstarttime
        ,sendtime
        ,-100
        ,SUM(view_times) AS pv 
        ,COUNT(DISTINCT iuin) AS uv
        ,SUM(ionlinetime) AS total_time
        ,%s
        ,COUNT(DISTINCT effect_uin) AS effect_uin
        ,SUM(effect_time) AS effect_time
        ,imatchsubid
        FROM 
        (
        SELECT
                             idx,
                            smatchid
                            ,imatchsubid
                            ,sname
                            ,sstarttime
                            ,sendtime
                            ,iuin
                            ,view_times
                            ,ionlinetime
                            ,CASE WHEN ionlinetime >= 60 THEN iuin ELSE NULL END AS effect_uin
                            ,CASE WHEN ionlinetime >= 60 THEN ionlinetime ELSE 0 END AS effect_time 
            FROM
                (
                    SELECT
                            idx,
                            smatchid
                            ,imatchsubid
                            ,sname
                            ,sstarttime
                            ,sendtime
                            ,iuin
                            ,COUNT(*) AS view_times
                            ,SUM(ionlinetime) AS ionlinetime
                        FROM
                            (
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
                            ) t1
                        GROUP BY
                            idx,smatchid,imatchsubid ,sname ,sstarttime  ,sendtime,iuin
                ) t2
                
        )t3
        GROUP BY  idx,smatchid
        ,imatchsubid
        ,sname
        ,sstarttime
        ,sendtime
                    """ %(date_str1,pre_date_str1,date_str1,partitin_date_str,date_str)
    
    tdw.WriteLog(sql)
    tdw.execute(sql)

    tdw.WriteLog("== end OK ==")
    
    
    

    
    
    