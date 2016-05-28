#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_tb_app_actscaledis.py
# 功能描述:     掌盟掌火活跃留存跟踪数据统计
# 输入参数:     yyyymmdd    例如：20160309
# 目标表名:     ieg_qt_community_app.tb_app_actscaledis
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
            CREATE TABLE IF NOT EXISTS tb_app_actscaledis
        (
        dtstatdate INT COMMENT '统计日期',
        sflag STRING COMMENT '活跃或新进的标记位置',
        iclienttype INT COMMENT '客户端类型',
        idaydelta INT COMMENT '与今日相差日期',
        itotaluin BIGINT COMMENT '对应日期总活跃用户数',
        istayuin BIGINT COMMENT '对应日期在当日的留存用户数'
        ) """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql="""DELETE FROM tb_app_actscaledis WHERE dtstatdate = %s AND sflag = 'dayreg' """ % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)


    sql = """
            INSERT TABLE tb_app_actscaledis
            SELECT
            %(sDate)s AS dtstatdate,
            'dayreg' AS sflag,
            iclienttype,
            datediff(%(sDate)s,iregdate) AS idaydelta, 
            count(iuin) as iRegNum,
            sum(case when substr(cbitmap,1,1)='1' then 1 else 0 end) as iActiNum
            FROM  tb_app_reg_account PARTITION (p_%(sDate)s) a WHERE iregdate >= %(pre_30_date_str)s AND iregdate < %(sDate)s
            GROUP BY iclienttype,iregdate

                    """ % (locals())
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##创建临时表存储act数据
    sql = """
    CREATE TABLE IF NOT EXISTS tb_app_round_actiscale_%s_dayact
        (
        iclienttype bigint ,
        OldActi1 bigint,NowActi1 bigint ,
        OldActi2 bigint,NowActi2 bigint ,
        OldActi3 bigint,NowActi3 bigint ,
        OldActi4 bigint,NowActi4 bigint ,
        OldActi5 bigint,NowActi5 bigint ,
        OldActi6 bigint,NowActi6 bigint ,
        OldActi7 bigint,NowActi7 bigint ,
        OldActi8 bigint,NowActi8 bigint ,
        OldActi9 bigint,NowActi9 bigint ,
        OldActi10 bigint,NowActi10 bigint ,
        OldActi11 bigint,NowActi11 bigint ,
        OldActi12 bigint,NowActi12 bigint ,
        OldActi13 bigint,NowActi13 bigint ,
        OldActi14 bigint,NowActi14 bigint ,
        OldActi15 bigint,NowActi15 bigint ,
        OldActi16 bigint,NowActi16 bigint ,
        OldActi17 bigint,NowActi17 bigint ,
        OldActi18 bigint,NowActi18 bigint ,
        OldActi19 bigint,NowActi19 bigint ,
        OldActi20 bigint,NowActi20 bigint ,
        OldActi21 bigint,NowActi21 bigint ,
        OldActi22 bigint,NowActi22 bigint ,
        OldActi23 bigint,NowActi23 bigint ,
        OldActi24 bigint,NowActi24 bigint ,
        OldActi25 bigint,NowActi25 bigint ,
        OldActi26 bigint,NowActi26 bigint ,
        OldActi27 bigint,NowActi27 bigint ,
        OldActi28 bigint,NowActi28 bigint ,
        OldActi29 bigint,NowActi29 bigint ,
        OldActi30 bigint,NowActi30 bigint ,
        OldActi31 bigint,NowActi31 bigint 
        ) STORED AS FORMATFILE COMPRESS 
    """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##数据写入临时表
    sql = """ DELETE FROM tb_app_round_actiscale_%s_dayact """%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ INSERT OVERWRITE TABLE tb_app_round_actiscale_%(sDate)s_dayact

            SELECT
            iclienttype,
            sum(case when substr(cbitmap,1,1)='1' then 1 else 0 end) as OldActi1,
            sum(case when substr(cbitmap,1,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi1,
            
            sum(case when substr(cbitmap,2,1)='1' then 1 else 0 end) as OldActi2,
            sum(case when substr(cbitmap,2,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi2,
            
            
            sum(case when substr(cbitmap,3,1)='1' then 1 else 0 end) as OldActi3,
            sum(case when substr(cbitmap,3,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi3,
            
            sum(case when substr(cbitmap,4,1)='1' then 1 else 0 end) as OldActi4,
            sum(case when substr(cbitmap,4,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi4,
            
            sum(case when substr(cbitmap,5,1)='1' then 1 else 0 end) as OldActi5,
            sum(case when substr(cbitmap,5,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi5,
            
            sum(case when substr(cbitmap,6,1)='1' then 1 else 0 end) as OldActi6,
            sum(case when substr(cbitmap,6,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi6,
            
            sum(case when substr(cbitmap,7,1)='1' then 1 else 0 end) as OldActi7,
            sum(case when substr(cbitmap,7,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi7,
            
            sum(case when substr(cbitmap,8,1)='1' then 1 else 0 end) as OldActi8,
            sum(case when substr(cbitmap,8,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi8,
            
            sum(case when substr(cbitmap,9,1)='1' then 1 else 0 end) as OldActi9,
            sum(case when substr(cbitmap,9,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi9,
            
            sum(case when substr(cbitmap,10,1)='1' then 1 else 0 end) as OldActi10,
            sum(case when substr(cbitmap,10,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi10,
            
            sum(case when substr(cbitmap,11,1)='1' then 1 else 0 end) as OldActi11,
            sum(case when substr(cbitmap,11,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi11,
            
            sum(case when substr(cbitmap,12,1)='1' then 1 else 0 end) as OldActi12,
            sum(case when substr(cbitmap,12,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi12,
            
            sum(case when substr(cbitmap,13,1)='1' then 1 else 0 end) as OldActi13,
            sum(case when substr(cbitmap,13,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi13,
            
            sum(case when substr(cbitmap,14,1)='1' then 1 else 0 end) as OldActi14,
            sum(case when substr(cbitmap,14,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi14,
            
            sum(case when substr(cbitmap,15,1)='1' then 1 else 0 end) as OldActi15,
            sum(case when substr(cbitmap,15,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi15,
            
            sum(case when substr(cbitmap,16,1)='1' then 1 else 0 end) as OldActi16,
            sum(case when substr(cbitmap,16,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi16,
            
            sum(case when substr(cbitmap,17,1)='1' then 1 else 0 end) as OldActi17,
            sum(case when substr(cbitmap,17,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi17,
            
            sum(case when substr(cbitmap,18,1)='1' then 1 else 0 end) as OldActi18,
            sum(case when substr(cbitmap,18,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi18,
            
            sum(case when substr(cbitmap,19,1)='1' then 1 else 0 end) as OldActi19,
            sum(case when substr(cbitmap,19,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi19,
            
            sum(case when substr(cbitmap,20,1)='1' then 1 else 0 end) as OldActi20,
            sum(case when substr(cbitmap,20,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi20,
            
            sum(case when substr(cbitmap,21,1)='1' then 1 else 0 end) as OldActi21,
            sum(case when substr(cbitmap,21,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi21,
            
            sum(case when substr(cbitmap,22,1)='1' then 1 else 0 end) as OldActi22,
            sum(case when substr(cbitmap,22,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi22,
            
            sum(case when substr(cbitmap,23,1)='1' then 1 else 0 end) as OldActi23,
            sum(case when substr(cbitmap,23,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi23,
            
            sum(case when substr(cbitmap,24,1)='1' then 1 else 0 end) as OldActi24,
            sum(case when substr(cbitmap,24,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi24,
            
            sum(case when substr(cbitmap,25,1)='1' then 1 else 0 end) as OldActi25,
            sum(case when substr(cbitmap,25,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi25,
            
            sum(case when substr(cbitmap,26,1)='1' then 1 else 0 end) as OldActi26,
            sum(case when substr(cbitmap,26,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi26,
            
            sum(case when substr(cbitmap,27,1)='1' then 1 else 0 end) as OldActi27,
            sum(case when substr(cbitmap,27,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi27,
            
            sum(case when substr(cbitmap,28,1)='1' then 1 else 0 end) as OldActi28,
            sum(case when substr(cbitmap,28,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi28,
            
            sum(case when substr(cbitmap,29,1)='1' then 1 else 0 end) as OldActi29,
            sum(case when substr(cbitmap,29,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi29,
            
            sum(case when substr(cbitmap,30,1)='1' then 1 else 0 end) as OldActi30,
            sum(case when substr(cbitmap,30,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi30,
            
            sum(case when substr(cbitmap,31,1)='1' then 1 else 0 end) as OldActi31,
            sum(case when substr(cbitmap,31,1)='1' and substr(cbitmap,1,1)='1' then 1 else 0 end) as NowActi31 
            
            FROM  tb_app_reg_account PARTITION (p_%(sDate)s) a 
            GROUP BY iclienttype"""%(locals())
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    ##结果数据先删除 后写入
    sql = """ DELETE FROM tb_app_actscaledis WHERE dtstatdate = %s AND sflag = 'dayact' """ %(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    INSERT TABLE tb_app_actscaledis 
    SELECT
                %(sDate)s AS dtstatdate,
                'dayact' AS sflag,
                iclienttype,
                ideltadays - 1,
                SUM(itotalact) AS itotalact,
                SUM(istayact) AS istayact
                FROM 
                (
                
                SELECT iclienttype,2 as ideltadays,OldActi2 AS itotalact, NowActi2 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,3 as ideltadays,OldActi3 AS itotalact, NowActi3 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,4 as ideltadays,OldActi4 AS itotalact, NowActi4 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,5 as ideltadays,OldActi5 AS itotalact, NowActi5 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,6 as ideltadays,OldActi6 AS itotalact, NowActi6 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,7 as ideltadays,OldActi7 AS itotalact, NowActi7 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,8 as ideltadays,OldActi8 AS itotalact, NowActi8 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,9 as ideltadays,OldActi9 AS itotalact, NowActi9 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,10 as ideltadays,OldActi10 AS itotalact, NowActi10 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,11 as ideltadays,OldActi11 AS itotalact, NowActi11 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,12 as ideltadays,OldActi12 AS itotalact, NowActi12 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,13 as ideltadays,OldActi13 AS itotalact, NowActi13 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,14 as ideltadays,OldActi14 AS itotalact, NowActi14 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,15 as ideltadays,OldActi15 AS itotalact, NowActi15 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,16 as ideltadays,OldActi16 AS itotalact, NowActi16 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,17 as ideltadays,OldActi17 AS itotalact, NowActi17 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,18 as ideltadays,OldActi18 AS itotalact, NowActi18 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,19 as ideltadays,OldActi19 AS itotalact, NowActi19 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,20 as ideltadays,OldActi20 AS itotalact, NowActi20 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,21 as ideltadays,OldActi21 AS itotalact, NowActi21 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,22 as ideltadays,OldActi22 AS itotalact, NowActi22 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,23 as ideltadays,OldActi23 AS itotalact, NowActi23 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,24 as ideltadays,OldActi24 AS itotalact, NowActi24 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,25 as ideltadays,OldActi25 AS itotalact, NowActi25 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,26 as ideltadays,OldActi26 AS itotalact, NowActi26 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,27 as ideltadays,OldActi27 AS itotalact, NowActi27 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,28 as ideltadays,OldActi28 AS itotalact, NowActi28 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,29 as ideltadays,OldActi29 AS itotalact, NowActi29 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,30 as ideltadays,OldActi30 AS itotalact, NowActi30 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact UNION ALL 
                
                SELECT iclienttype,31 as ideltadays,OldActi31 AS itotalact, NowActi31 AS istayact FROM tb_app_round_actiscale_%(sDate)s_dayact
                )t WHERE itotalact != 0
                GROUP BY iclienttype,ideltadays

                 """%(locals())
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """ DROP TABLE tb_app_round_actiscale_%s_dayact"""%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    tdw.WriteLog("== end OK ==")
    
    
    
    