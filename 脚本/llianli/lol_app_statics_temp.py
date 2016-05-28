#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_statics_temp.py
# 功能描述:     掌盟临时数据统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.tb_temp_lol_app_black_battle_info_20160302
# 目标表名:     ieg_qt_community_app.tb_temp_lol_app_normal_battle_info_20160302
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2014-10-29
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    tdw.WriteLog("== temp = " + 'temp' + " ==")

    ##sDate = argv[0];
    ##sDate = '20141201'

    ##tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

##    sql = """set hive.inputfiles.splitbylinenum=true"""
##    res = tdw.execute(sql)
##    sql = """set hive.inputfiles.line_num_per_split=1000000"""
##    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS tb_temp_lol_app_black_battle_info_20160302
            (
            uin BIGINT COMMENT '黑名单用户的ID',
            battle_times BIGINT COMMENT '被封前七天对战的总局数',
            game_days BIGINT COMMENT '被封前游戏的天数'
            ) """
    #tdw.WriteLog(sql)
    #res = tdw.execute(sql)

    sql = """
            INSERT OVERWRITE TABLE tb_temp_lol_app_black_battle_info_20160302
            SELECT
            uin,
            COUNT(DISTINCT game_id) AS battle_times,
            COUNT(DISTINCT dtstatdate) AS game_days 
            FROM 
            (
            SELECT 
            t.uin AS uin,
            CASE 
                WHEN t1.dtstatdate >= t.i7daybeforeblackdate AND t1.dtstatdate <= t.iblackdate 
                THEN t1.game_id 
            ELSE NULL
            END AS game_id ,
            
            
            CASE 
                WHEN t1.dtstatdate >= t.i7daybeforeblackdate AND t1.dtstatdate <= t.iblackdate 
                THEN t1.dtstatdate 
            ELSE NULL
            END AS dtstatdate 
            
            FROM 
            tb_temp_lol_app_black_uin_20160302  t
            JOIN 
            (
            SELECT
            CAST(date_sub(tdbank_imp_date , 1 ) AS INT) as dtstatdate,
            qquin,
            game_id
            FROM
            ieg_tdbank::tgppallas_dsl_pallas_index_battle_player_fdt0 
            WHERE tdbank_imp_date BETWEEN '20150502' AND '20160301'   
            )t1
            ON(t.uin = t1.qquin)
            )t2
            GROUP BY uin
                    """ 
    
    #tdw.WriteLog(sql)
    #res = tdw.execute(sql)

    sql = """
    CREATE TABLE IF NOT EXISTS tb_temp_lol_app_normal_battle_info_20160302
    (
    uin BIGINT COMMENT '黑名单用户的ID',
    battle_times BIGINT COMMENT '被封前七天对战的总局数',
    game_days BIGINT COMMENT '被封前游戏的天数'
    ) 
    """
    #tdw.WriteLog(sql)
    #res = tdw.execute(sql)
    
    
    sql = """
    INSERT OVERWRITE TABLE tb_temp_lol_app_normal_battle_info_20160302
    SELECT
    uin,
    COUNT(DISTINCT game_id) AS battle_times,
    COUNT(DISTINCT dtstatdate) AS game_days 
    FROM 
    (
    SELECT 
    t.uin AS uin,
    CASE 
        WHEN t1.dtstatdate >= t.i7daysbeforelastdate  AND t1.dtstatdate <= t.ilastdate  
        THEN t1.game_id 
    ELSE NULL
    END AS game_id ,
    
    
    CASE 
        WHEN t1.dtstatdate >= t.i7daysbeforelastdate  AND t1.dtstatdate <= t.ilastdate  
        THEN t1.dtstatdate 
    ELSE NULL
    END AS dtstatdate 
    
    FROM 
    tb_temp_lol_app_normal_uin_20160302  t
    JOIN 
    (
    SELECT
    CAST(date_sub(tdbank_imp_date , 1 ) AS INT) as dtstatdate,
    qquin,
    game_id
    FROM
    ieg_tdbank::tgppallas_dsl_pallas_index_battle_player_fdt0 
    WHERE tdbank_imp_date BETWEEN '20160126' AND '20160301'   
    )t1
    ON(t.uin = t1.qquin)
    )t2
    GROUP BY uin
 
    """
    ##tdw.WriteLog(sql)
    ##res = tdw.execute(sql)
    
    
    ##ei事件新加入上报
    sql = """
    INSERT TABLE tb_lol_app_ei_new
SELECT 
sdate AS fdate,
id,
ei,
uin_info,
uin,
COUNT(*) AS pv
FROM 
(
SELECT
sdate,
id,
case
            
               
               WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯娱乐') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '娱乐')
            THEN '资讯娱乐' 
            
            
            WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯官方') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '官方')
            THEN '资讯官方' 
            
            
            
            WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯搞笑') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '搞笑')
            THEN '资讯搞笑' 
            
            
            
            WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯收藏') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '收藏')
            THEN '资讯收藏' 
            
            WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯攻略') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '攻略')
            THEN '资讯攻略' 
            
            WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯最新') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '最新')
            THEN '资讯最新' 
            
            
            WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯活动') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '活动')
            THEN '资讯活动' 
            
            
            WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯精华') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '精华')
            THEN '资讯精华' 
            
            
            WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯视频') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '视频')
            THEN '资讯视频' 
            
            
            WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯赛事') OR 
                 (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '赛事')
            THEN '资讯赛事' 
            
            WHEN ( pi = 'com.tencent.qt.qtl.activity.info.InfoSearchActivity'  AND rf = 'com.tencent.qt.qtl.activity.info.InfoBaseActivity' ) OR 
                 ( pi = 'NewsSearchViewController' AND rf = 'NewsTabViewController' ) 
            THEN '搜索框'
            
            ELSE 'other'    
            end as ei,
            
concat(ui,mc) AS uin_info,
get_json_object(kv,'$.uin') AS uin 
FROM  teg_mta_intf::ieg_lol WHERE sdate >= 20160229  AND id in (1100678382,1200678382)
)t1 WHERE  ei != 'other' 
GROUP BY sdate,id,ei,uin_info,uin  
    """
    #tdw.WriteLog(sql)
    #res = tdw.execute(sql)
    
    
    ##年龄、性别数据写入
    sql = """ CREATE TABLE IF NOT EXISTS tb_lol_app_uin_properties_nature
(
statis_month INT COMMENT '统计月份',
appid INT COMMENT '使用的系统是安卓还是IOS 1100678382，1200678382',
uin BIGINT COMMENT '用户UIN',
gender INT COMMENT '1:男，2：女，0：未知',
age INT COMMENT '用户年龄',
cagerange STRING COMMENT '年龄区间，目前按照学历做划分',
profession_id    BIGINT    COMMENT '职业ID' 
)PARTITION BY LIST (statis_month)
            (
            partition p_201602  VALUES IN (201602),
            partition p_201603  VALUES IN (201603)
            )  """
    #tdw.WriteLog(sql)
    #res = tdw.execute(sql)
    
    
    sql = """ ALTER TABLE tb_lol_app_uin_properties_nature DROP PARTITION (p_201603) """
    #tdw.WriteLog(sql)
    #res = tdw.execute(sql)
    
    sql = """ ALTER TABLE tb_lol_app_uin_properties_nature ADD PARTITION p_201603 VALUES IN  (201603)"""
   # tdw.WriteLog(sql)
    #res = tdw.execute(sql)
    
    
    sql = """
    INSERT TABLE tb_lol_app_uin_properties_nature
SELECT
201603 AS statis_month,
a.appid AS appid,
a.iuin AS iuin,
b.gender AS gender,
b.age AS age,
CASE 
    WHEN b.age >= 5 AND b.age <= 12 THEN '5-12'
    WHEN b.age >= 13 AND b.age <= 15 THEN '13-15'
    WHEN b.age >= 16 AND b.age <= 18 THEN '16-18'
    WHEN b.age >= 19 AND b.age <= 22 THEN '19-22'
    WHEN b.age >= 23 AND b.age <= 26 THEN '23-26'
    WHEN b.age >= 27 AND b.age <= 35 THEN '27-35'
    WHEN b.age > 35  AND b.age < 100 THEN '大于35'
    ELSE 'other'
    END 
AS cagerange,
b.profession_id AS profession_id
FROM 
(
SELECT 
DISTINCT 
iuin,
appid
FROM 
(
SELECT
iuin,
CASE WHEN iclienttype = 9 THEN 1100678382 ELSE 1200678382 END AS appid
FROM 
tb_app_original_data WHERE dtstatdate BETWEEN 20160224 AND 20160324 AND iclienttype IN (9,10)
)t 
)a 

JOIN
(
SELECT
qq_num AS iuin,
MAX(age) AS age,
MAX(gender) AS gender,
MAX(profession_id) AS profession_id
FROM 
bic::t_fat_userprofile WHERE statis_month BETWEEN 201601 AND 201602 
GROUP BY qq_num  
)b
ON (a.iuin = b.iuin) 
    """
   # tdw.WriteLog(sql)
   # res = tdw.execute(sql)

    sql = """
    INSERT OVERWRITE TABLE tb_lol_app_lol_game_cross_original_water
SELECT
CASE 
    WHEN a.dtstatdate IS NULL THEN CAST(b.dtstatdate AS INT)
    WHEN a.dtstatdate IS NOT   NULL THEN  a.dtstatdate
END AS dtstatdate,
CASE 
    WHEN a.iuin IS NULL AND b.qq_uin IS NOT NULL THEN 'game'
    WHEN a.iuin IS NOT NULL AND b.qq_uin IS NULL THEN 'app'
    WHEN a.iuin IS NOT NULL AND b.qq_uin IS NOT NULL THEN 'app&game'
END AS sflag,
CASE 
    WHEN a.iuin IS NULL THEN b.qq_uin
    WHEN a.iuin IS NOT   NULL THEN a.iuin
END AS iuin
FROM 
(
SELECT
DISTINCT   
dtstatdate ,
iuin 
FROM 
 tb_app_original_data WHERE  dtstatdate BETWEEN 20160222 AND 20160320 AND iclienttype in (9,10)
 
)a 
FULL OUTER JOIN
(
SELECT
date_sub( tdbank_imp_date,1) AS dtstatdate,qquin AS qq_uin
FROM ieg_tdbank::tgppallas_dsl_pallas_index_battle_player_fdt0 WHERE tdbank_imp_date BETWEEN '20160223' AND '20160321'
GROUP BY  tdbank_imp_date,qquin
)b 
ON(a.dtstatdate = b.dtstatdate AND a.iuin = b.qq_uin)

 
    """
    #tdw.WriteLog(sql)
    #res = tdw.execute(sql)
    
    sql = """ 
    INSERT OVERWRITE TABLE tb_game_app_temp
SELECT 
CASE 
    WHEN dtstatdate IN (20160222,20160223,20160224,20160225) THEN 'work_1'
    WHEN dtstatdate IN (20160229,20160301,20160302,20160303) THEN 'work_2'
    WHEN dtstatdate IN (20160307,20160308,20160309,20160310) THEN 'work_3'
    WHEN dtstatdate IN (20160314,20160315,20160316,20160317) THEN 'work_4'
    
    WHEN dtstatdate IN (20160226,20160227,20160228) THEN 'weednd_1'
    WHEN dtstatdate IN (20160304,20160305,20160306) THEN 'weednd_2'
    WHEN dtstatdate IN (20160311,20160312,20160313) THEN 'weednd_3'
    WHEN dtstatdate IN (20160318,20160319,20160320) THEN 'weednd_4'
    
    ELSE 'other' 
    
END AS sdayflag ,
iuin 
FROM tb_lol_app_lol_game_cross_original_water    
WHERE dtstatdate >= 20160222 AND dtstatdate <= 20160320 AND sflag = 'app&game' AND iuin IS NOT NULL 

    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    INSERT OVERWRITE TABLE tb_game_temp
SELECT 
CASE 
    WHEN dtstatdate IN (20160222,20160223,20160224,20160225) THEN 'work_1'
    WHEN dtstatdate IN (20160229,20160301,20160302,20160303) THEN 'work_2'
    WHEN dtstatdate IN (20160307,20160308,20160309,20160310) THEN 'work_3'
    WHEN dtstatdate IN (20160314,20160315,20160316,20160317) THEN 'work_4'
    
    WHEN dtstatdate IN (20160226,20160227,20160228) THEN 'weednd_1'
    WHEN dtstatdate IN (20160304,20160305,20160306) THEN 'weednd_2'
    WHEN dtstatdate IN (20160311,20160312,20160313) THEN 'weednd_3'
    WHEN dtstatdate IN (20160318,20160319,20160320) THEN 'weednd_4'
    
    ELSE 'other' 
    
END AS sdayflag ,
iuin 
FROM tb_lol_app_lol_game_cross_original_water    
WHERE  dtstatdate >= 20160222 AND dtstatdate <= 20160320 AND  sflag = 'game' AND iuin IS NOT NULL 
     """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    sql = """
    CREATE TABLE IF NOT EXISTS tb_app_temp
(
sdayflag STRING,
iuin BIGINT
)

INSERT OVERWRITE TABLE tb_app_temp
SELECT 
CASE 
    WHEN dtstatdate IN (20160222,20160223,20160224,20160225) THEN 'work_1'
    WHEN dtstatdate IN (20160229,20160301,20160302,20160303) THEN 'work_2'
    WHEN dtstatdate IN (20160307,20160308,20160309,20160310) THEN 'work_3'
    WHEN dtstatdate IN (20160314,20160315,20160316,20160317) THEN 'work_4'
    
    WHEN dtstatdate IN (20160226,20160227,20160228) THEN 'weednd_1'
    WHEN dtstatdate IN (20160304,20160305,20160306) THEN 'weednd_2'
    WHEN dtstatdate IN (20160311,20160312,20160313) THEN 'weednd_3'
    WHEN dtstatdate IN (20160318,20160319,20160320) THEN 'weednd_4'
    
    ELSE 'other' 
    
END AS sdayflag ,
iuin 
FROM tb_lol_app_lol_game_cross_original_water    
WHERE dtstatdate >= 20160222 AND dtstatdate <= 20160320 AND sflag = 'app' AND iuin IS NOT NULL 
 
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    INSERT  TABLE tb_lol_app_cross_game_temp
SELECT
sflag,
cnt
FROM
(
SELECT 
'app&game1-2' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_1'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_2'
)b
ON(a.iuin = b.iuin)

UNION ALL

SELECT 
'app&game2-3' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_2'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_3'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'app&game3-4' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_3'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_4'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'app&game1-4' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_1'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_4'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'app&game1-3' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_1'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_3'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'app&game2-4' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_2'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_app_temp  WHERE sdayflag = 'work_4'
)b
ON(a.iuin = b.iuin)
)t 
     """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    INSERT  TABLE tb_lol_app_cross_game_temp
SELECT
sflag,
cnt
FROM
(
SELECT 
'game1-2' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_1'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_2'
)b
ON(a.iuin = b.iuin)

UNION ALL

SELECT 
'game2-3' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_2'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_3'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'game3-4' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_3'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_4'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'game1-4' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_1'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_4'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'game1-3' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_1'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_3'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'game2-4' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_2'
)a
JOIN
(
SELECT
iuin 
FROM tb_game_temp  WHERE sdayflag = 'work_4'
)b
ON(a.iuin = b.iuin)
)t  
    """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = """
    INSERT  TABLE tb_lol_app_cross_game_temp
SELECT
sflag,
cnt
FROM
(
SELECT 
'app1-2' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_1'
)a
JOIN
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_2'
)b
ON(a.iuin = b.iuin)

UNION ALL

SELECT 
'app2-3' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_2'
)a
JOIN
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_3'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'app3-4' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_3'
)a
JOIN
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_4'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'app1-4' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_1'
)a
JOIN
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_4'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'app1-3' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_1'
)a
JOIN
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_3'
)b
ON(a.iuin = b.iuin)


UNION ALL

SELECT 
'app2-4' AS sflag,
COUNT(*) AS cnt
FROM 
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_2'
)a
JOIN
(
SELECT
iuin 
FROM tb_app_temp  WHERE sdayflag = 'work_4'
)b
ON(a.iuin = b.iuin)
)t
     """
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

    tdw.WriteLog("== end OK ==")
    
    
    
    
    
    
    