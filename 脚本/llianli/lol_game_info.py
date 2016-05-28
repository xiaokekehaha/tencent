#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_game_info.py
# 功能描述:     LOL游戏信息
# 输入参数:     yyyymmdd    例如：20150723
# 目标表名:     
# 数据源表:     ieg_tdbank::tgppallas_dsl_pallas_index_battles_fdt0 
# 创建人名:     llianli
# 创建日期:     2015-12-27
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

    sDate = argv[0]
    today_str=sDate
    
    today_date = datetime.date(int(today_str[0:4]), int(today_str[4:6]), int(today_str[6:8]))
    today_str = today_date.strftime("%Y%m%d")

    
    pre_1_date = today_date - datetime.timedelta(days = 1)
    pre_1_date_str = pre_1_date.strftime("%Y%m%d")

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表，统计的原始表
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_lol_game_info
(
dtstatdate INT COMMENT '统计日期',
game_id INT COMMENT '游戏战局信息',
battle_type INT COMMENT '战局类型',
qq_uin INT COMMENT '用户qq',
win INT COMMENT '用户是否获胜',
area_id INT COMMENT '大区ID',
champion_id INT COMMENT '英雄ID'
)PARTITION BY LIST (dtstatdate)
(
                partition p_20151002  VALUES IN (20151002),
                partition p_20151003  VALUES IN (20151003)
                )  '''
            
    res = tdw.execute(sql)


    sql=''' ALTER TABLE   tb_lol_game_info DROP PARTITION (p_%s)''' % (today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' ALTER TABLE tb_lol_game_info ADD PARTITION p_%s VALUES IN (%s)'''%(today_str,today_str)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##处理数据写入这个表中用以进行计算
    sql = ''' 
    INSERT TABLE tb_lol_game_info
SELECT 
%s AS dtstatdate,
t.game_id ,
t.battle_type,
t1.qquin,
t1.win,
t1.area_id,
t1.champion_id
FROM
(
SELECT 
area_id,
game_id,
battle_type
FROM ieg_tdbank::tgppallas_dsl_pallas_index_battles_fdt0 WHERE tdbank_imp_date = '%s'
)t 
JOIN
(
SELECT 
game_id,
qquin,
win,
area_id,
champion_id
FROM ieg_tdbank::tgppallas_dsl_pallas_index_battle_player_fdt0 WHERE tdbank_imp_date = '%s'
)t1
ON (t.game_id = t1.game_id AND t.area_id = t1.area_id)
    '''%(pre_1_date_str,today_str,today_str)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
     
    tdw.WriteLog("== end OK ==")
    
    
    
    