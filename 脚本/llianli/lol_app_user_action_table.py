#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_user_action_table.py
# 功能描述:     lol app 用户行为宽表
# 输入参数:     yyyymmdd    例如：20151208
# 目标表名:     
# 数据源表:     teg_mta_intf::ieg_lol
# 创建人名:     llianli
# 创建日期:     2015-12-08
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module


# main entry
import datetime
import time


def TDW_PL(tdw, argv=[]):

    tdw.WriteLog("== begin ==")


    #tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    #sDate = argv[0]
    
    

    #tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)




    
    
    
    #形成用户宽表
    
    
    
    sql = '''
    INSERT OVERWRITE TABLE tb_lol_app_use_all_action_new
SELECT 
t.*,

CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.min_time END AS min_time,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.avg_time END AS avg_time,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.max_time END AS max_time,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.pre_min_time END AS pre_min_time,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.pre_avg_time END AS pre_avg_time,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.pre_max_time END AS pre_max_time,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.min_time_delta END AS min_time_delta,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.avg_time_delta END AS avg_time_delta,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.max_time_delta END AS max_time_delta,


CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.min_pv END AS min_pv,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.avg_pv END AS avg_pv,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.max_pv END AS max_pv,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.pre_min_pv END AS pre_min_pv,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.pre_avg_pv END AS pre_avg_pv,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.pre_max_pv END AS pre_max_pv,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.min_pv_delta END AS min_pv_delta,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.avg_pv_delta END AS avg_pv_delta,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.max_pv_delta END AS max_pv_delta,


CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.min_path END AS min_path,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.avg_path END AS avg_path,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.max_path END AS max_path,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.pre_min_path END AS pre_min_path,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.pre_avg_path END AS pre_avg_path,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.pre_max_path END AS pre_max_path,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.min_path_delta END AS min_path_delta,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.avg_path_delta END AS avg_path_delta,
CASE WHEN t1.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t1.max_path_delta END AS max_path_delta,

CASE WHEN t2.uinmac IS NULL THEN -10000 ELSE t2.min_session END AS min_session,
CASE WHEN t2.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t2.avg_session END AS avg_session,
CASE WHEN t2.uinmac IS NULL THEN -10000 ELSE t2.max_session END AS max_session,

CASE WHEN t2.uinmac IS NULL THEN -10000 ELSE t2.pre_min_session END AS pre_min_session,
CASE WHEN t2.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t2.pre_avg_session END AS pre_avg_session,
CASE WHEN t2.uinmac IS NULL THEN -10000 ELSE t2.pre_max_session END AS pre_max_session,

CASE WHEN t2.uinmac IS NULL THEN -10000 ELSE t2.delta_min_session END AS delta_min_session,
CASE WHEN t2.uinmac IS NULL THEN cast(-10000.0 as float) ELSE t2.delta_avg_session END AS delta_avg_session,
CASE WHEN t2.uinmac IS NULL THEN -10000 ELSE t2.delta_max_session END AS delta_max_session,


CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.info_list END AS info_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.friend_list END AS friend_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.chat_list END AS chat_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.find_list END AS find_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.me_list END AS me_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.hero_info END AS hero_info,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.battle_detail END AS battle_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.info_detail END AS info_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.playerinfo_view_list END AS playerinfo_view_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.skin END AS skin,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.chat_list END AS chat_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.nearby_people END AS nearby_people,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.sns_battle_list END AS sns_battle_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.sns_asset END AS sns_asset,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.sns_ability END AS sns_ability,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.win_detail END AS win_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.hero_time END AS hero_time,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.knowledge_college END AS knowledge_college,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.hero_time_watch END AS hero_time_watch,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.goods_info END AS goods_info,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.goods_detail END AS goods_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.battle_assist END AS battle_assist,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.gift_simulate END AS gift_simulate,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.rune_simulate END AS rune_simulate,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.club END AS club,

CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_info_list END AS pre_info_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_friend_list END AS pre_friend_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_chat_list END AS pre_chat_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_find_list END AS pre_find_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_me_list END AS pre_me_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_hero_info END AS pre_hero_info,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_battle_detail END AS pre_battle_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_info_detail END AS pre_info_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_playerinfo_view_list END AS pre_playerinfo_view_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_skin END AS pre_skin,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_chat_list END AS pre_chat_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_nearby_people END AS pre_nearby_people,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_sns_battle_list END AS pre_sns_battle_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_sns_asset END AS pre_sns_asset,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_sns_ability END AS pre_sns_ability,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_win_detail END AS pre_win_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_hero_time END AS pre_hero_time,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_knowledge_college END AS pre_knowledge_college,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_hero_time_watch END AS pre_hero_time_watch,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_goods_info END AS pre_goods_info,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_goods_detail END AS pre_goods_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_battle_assist END AS pre_battle_assist,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_gift_simulate END AS pre_gift_simulate,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_rune_simulate END AS pre_rune_simulate,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.pre_club END AS pre_club,


CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_info_list END AS delta_info_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_friend_list END AS delta_friend_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_chat_list END AS delta_chat_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_find_list END AS delta_find_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_me_list END AS delta_me_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_hero_info END AS delta_hero_info,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_battle_detail END AS delta_battle_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_info_detail END AS delta_info_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_playerinfo_view_list END AS delta_playerinfo_view_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_skin END AS delta_skin,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_chat_list END AS delta_chat_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_nearby_people END AS delta_nearby_people,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_sns_battle_list END AS delta_sns_battle_list,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_sns_asset END AS delta_sns_asset,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_sns_ability END AS delta_sns_ability,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_win_detail END AS delta_win_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_hero_time END AS delta_hero_time,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_knowledge_college END AS delta_knowledge_college,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_hero_time_watch END AS delta_hero_time_watch,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_goods_info END AS delta_goods_info,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_goods_detail END AS delta_goods_detail,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_battle_assist END AS delta_battle_assist,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_gift_simulate END AS delta_gift_simulate,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_rune_simulate END AS delta_rune_simulate,
CASE WHEN t3.uinmac IS NULL THEN -10000 ELSE t3.delta_club END AS delta_club,


CASE WHEN t4.iuin is null then -10000 ELSE t4.level END AS max_level,
CASE WHEN t4.iuin is null then -10000 ELSE t4.tier END AS min_tier,

CASE WHEN t5.iuin IS NULL THEN -10000 ELSE t5.igamelostflag END AS igamelostflag

FROM
 tb_lol_app_effect_static_uin_mac_new t  
 LEFT OUTER JOIN
 tb_lol_app_pi_total_ana_new t1 
 ON (t.uinmac = t1.uinmac and t.id = t1.id) 
 LEFT OUTER JOIN
 tb_lol_app_si_ana_new t2
 ON (t.uinmac = t2.uinmac and t.id = t2.id)
 LEFT OUTER JOIN
 tb_lol_app_ei_ana_new t3
 ON (t.uinmac = t3.uinmac and t.id = t3.id)
 LEFT OUTER JOIN
 tb_app_user_level_in_game t4
 ON (t.iuin = t4.iuin)
  LEFT OUTER JOIN
 tb_lol_app_game_lost_data t5
 ON (t.iuin = t5.iuin)  
    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    