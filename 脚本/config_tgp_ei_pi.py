#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     config_tgp_ei_pi.py
# 功能描述:      
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     
# 数据源表:      
# 创建人名:     bauerzhou
# 创建日期:     2016-01-11
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:     
# 修改日期:
# 修改原因:
# ******************************************************************************


#import system module

from datetime import date, timedelta

clienttype = "601,602"
mta_id = "1100679521, 1200679521"
mta_table = "teg_mta_intf::ieg_tgp" 

lol_eilist = ['real_time_goto_hero_detail',
              'MatchSubscribe',
              'match_square_features',
              'ClubMainPraise',
              'battle_share',
              '资讯'
              ]
lol_pi_list = [ 'com.tencent.qt.qtl.activity.info.InfoBaseActivity',
                'com.tencent.qt.qtl.activity.info.NewsDetailXmlActivity',
                'com.tencent.qt.qtl.activity.sns.MyInfoActivity',
                'com.tencent.qt.qtl.activity.sns.FriendInfoActivity',
                'com.tencent.qt.qtl.activity.friend.battle.BattleDetailActivity',
                'com.tencent.qt.qtl.activity.topic.TopicActivity',
                
                #ios
                'CommunityViewController',
                'NewsTabViewController',
                '个人中心客态',
                '我',
                'BattleDetailViewController',
                'CommunityViewController'
               ]
#dnf_eilist = ['abcd']
#key:mtgp_gameid value: xxx_eilist
game_dict = {26:lol_eilist }
game_pi_dict = {26:lol_pi_list}

def list_to_str(l):
    if len(l) == 0:
        return ""
    content = ""
    length = len(l)
    for i,item in enumerate(l):
        content += "'" +item+"'"
        if i < length - 1:
            content += ","
    
    return content
 

    
 