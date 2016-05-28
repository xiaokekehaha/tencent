#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lolapp_ei.py
# 功能描述:     lolapp每日访问的事件n数目
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


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0]
    
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


     ##创建表写数据
    sql = '''
      CREATE TABLE IF NOT EXISTS tb_lol_app_ei_new
(
fdate INT,
id INT,
ei STRING,
uin_mac STRING,
uin STRING,
pv BIGINT 
)PARTITION BY LIST (fdate)
                (
                partition p_20160108  VALUES IN (20160108),
                partition p_20160109  VALUES IN (20160109)
                ) 
                      '''
    tdw.WriteLog(sql)   
    res = tdw.execute(sql)


    

    sql=''' ALTER TABLE   tb_lol_app_ei_new DROP PARTITION (p_%s)''' % (sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    sql = ''' ALTER TABLE tb_lol_app_ei_new ADD PARTITION p_%s VALUES IN (%s)'''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

 
    
    ##将每日的数据配置写入表中
    sql = ''' 
     INSERT TABLE tb_lol_app_ei_new

SELECT 
%s AS fdate,
id,
ei,
uin_info,
uin,
COUNT(*) AS pv
FROM 
(
SELECT
id,

case
            when pi like '%%com.tencent.qt.qtl.activity.info.InfoBaseActivity%%'  or pi like '%%NewsTabViewController%%'
            then '资讯列表'

            when pi like '%%FriendFragment%%' or pi like '%%ExtendedConversationFragment%%' or pi like '%%FriendsViewController%%'
            then '好友列表'

            when pi like '%%com.tencent.qt.qtl.activity.friend.ChatActivity%%'  or pi like '%%QTChatViewController%%'
            then '聊天'

            when pi like '%%com.tencent.qt.qtl.activity.find.FindActivity%%'  or pi like '%%LocateViewController%%'
            then '发现'

            when pi like '%%com.tencent.qt.qtl.activity.sns.MyInfoActivity%%'  or pi like '%%我%%'
            then '我'
            when pi like '%%com.tencent.qt.qtl.activity.hero.HeroMainActivity%%'  or pi like '%%HeroMainViewController%%'
            then '英雄资料'

            when pi like '%%com.tencent.qt.qtl.activity.friend.battle.BattleDetailActivity%%'  or pi like '%%BattleDetailViewController%%'
            then  '战绩详情'

            when pi like '%%com.tencent.qt.qtl.activity.info.NewsDetailXmlActivity%%'  or ei = 'web_kind_statistics' 
            then '资讯详情'
            

            when pi like '%%RoleDetailActivity%%'  or pi like  '%%PlayerInfoViewController%%'
            then   '召唤师'
            
            when pi like '%%HeroMySkinActivity%%' or pi like '%%HeroSkinListActivity%%'   or pi like '%%MineHeroSkinViewController%%' or pi like '%%HeroSkinFlowViewController%%'
            then  '皮肤' 
            
             
            when ei = 'enter_CommentPage' or ei = '进入评论页面'  or pi = 'CommentsViewController'   or
                 ei = 'public_InfoComment'  or ei = '发表评论'    or
                 ei = 'click_ZanComment' or ei = '点击赞'         or
                 ei = 'peply_InfoComment' or ei = '发表回复'      
            then  '评论相关' 
            
             
            when ei = 'public_InfoComment'  or pi = '发表评论'    or
                 ei = 'click_ZanComment' or ei = '点击赞'         or
                 ei = 'peply_InfoComment' or ei = '发表回复'      
            then '评论写' 
            
             
            when pi like '%%PeoplenearbyMainActivity%%' or pi like '%%NearbyViewController%%'
                then  '附近的人'
            
            
             
               when ei = 'sns_tab_battlelist' 
               then '个人中心-战绩'
            
            
             
               when ei = 'sns_tab_asset' 
               then '个人中心-资产'
            
            
             
               when ei = 'sns_tab_ability' 
               then '个人中心-能力'
            
            
             
               when pi like '%%WinDetailActivity%%' or pi like '%%PlayerBattleDetailVIewController%%' 
               then '胜率详情'
            
            
            
             
               when pi like '%%GameCoolVideoListActivity%%' or pi like '%%MineHeroTimeViewController%%' 
               then '英雄时刻'
            
            
             
               when ei = '发现模块' and get_json_object(kv,'$.title') = '知识学院'
               then '知识学院'
            
            
             
               when ei = '发现模块' and get_json_object(kv,'$.title') = '英雄时刻'
               then '英雄时刻观看'
            
            

               when pi like '%%GoodsDataViewController%%' or pi like '%%ItemMainActivity%%' 
               then '物品资料'
            
            
             
               when pi like '%%GoodsDataDetailViewController%%' or pi like '%%ItemDetailActivity%%' 
               then  '物品资料详情'
            
            
            
             
               when pi like '%%CurrentMatchViewController%%' or pi like '%%com.tencent.qt.qtl.activity.battle.RealTimeBattleActivity%%' 
               then '对战助手-整体'
            
            
             
               when ei = 'real_time_push_open'  
               then '对战助手-push进入'
            
            
             
               when ei = '发现模块' and get_json_object(kv,'$.title') = '对战助手'
               then '对战助手-发现木块'
            
            
             
               when  ei = 'talent_load' or (ei = '发现模块' and get_json_object(kv,'$.id') = '11035')
               then '天赋模拟器'
            
             
                when pi like '%%RuneMainActivity%%' or pi like '%%RuneSimulatorViewController%%'
                then '符文模拟器'
            
            

            
             
                when pi like '%%com.tencent.qt.qtl.activity.club.ClubSquareActivity%%' or pi like '%%com.tencent.qt.qtl.activity.club.ClubMainPageActivity%%' or pi like '%%com.tencent.qt.qtl.activity.club.PostDetailActivity%%'  or 
                     pi like '%%ClubMainViewController%%' or pi like '%%ClubSquareViewController%%' or pi like '%%ClubFansCircleDetailViewController%%' or pi like '%%ClubTopicContentDetailViewController%%'
                then '俱乐部'
            
            
            
             
                when pi like '%%com.tencent.qt.qtl.activity.wallpaper.WallpaperMainActivity%%'  or 
                     pi like '%%WallPaperViewController%%'
                then '墙纸'
            
             
                when ei = 'user_tag_total_count' 
                then  '用户印象'
           
            
               when pi = 'TouchWithMeViewController' or pi = 'com.tencent.qt.qtl.activity.topic.PersonalMsgBoxActivity'
               then '与我相关'
               
               
               
                
               when ei = 'competition_card_touch' or 
                    (id = 1200678382 and ei = '资讯TAB' and get_json_object(kv,'$.tabindex') = '赛事') or
                    (id = 1100678382 and ei = '资讯分类' and get_json_object(kv,'$.type') = '资讯赛事')    
               then '赛事中心'
           
           
            
               when pi like  '%%GiftList%%' or pi like '%%PurchaseList%%'
                    or pi like '%%购买记录%%' or  pi like '%%礼品记录%%'
               then '资产消费'
           
           
            
               when pi like  '%%com.tencent.qt.qtl.activity.internet_cafes.NetCafeListActivity%%' or pi like '%%InternetBarTableViewController%%'
               then '网吧列表'
           
           
            
               when ei = 'netbar_detail'
               then '网吧详情'
               
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
FROM  teg_mta_intf::ieg_lol WHERE sdate = %s  AND id in (1100678382,1200678382)
)t1 WHERE  ei != 'other' 
GROUP BY id,ei,uin_info,uin                 

    '''%(sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    

 

    tdw.WriteLog("== end OK ==")
    
    
    
    