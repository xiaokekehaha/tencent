#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     qtx_lol_penetrance.py
# 功能描述:     掌上英雄联盟功能渗透率统计
# 输入参数:     yyyymmdd    例如：20140113
# 目标表名:     ieg_qt_community_app.qtx_lol_penetrance
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


    tdw.WriteLog("== argv[0] = " + argv[0] + " ==")

    sDate = argv[0];
    ##sDate = '20150111'

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = """
            CREATE TABLE IF NOT EXISTS qtx_lol_penetrance
            (
            sdate int,
            id bigint,
            av string,
            total_uin bigint,
            infolistcnt bigint,
            friendlistcnt bigint,
            chatlistcnt bigint,
            findlistcnt bigint,
            myselflistcnt bigint,
            heroinfolistcnt bigint,
            gainsdetailcnt bigint,
            infodetailcnt bigint,
            total_use_uin bigint,
            playerinfoviewcnt bigint,
            heroskincnt bigint,
            commentcnt bigint,
            commentwritecnt bigint,
            nearbypeoplecnt bigint,
            
            battle_list_cnt bigint,
            asset_cnt bigint,
            ability_cnt bigint,
            win_detail_cnt bigint,
            hero_time_cnt bigint,
            
            knowledge_college_cnt bigint,
            hero_time_view_cnt bigint,
            battle_assist_cnt bigint,
            goods_view_cnt bigint,
            goods_detail_vies_cnt bigint,
            
            battle_assist_total_cnt bigint,
            battle_assist_push_cnt bigint,
            battle_assist_find_cnt bigint,
            
            talent_simulate_cnt bigint,
            rune_simulate bigint,
            
            club_uin bigint,
            
            wall_paper bigint,
            user_tag bigint,
            
            personal_msg_cnt bigint,
            
            
            match_center_cnt bigint COMMENT '赛事中心渗透率统计' ,
            cost_record_cnt bigint COMMENT '消费记录渗透率统计' ,
            
            netbar_list bigint COMMENT '附近网吧列表',
            netbar_detail bigint COMMENT '附近网吧详情'
            ) """
    res = tdw.execute(sql)

    sql="""delete from qtx_lol_penetrance where sdate=%s  """ % (sDate)
    res = tdw.execute(sql)

 
    sql = """
            insert  table qtx_lol_penetrance
            select
            cast(t1.sdate as int) as sdate,
            cast(t1.id as bigint) as id,
            cast(t1.av as string) as av,
            cast(t2.total_uin  as bigint) as total_uin,
            cast(t1.info_list  as bigint) as info_list,
            cast(t1.frient_list  as bigint) as frient_list,
            cast(t1.chat_list  as bigint) as chat_list,
            cast(t1.find_list  as bigint) as find_list,
            cast(t1.myself_list  as bigint) as myself_list,
            cast(t1.hero_info_list  as bigint) as hero_info_list,
            cast(t1.gains_detail  as bigint) as gains_detail,
            cast(t1.info_detail  as bigint) as info_detail,
            cast(t1.total_use_uin  as bigint) as total_use_uin,
            cast(t1.playerinfo_view_uin  as bigint) as playerinfo_view_uin,
            cast(t1.hero_skin_uin  as bigint) as hero_skin_uin,
            cast(t1.comment_uin  as bigint) as comment_uin,
            cast(t1.comment_write_uin  as bigint) as comment_write_uin,
            cast(t1.nearby_people_uin as bigint) as nearby_people_uin  ,
            
            cast(t1.battle_list as bigint) as battle_list  ,
            cast(t1.asset as bigint) as asset  ,
            cast(t1.ability as bigint) as ability  ,
            cast(t1.win_detail as bigint) as win_detail  ,
            cast(t1.hero_time as bigint) as hero_time  ,
            
            cast(t1.knowledge_college as bigint) as knowledge_college_cnt  ,
            cast(t1.hero_time_view as bigint) as hero_time_view_cnt  ,
            cast(t1.battle_assist as bigint) as battle_assist_cnt  ,
            cast(t1.goods_view as bigint) as goods_view_cnt  ,
            cast(t1.goods_detail_view as bigint) as goods_detail_view_cnt  ,
            
            
            cast(t1.battle_assist_total as bigint) as battle_assist_total_cnt   ,
            cast(t1.battle_assist_push as bigint) as battle_assist_push_cnt   ,
            cast(t1.battle_assist_find as bigint) as battle_assist_find_cnt   ,
            
            cast(t1.talent_simulate as bigint) as talent_simulate_cnt,   
            
            cast(t1.rune_simulate as bigint) as rune_simulate,
            cast(t1.club_uin as bigint) as club_uin   ,
            
            cast(t1.wall_paper as bigint) as wall_paper   ,
            
            cast(t1.user_tag as bigint) as user_tag,
            
            cast(t1.personal_msg as bigint) as personal_msg,
            
            cast(t1.match_center_uin as bigint) as match_center_uin,
            cast(t1.cost_record_uin as bigint) as cost_record_uin,
            
            cast(t1.netbar_list_uin as bigint) as netbar_list_uin,
            cast(t1.netbar_detail_uin as bigint) as netbar_detail_uin
            
            from
            (
            select
            sdate,
            id,
            av,
            case when info_list > 0 then info_list - 1 else cast(0 as double) end as info_list,
            case when frient_list > 0 then frient_list - 1 else cast(0 as double) end as frient_list,
            case when chat_list > 0 then chat_list - 1 else cast(0 as double) end as chat_list,
            case when find_list > 0 then find_list - 1 else cast(0 as double) end as find_list,
            case when myself_list > 0 then myself_list - 1 else cast(0 as double) end as myself_list,
            case when hero_info_list > 0 then hero_info_list - 1 else cast(0 as double) end as hero_info_list,
            case when gains_detail > 0 then gains_detail - 1 else cast(0 as double) end as gains_detail,
            case when info_detail > 0 then info_detail - 1 else cast(0 as double) end as info_detail,
            case when total_use_uin > 0 then total_use_uin - 1 else cast(0 as double) end as total_use_uin,
            case when playerinfo_view_uin > 0 then playerinfo_view_uin - 1 else cast(0 as double) end as playerinfo_view_uin, 
            case when hero_skin_uin > 0 then hero_skin_uin - 1 else cast(0 as double) end as hero_skin_uin,
            case when comment_uin > 0 then comment_uin - 1 else cast(0 as double) end as comment_uin,
            case when comment_write_uin > 0 then comment_write_uin - 1 else cast(0 as double) end as comment_write_uin,
            case when nearby_people_uin > 0 then nearby_people_uin - 1 else cast(0 as double) end as nearby_people_uin,
            
            case when battle_list > 0 then battle_list - 1 else cast(0 as double) end as battle_list,
            case when asset > 0 then asset - 1 else cast(0 as double) end as asset,
            case when ability > 0 then ability - 1 else cast(0 as double) end as ability,
            case when win_detail > 0 then win_detail - 1 else cast(0 as double) end as win_detail,
            case when hero_time > 0 then hero_time - 1 else cast(0 as double) end as hero_time,
            
            case when knowledge_college > 0 then knowledge_college - 1 else cast(0 as double) end as knowledge_college,
            case when hero_time_view > 0 then hero_time_view - 1 else cast(0 as double) end as hero_time_view,
            case when battle_assist > 0 then battle_assist - 1 else cast(0 as double) end as battle_assist,
            case when goods_view > 0 then goods_view - 1 else cast(0 as double) end as goods_view,
            case when goods_detail_view > 0 then goods_detail_view - 1 else cast(0 as double) end as goods_detail_view,
            
            case when battle_assist_total > 0 then battle_assist_total - 1 else cast(0 as double) end as battle_assist_total,
            case when battle_assist_push > 0 then battle_assist_push - 1 else cast(0 as double) end as battle_assist_push,
            case when battle_assist_find > 0 then battle_assist_find - 1 else cast(0 as double) end as battle_assist_find,
            
            case when talent_simulate > 0 then talent_simulate - 1 else cast(0 as double) end as talent_simulate,
            
            case when rune_simulate > 0 then rune_simulate - 1 else cast(0 as double) end as rune_simulate,
            case when club_uin > 0 then club_uin - 1 else cast(0 as double) end as club_uin,
            case when wall_paper > 0 then wall_paper - 1 else cast(0 as double) end as wall_paper,
            
            case when user_tag > 0 then user_tag - 1 else cast(0 as double) end as user_tag ,
            
            case when personal_msg > 0 then personal_msg - 1 else cast(0 as double) end as personal_msg,
            
            case when match_center_uin > 0 then match_center_uin - 1 else cast(0 as double) end as match_center_uin,
            case when cost_record_uin > 0 then cost_record_uin - 1 else cast(0 as double) end as cost_record_uin,
            
            case when netbar_list_uin > 0 then netbar_list_uin - 1 else cast(0 as double) end as netbar_list_uin,
            case when netbar_detail_uin > 0 then netbar_detail_uin - 1 else cast(0 as double) end as netbar_detail_uin

            
            from
            (
            select
            sdate,
            id,
            av,
            count(distinct info_list) as info_list,
            count(distinct frient_list) as frient_list,
            count(distinct chat_list) as chat_list,
            count(distinct find_list) as find_list,
            count(distinct myself_list) as myself_list,
            count(distinct hero_info_list) as hero_info_list,
            count(distinct gains_detail) as gains_detail,
            count(distinct info_detail) as info_detail,
            count(distinct total_use_uin) as total_use_uin,
            count(distinct playerinfo_view_uin) as  playerinfo_view_uin,
            count(distinct hero_skin_uin) as  hero_skin_uin,  
            count(distinct comment_uin) as comment_uin,
            count(distinct comment_write_uin) as comment_write_uin,
            count(distinct nearby_people_uin) as nearby_people_uin,
            
            count(distinct battle_list) as battle_list,
            count(distinct asset) as asset,
            count(distinct ability) as ability,
            count(distinct win_detail) as win_detail,
            count(distinct hero_time) as hero_time ,
            
            count(distinct knowledge_college) as knowledge_college,
            count(distinct hero_time_view) as hero_time_view,
            count(distinct battle_assist) as battle_assist,
            count(distinct goods_view) as goods_view,
            count(distinct goods_detail_view) as goods_detail_view,
            
            count(distinct battle_assist_total) as battle_assist_total,
            count(distinct battle_assist_push) as battle_assist_push,
            count(distinct battle_assist_find) as battle_assist_find,
            
            count(distinct talent_simulate) as talent_simulate,
            
            count(distinct rune_simulate) as rune_simulate,
            
            count(distinct club_uin) as club_uin,
            
            count(distinct wall_paper) as wall_paper,
            
            count(distinct user_tag) as user_tag,
            
            count(distinct personal_msg) as personal_msg,
            
            count(distinct match_center_uin) as match_center_uin,
            count(distinct cost_record_uin) as cost_record_uin,
            
            count(distinct netbar_list_uin) as netbar_list_uin,
            count(distinct netbar_detail_uin) as netbar_detail_uin
            
            from
            (
            select
            sdate,
            id,
             'all' as av,
            case
            when pi like '%%com.tencent.qt.qtl.activity.info.InfoBaseActivity%%'  or pi like '%%NewsTabViewController%%'
            then concat(ui,mc)
            else cast(0  as string)
            end as info_list,

            case
            when pi like '%%FriendFragment%%' or pi like '%%ExtendedConversationFragment%%' or pi like '%%FriendsViewController%%'
            then concat(ui,mc)
            else cast(0  as string)
            end as frient_list,

            case
            when pi like '%%com.tencent.qt.qtl.activity.friend.ChatActivity%%'  or pi like '%%QTChatViewController%%'
            then concat(ui,mc)
            else cast(0  as string)
            end as chat_list,

            case
            when pi like '%%com.tencent.qt.qtl.activity.find.FindActivity%%'  or pi like '%%LocateViewController%%'
            then concat(ui,mc)
            else cast(0  as string)
            end as find_list,

            case when pi like '%%com.tencent.qt.qtl.activity.sns.MyInfoActivity%%'  or pi like '%%我%%'
            then concat(ui,mc)
            else cast(0  as string)
            end as myself_list,

            case
            when pi like '%%com.tencent.qt.qtl.activity.hero.HeroMainActivity%%'  or pi like '%%HeroMainViewController%%'
            then concat(ui,mc)
            else cast(0  as string)
            end as hero_info_list,

            case
            when pi like '%%com.tencent.qt.qtl.activity.friend.battle.BattleDetailActivity%%'  or pi like '%%BattleDetailViewController%%'
            then concat(ui,mc)
            else cast(0  as string)
            end as gains_detail,

            case
            when pi like '%%com.tencent.qt.qtl.activity.info.NewsDetailXmlActivity%%'  or ei = 'web_kind_statistics' 
            then concat(ui,mc)
            else cast(0  as string)
            end as info_detail,
            
            case
            when 
            pi != 'com.tencent.common.sso.ui.QuickLoginFirstActivity' and
            pi != 'com.tencent.common.sso.ui.NormalLoginActivity'  and 
            pi != 'com.tencent.common.sso.ui.LoginErrorActivity' and 
            pi != 'com.tencent.common.sso.ui.ImageCodeVerifyActivity'  and 
            pi != 'com.tencent.qt.qtl.login.StartupActivity'  and
            pi != 'com.tencent.qt.qtl.login.ImageCodeVerifyActivity'  and  
            pi != 'com.tencent.qt.qtl.activity.login.startup.StartupActivity'  and  
            pi != 'com.tencent.qt.qtl.activity.login.login.LoginActivity'  and    
            pi != 'com.tencent.qt.qtl.activity.main.LauncherActivity' and
            pi != 'com.tencent.common.license.login_ui.QuickLoginFirstActivity' and 
            pi != 'com.tencent.common.license.login_ui.NormalLoginActivity' and 
            pi != 'com.tencent.common.license.login_ui.ImageCodeVerifyActivity' and  
            pi != 'PreLoginViewController' and
            pi != 'LoginViewController' and 
            pi != 'LoginPictureViewController' and 
            pi != 'LancherViewController'   and 
            pi != '-'
            then concat(ui,mc) 
            else cast(0  as string)
            end as total_use_uin,
            
            case
            when pi like '%%RoleDetailActivity%%'  or pi like  '%%PlayerInfoViewController%%'
            then concat(ui,mc)
            else cast(0  as string)
            end as playerinfo_view_uin,
            
            case 
            when pi like '%%HeroMySkinActivity%%' or pi like '%%HeroSkinListActivity%%'   or pi like '%%MineHeroSkinViewController%%' or pi like '%%HeroSkinFlowViewController%%'
            then concat(ui,mc)
            else cast(0 as string)
            end as hero_skin_uin ,
            
            
            case 
            when ei = 'enter_CommentPage' or ei = '进入评论页面'  or pi = 'CommentsViewController'   or
                 ei = 'public_InfoComment'  or ei = '发表评论'    or
                 ei = 'click_ZanComment' or ei = '点击赞'         or
                 ei = 'peply_InfoComment' or ei = '发表回复'      
            then concat(ui,mc)
            else cast(0 as string)
            end as comment_uin ,
            
            
            case 
            when ei = 'public_InfoComment'  or pi = '发表评论'    or
                 ei = 'click_ZanComment' or ei = '点击赞'         or
                 ei = 'peply_InfoComment' or ei = '发表回复'      
            then concat(ui,mc)
            else cast(0 as string)
            end as comment_write_uin ,
            
            case 
                when pi like '%%PeoplenearbyMainActivity%%' or pi like '%%NearbyViewController%%'
                then  concat(ui,mc)
                else cast(0 as string)
            end as nearby_people_uin, 
            
            
            case 
               when ei = 'sns_tab_battlelist' 
               then concat(ui,mc)
               else cast(0 as string)
            end as battle_list,
            
            
            case 
               when ei = 'sns_tab_asset' 
               then concat(ui,mc)
               else cast(0 as string)
            end as asset,
            
            
            case 
               when ei = 'sns_tab_ability' 
               then concat(ui,mc)
               else cast(0 as string)
            end as ability,
            
            
            case 
               when pi like '%%WinDetailActivity%%' or pi like '%%PlayerBattleDetailVIewController%%' 
               then concat(ui,mc)
               else cast(0 as string)
            end as win_detail,
            
            
            
            case 
               when pi like '%%GameCoolVideoListActivity%%' or pi like '%%MineHeroTimeViewController%%' 
               then concat(ui,mc)
               else cast(0 as string)
            end as hero_time,
            
            
            case 
               when ei = '发现模块' and get_json_object(kv,'$.title') = '知识学院'
               then concat(ui,mc)
               else cast(0 as string)
            end as knowledge_college,
            
            
            case 
               when ei = '发现模块' and get_json_object(kv,'$.title') = '英雄时刻'
               then concat(ui,mc)
               else cast(0 as string)
            end as hero_time_view,
            
            
            case 
               when pi like '%%CurrentMatchViewController%%' or pi like '%%RealTimeBattleActivity%%' 
               then concat(ui,mc)
               else cast(0 as string)
            end as battle_assist,
            
            
            case 
               when pi like '%%GoodsDataViewController%%' or pi like '%%ItemMainActivity%%' 
               then concat(ui,mc)
               else cast(0 as string)
            end as goods_view,
            
            
            case 
               when pi like '%%GoodsDataDetailViewController%%' or pi like '%%ItemDetailActivity%%' 
               then concat(ui,mc)
               else cast(0 as string)
            end as goods_detail_view,
            
            
            
            case 
               when pi like '%%CurrentMatchViewController%%' or pi like '%%com.tencent.qt.qtl.activity.battle.RealTimeBattleActivity%%' 
               then concat(ui,mc)
               else cast(0 as string)
            end as battle_assist_total,
            
            
            case 
               when ei = 'real_time_push_open'  
               then concat(ui,mc)
               else cast(0 as string)
            end as battle_assist_push,
            
            
            case 
               when ei = '发现模块' and get_json_object(kv,'$.title') = '对战助手'
               then concat(ui,mc)
               else cast(0 as string)
            end as battle_assist_find,
            
            
            case 
               when  ei = 'talent_load' or (ei = '发现模块' and get_json_object(kv,'$.title') = '天赋模拟器')
               then concat(ui,mc)
               else cast(0 as string)
            end as talent_simulate,
            
            case 
                when pi like '%%RuneMainActivity%%' or pi like '%%RuneSimulatorViewController%%'
                then concat(ui,mc)
                else cast(0 as string)
            end as rune_simulate,
            
            

            
            case 
                when pi like '%%com.tencent.qt.qtl.activity.club.ClubSquareActivity%%' or pi like '%%com.tencent.qt.qtl.activity.club.ClubMainPageActivity%%' or pi like '%%com.tencent.qt.qtl.activity.club.PostDetailActivity%%'  or 
                     pi like '%%ClubMainViewController%%' or pi like '%%ClubSquareViewController%%' or pi like '%%ClubFansCircleDetailViewController%%' or pi like '%%ClubTopicContentDetailViewController%%'
                then concat(ui,mc)
                else cast(0 as string)
            end as club_uin,
            
            
            
            case 
                when pi like '%%com.tencent.qt.qtl.activity.wallpaper.WallpaperMainActivity%%'  or 
                     pi like '%%WallPaperViewController%%'
                then concat(ui,mc)
                else cast(0 as string)
            end as wall_paper,
            
            case 
                when ei = 'user_tag_total_count' 
                then  concat(ui,mc) 
                else cast(0 as string)
           end as user_tag,
           
           case 
               when pi = 'TouchWithMeViewController' or pi = 'com.tencent.qt.qtl.activity.topic.PersonalMsgBoxActivity'
               then concat(ui,mc) 
               else cast(0 as string)
           end as personal_msg,
           
           
           case 
               when ei = 'competition_card_touch' or 
                    (id = 1200678382 and ei = '资讯TAB' and get_json_object(kv,'$.tabindex') = '赛事') or
                    (id = 1100678382 and ei = '资讯分类' and get_json_object(kv,'$.type') = '资讯赛事')    
               then concat(ui,mc) 
               else cast(0 as string)
           end as match_center_uin,
           
           
           case 
               when pi like  '%%GiftList%%' or pi like '%%PurchaseList%%'
                    or pi like '%%购买记录%%' or  pi like '%%礼品记录%%'
               then concat(ui,mc) 
               else cast(0 as string)
           end as cost_record_uin,
           
           
           case 
               when pi like  '%%com.tencent.qt.qtl.activity.internet_cafes.NetCafeListActivity%%' or pi like '%%InternetBarTableViewController%%'
               then concat(ui,mc) 
               else cast(0 as string)
           end as netbar_list_uin,
           
           
           case 
               when ei = 'netbar_detail'
               then concat(ui,mc) 
               else cast(0 as string)
           end as netbar_detail_uin
            

            
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100678382,1200678382)
            )t  group by sdate,id, av
            )t3
            )t1
            join
            (
            select
            sdate,
            id,
            av,
            count(distinct uin_info) as total_uin
            from
            (
            select
            sdate,
            id,
            'all' as av,
            case 
                when id = 1100678382 and et = 2 then cast(0 as string)
                when id = 1100678382 and et = 1000 and ei in ('断线重连','断线重连失败','Protocol_Fail_Multi') then cast (0 as string) 
                else concat(ui,mc) 
            end as uin_info
            from teg_mta_intf::ieg_lol where sdate=%s and id in (1100678382,1200678382)
            )j_1  group by sdate,id,av
            )t2  on (t1.id=t2.id and t1.av=t2.av and t1.sdate=t2.sdate ) 

                    """ % (sDate,sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)

    
    tdw.WriteLog("== end OK ==")
    
    
    
    