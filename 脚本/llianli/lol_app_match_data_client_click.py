#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     lol_app_match_data_client_click.py
# 功能描述:     掌盟赛事中心数据统计——客户端点击数据
# 输入参数:     yyyymmdd    例如：20160216
# 目标表名:     ieg_qt_community_app.tb_lol_app_match_client_click
# 数据源表:     teg_mta_intf.ieg_lol
# 创建人名:     llianli
# 创建日期:     2016-02-17
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
    

    tdw.WriteLog("== sDate = " + sDate + " ==")


    tdw.WriteLog("== connect tdw ==")
    sql = """use ieg_qt_community_app"""
    res = tdw.execute(sql)

    sql = """set hive.inputfiles.splitbylinenum=true"""
    res = tdw.execute(sql)
    sql = """set hive.inputfiles.line_num_per_split=1000000"""
    res = tdw.execute(sql)


    sql = '''
    CREATE TABLE IF NOT EXISTS tb_lol_app_match_client_click
    (
    dtstatdate INT COMMENT '统计日期',
    id INT COMMENT 'appid  1100678382：安卓， 1200678382：IOS',
    ei STRING COMMENT '上报事件名称',
    uin_cnt BIGINT COMMENT '点击的去重UIN数目',
    mac_cnt BIGINT COMMENT '点击的去重设备号数目',
    click_cnt BIGINT COMMENT '总点击量',
    use_time BIGINT COMMENT '使用时长'
    )

    '''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    



    sql = """  DELETE FROM tb_lol_app_match_client_click WHERE dtstatdate = %s """ %(sDate)
    
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    

    sql = """
        INSERT TABLE tb_lol_app_match_client_click 
    SELECT
    %s AS dtstatdate,
    id,
    ei,
    COUNT(DISTINCT uin) AS uin_cnt,
    COUNT(DISTINCT uin_info) AS mac_cnt,
    COUNT(*) AS click_cnt,
    SUM(du) AS use_time
    FROM 
    (
    SELECT
    id,
    get_json_object(kv,'$.uin') AS uin,
    concat(ui,mc) AS uin_info,
    du,
    
    CASE 
        WHEN (id = 1100678382 AND ei = '资讯分类' AND get_json_object(kv,'$.type') = '资讯赛事' ) OR  
             (id = 1200678382 AND ei = '资讯TAB' AND get_json_object(kv,'$.tabindex') = '赛事' )
           THEN '赛事TAB'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_goto_list'  ) OR  
             (id = 1200678382 AND ei = 'competition_goto_list'  )
           THEN '全年赛事入口'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_live_card_play'  ) OR  
             (id = 1200678382 AND ei = 'competition_live_card_play'  )
           THEN '直播卡片-直播卡片进入直播专题页'
           
           
           
           WHEN (id = 1100678382 AND ei = 'competition_goto_chat_room'  ) OR  
             (id = 1200678382 AND ei = 'competition_goto_chat_room'  )
           THEN '直播卡片-直播卡片进入聊天室'
           
           
           WHEN (id = 1100678382 AND ei = 'match_square_functions' AND get_json_object(kv,'$.function_name') = '赛事视频' ) OR  
             (id = 1200678382 AND ei = 'competition_goto_video_detail'  )
           THEN '赛事首页功能专区-进入赛事视频'
           
           
           WHEN (id = 1100678382 AND ei = 'match_square_functions' AND get_json_object(kv,'$.function_name') = '赛程订阅' ) OR  
             (id = 1200678382 AND ei = 'competition_goto_subscribe'  )
           THEN '赛事首页功能专区—进入赛程订阅'
           
           
           WHEN (id = 1100678382 AND ei = 'match_square_functions' AND get_json_object(kv,'$.function_name') = '俱乐部' ) OR  
             (id = 1200678382 AND ei = 'competition_function_goto_club'  )
           THEN '赛事首页功能专区—进入俱乐部'
           
           
           WHEN (id = 1100678382 AND ei = 'match_square_functions' AND get_json_object(kv,'$.function_name') = '赛况数据' ) OR  
             (id = 1200678382 AND ei = 'competition_goto_game_data'  )
           THEN '赛事首页功能专区—进入赛况数据'
           
           
           WHEN (id = 1100678382 AND ei = '资讯详情' AND get_json_object(kv,'$.type') = '资讯赛事' ) OR  
             (id = 1200678382 AND ei = '资讯详情' AND get_json_object(kv,'$.type') = '赛事' )
           THEN '赛事资讯'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_finish_subscribe'  ) OR  
             (id = 1200678382 AND ei = 'competition_finish_subscribe'  )
           THEN '资讯首页订阅卡片-完成订阅'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_card_touch'  ) OR  
             (id = 1200678382 AND ei = 'competition_card_touch'  )
           THEN '资讯首页订阅/直播卡片-点击进入'
           
           WHEN (id = 1100678382 AND pi LIKE  '%%MatchDetailActivity%%') OR
                (id = 1200678382 AND pi LIKE '%%CompetitionCentreDetailViewController%%')
           THEN '全年赛事列表—进入赛事详情'
           
           
           
           
           
           WHEN (id = 1100678382 AND ei = 'competition_goto_video_list'  ) OR  
             (id = 1200678382 AND ei = 'competition_goto_video_list'  )
           THEN '赛事详情页-查看赛程视频'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_goto_team_list'  ) OR  
             (id = 1200678382 AND ei = 'competition_goto_team_list'  )
           THEN '赛事详情页-查看参赛战队'
           
           
           WHEN (id = 1100678382 AND ei = '每条咨询的浏览次数' AND get_json_object(kv,'$.type') = 'match_main'  ) OR  
             (id = 1200678382 AND ei = 'competition_goto_news_detail'  )
           THEN '赛事详情页-查看相关资讯'
           
           
           
      
           WHEN (id = 1100678382 AND ei = 'competition_finish_unsubscribe' AND get_json_object(kv,'$.path') = 'match_list') OR  
             (id = 1200678382 AND ei = 'competition_finish_unsubscribe' AND get_json_object(kv,'$.path') = '2'  )
           THEN '赛事订阅-取消订阅-全年赛事'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_finish_unsubscribe' AND get_json_object(kv,'$.path') = 'match_detail') OR  
             (id = 1200678382 AND ei = 'competition_finish_unsubscribe' AND get_json_object(kv,'$.path') = '1'  )
           THEN '赛事订阅-取消订阅-赛事详情页'
           
           WHEN (id = 1100678382 AND ei = 'competition_finish_unsubscribe' AND get_json_object(kv,'$.path') = 'match_card') OR  
             (id = 1200678382 AND ei = 'competition_finish_unsubscribe' AND get_json_object(kv,'$.path') = '4'  )
           THEN '赛事订阅-取消订阅-资讯订阅卡片'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_finish_unsubscribe' AND get_json_object(kv,'$.path') = 'others') OR  
             (id = 1200678382 AND ei = 'competition_finish_unsubscribe' AND get_json_object(kv,'$.path') = '3'  )
           THEN '赛事订阅-取消订阅-赛况订阅icon'
           
           
           
           WHEN (id = 1100678382 AND ei = 'accept_friend' AND get_json_object(kv,'$.from') = '24'  ) OR  
             (id = 1200678382 AND ei = 'add_friend_success' AND get_json_object(kv,'$.add_type') = '24'   )
           THEN '聊天室—通过聊天室添加好友成功'
           
           
           
           WHEN (id = 1100678382 AND ei = 'add_friend' AND get_json_object(kv,'$.from') = '24'  ) OR  
             (id = 1200678382 AND ei = 'add_friend' AND get_json_object(kv,'$.add_type') = '24'   )
           THEN '聊天室—通过聊天室发起添加好友'
           
           
           WHEN (id = 1100678382 AND ei = 'chatroom_agaist'  ) OR  
             (id = 1200678382 AND ei = 'chat_room_report'  )
           THEN '聊天室—通过聊天室举报'
           
           
           WHEN (id = 1100678382 AND ei = 'chatroom_friend'  ) OR  
             (id = 1200678382 AND ei = 'chat_room_view_profile'  )
           THEN '聊天室—通过聊天室查看资料'
           
           
           WHEN (id = 1100678382 AND pi = 'com.tencent.qt.qtl.activity.chat_room.ChatRoomActivity'  )   OR
                (id = 1200678382 AND pi LIKE  '%%ChatRoomViewController%%'  )
        
           THEN '聊天室页面'

           
        ELSE 'other'
    END AS  ei
    
               
    FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND id in (1100678382,1200678382) AND du <= 6*60*60
    )t
    WHERE ei != 'other'
    GROUP BY id,ei

                    """ %(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    ##第一步无法统计的内容放到这里来算
    sql = """
        INSERT TABLE tb_lol_app_match_client_click 
    SELECT
    %s AS dtstatdate,
    id,
    ei,
    COUNT(DISTINCT uin) AS uin_cnt,
    COUNT(DISTINCT uin_info) AS mac_cnt,
    COUNT(*) AS click_cnt,
    SUM(du) AS use_time
    FROM 
    (
    SELECT
    id,
    get_json_object(kv,'$.uin') AS uin,
    concat(ui,mc) AS uin_info,
    du,
    
    CASE   
    WHEN (id = 1100678382 AND ei = 'competition_goto_game_data'  ) OR  
             (id = 1200678382 AND ei = 'competition_goto_game_data'  )
           THEN '赛事详情页-查看赛况'           

   WHEN (id = 1100678382 AND ei = 'competition_goto_video_detail' AND get_json_object(kv,'$.path') = 'match_detail'  ) OR  
             (id = 1200678382 AND ei = 'competition_goto_video_detail' AND get_json_object(kv,'$.path') = '1'  )
           THEN '赛事视频-观看视频-赛事详情页'
           
           
           
           WHEN (id = 1100678382 AND ei = 'competition_goto_video_detail' AND get_json_object(kv,'$.path') = 'match_list'  ) OR  
             (id = 1200678382 AND ei = 'competition_goto_video_detail' AND get_json_object(kv,'$.path') = '2'  )
           THEN '赛事视频-观看视频-全年赛事列表'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_goto_video_detail' AND get_json_object(kv,'$.path') = 'match_list_4_video') OR  
             (id = 1200678382 AND ei = 'competition_goto_video_detail' AND get_json_object(kv,'$.path') = '3'  )
           THEN '赛事视频-观看视频-赛事首页icon'


    WHEN (id = 1100678382 AND ei = 'competition_finish_subscribe' AND get_json_object(kv,'$.path') = 'match_list') OR  
             (id = 1200678382 AND ei = 'competition_finish_subscribe' AND get_json_object(kv,'$.path') = '2'  )
           THEN '赛事订阅-完成订阅-全年赛事'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_finish_subscribe' AND get_json_object(kv,'$.path') = 'match_detail') OR  
             (id = 1200678382 AND ei = 'competition_finish_subscribe' AND get_json_object(kv,'$.path') = '1'  )
           THEN '赛事订阅-完成订阅-赛事详情页'
           
           WHEN (id = 1100678382 AND ei = 'competition_finish_subscribe' AND get_json_object(kv,'$.path') = 'match_card') OR  
             (id = 1200678382 AND ei = 'competition_finish_subscribe' AND get_json_object(kv,'$.path') = '4'  )
           THEN '赛事订阅-完成订阅-资讯订阅卡片'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_finish_subscribe' AND get_json_object(kv,'$.path') = 'others') OR  
             (id = 1200678382 AND ei = 'competition_finish_subscribe' AND get_json_object(kv,'$.path') = '3'  )
           THEN '赛事订阅-完成订阅-赛况订阅icon'
  
        ELSE 'other'
    END AS  ei
    
               
    FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND id in (1100678382,1200678382) AND du <= 6*60*60
    )t
    WHERE ei != 'other'
    GROUP BY id,ei
 
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    sql = """
        INSERT TABLE tb_lol_app_match_client_click 
    SELECT
    %s AS dtstatdate,
    id,
    ei,
    COUNT(DISTINCT uin) AS uin_cnt,
    COUNT(DISTINCT uin_info) AS mac_cnt,
    COUNT(*) AS click_cnt,
    SUM(du) AS use_time
    FROM 
    (
    SELECT
    id,
    get_json_object(kv,'$.uin') AS uin,
    concat(ui,mc) AS uin_info,
    du,
    
    CASE   
   WHEN (id = 1100678382 AND ei = 'competition_goto_game_data' AND get_json_object(kv,'$.path') = '3') OR  
             (id = 1200678382 AND ei = 'competition_goto_game_data' AND get_json_object(kv,'$.path') = '3'  )
           THEN '赛况数据-赛况数据icon'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_goto_game_data' AND get_json_object(kv,'$.path') = '1') OR  
             (id = 1200678382 AND ei = 'competition_goto_game_data' AND get_json_object(kv,'$.path') = '1'  )
           THEN '赛况数据-赛事详情页'
           
           
           WHEN (id = 1100678382 AND ei = 'competition_goto_game_data' AND get_json_object(kv,'$.path') = '2') OR  
             (id = 1200678382 AND ei = 'competition_goto_game_data' AND get_json_object(kv,'$.path') = '2'  )
           THEN '赛况数据-全年赛事'

        ELSE 'other'
    END AS  ei
    
               
    FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND id in (1100678382,1200678382) AND du <= 6*60*60
    )t
    WHERE ei != 'other'
    GROUP BY id,ei
 
    """%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
    ###使用赛事的总时长
    
    
    ##使用聊天房间的时长分布
    sql = ''' 
    CREATE TABLE IF NOT EXISTS tb_lol_app_match_chatroom_staytime_range
    (
     dtstatdate INT COMMENT '统计时间',
     id INT COMMENT 'APP类型，安卓或IOS',
     range1_uin BIGINT COMMENT '使用1min以内',
     range2_uin BIGINT COMMENT '使用1-5m以内',
     range3_uin BIGINT COMMENT '使用5-15m以内',
     range4_uin BIGINT COMMENT '使用15-30m以内',
     range5_uin BIGINT COMMENT '使用30-60m以内',
     range6_uin BIGINT COMMENT '大于60m'
    )'''
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''
    DELETE FROM tb_lol_app_match_chatroom_staytime_range WHERE dtstatdate = %s 
    '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''
    INSERT TABLE tb_lol_app_match_chatroom_staytime_range
    SELECT
    %s AS dtstatdate,
    id,
    COUNT(DISTINCT range1_uin),
    COUNT(DISTINCT range2_uin),
    COUNT(DISTINCT range3_uin),
    COUNT(DISTINCT range4_uin),
    COUNT(DISTINCT range5_uin),
    COUNT(DISTINCT range6_uin)
    FROM
    (
    SELECT
    id,
    CASE WHEN time < 60 THEN uin_info ELSE NULL END AS range1_uin,
    CASE WHEN time >= 60 AND time <300 THEN uin_info ELSE NULL END AS range2_uin,
    CASE WHEN time >= 300 AND time < 900 THEN uin_info ELSE NULL END AS range3_uin,
    CASE WHEN time >= 900 AND time < 1800 THEN uin_info ELSE NULL END AS range4_uin,
    CASE WHEN time >= 1800 AND time < 3600 THEN uin_info ELSE NULL END AS range5_uin,
    CASE WHEN time >= 3600 THEN uin_info ELSE NULL END AS range6_uin
    FROM 
    (
    SELECT
    id,
    uin_info,
    SUM(du)  AS time
    FROM 
    ( 
    SELECT
    id,
    concat(ui,mc) AS uin_info,
    du
    FROM teg_mta_intf::ieg_lol WHERE sdate = %s AND id IN (1100678382,1200678382) AND (pi = 'com.tencent.qt.qtl.activity.chat_room.ChatRoomActivity' OR pi LIKE '%%ChatRoomViewController%%' ) 
    AND du < 5*60*60
    )t GROUP BY id, uin_info
    )t1
    )t2
    GROUP BY id 
    '''%(sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    
   ##聊天室信息统计
    sql = '''
    CREATE TABLE IF NOT EXISTS tb_lol_app_match_chatroom_msg
    (
    dtstatdate INT COMMENT '统计时间',
    biz_id STRING  COMMENT '赛事信息ID',
    msg_cnt BIGINT COMMENT '总共发送信息条数',
    send_msguin_cnt BIGINT COMMENT '发送信息用户数',
    enter_chatroom_uin_cnt BIGINT COMMENT '进入聊天室用户数'
    ) 
   ''' 
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''
    DELETE FROM tb_lol_app_match_chatroom_msg WHERE dtstatdate = %s 
    '''%(sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    sql = '''
    INSERT TABLE tb_lol_app_match_chatroom_msg
    SELECT 
    %s AS dtstatdate,
    t1.biz_id AS biz_id,
    CASE WHEN t.msg_cnt IS NULL THEN 0 ELSE t.msg_cnt END AS msg_cnt,
    CASE WHEN t.send_msguin_cnt  IS NULL THEN 0 ELSE  t.send_msguin_cnt END AS send_msguin_cnt,
    t1.enter_chatroom_uin_cnt AS enter_chatroom_uin_cnt 
    FROM 
    (
    SELECT 
    CASE WHEN GROUPING(biz_id) = 1 THEN '-100' ELSE biz_id END AS biz_id,
    COUNT(DISTINCT iuin) AS enter_chatroom_uin_cnt 
    FROM ieg_tdbank::qtalk_dsl_chatroommgr_fht0 
    WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23'
    GROUP BY cube(biz_id)
    )t1
    LEFT OUTER JOIN
    (
    SELECT
    CASE WHEN GROUPING(biz_id) = 1 THEN '-100' ELSE biz_id END AS biz_id,
    COUNT(*) AS msg_cnt,
    COUNT(DISTINCT iuin) AS send_msguin_cnt 
    FROM ieg_tdbank::qtalk_dsl_chatroommsg_fht0 
    WHERE tdbank_imp_date BETWEEN '%s00' AND '%s23' 
    GROUP BY cube(biz_id)
    )t
    ON (t1.biz_id = t.biz_id) 
    ''' %(sDate,sDate,sDate,sDate,sDate)
    tdw.WriteLog(sql)
    res = tdw.execute(sql)
    
    
    
    tdw.WriteLog("== end OK ==")
    
    
    

    
    
    