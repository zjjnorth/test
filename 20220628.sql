
Drop table if exists temp_tg_im_cl_0626_01;
create table temp_tg_im_cl_0626_01 as
select t1.dt,t1.is_new_user,t1.user_id
    ,t4.user_id cr_user_id
    ,count(distinct t2.relation_id)rel_cnt
    ,avg(t2.group_count)avg_group
    ,max(t2.group_count)max_gooup
    ,count(distinct t3.relation_id)rel_cnt_his
    ,avg(t3.group_count)avg_group_his
    ,max(t3.group_count)max_gooup_his
from dwd_tg_user_login_detail_day t1
-- 当天新达成
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
        ,to_char(create_time,'yyyymmdd') relation_dt
        ,new_uid
    from ods_mtapp_im_together_chat_stat_df tt
    lateral view explode(split(relation_id,'_'))tablead as new_uid
    where to_date(to_date(dt,'YYYYMMDD'))=to_date(create_time)
    -- dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
    and create_time>='2022-06-01 00:00:00'
    and dt>='20220601'
    )t2 on t1.user_id=t2.new_uid and t1.dt=t2.relation_dt
--不分时间达成
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count,new_uid
    from data_platform.data_platform.ods_mtapp_im_together_chat_stat_df tt
    lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')  
    )t3 on t1.user_id=t3.new_uid
left join dwd_tg_user_login_detail_day t4 on t1.user_id=t4.user_id 
    and to_char(dateadd(to_date(t1.dt,'yyyymmdd'),1,'dd'),'yyyymmdd')=t4.dt and t4.dt>='20220602'
where t1.dt>='20220601'
group by t1.dt,t1.is_new_user,t1.user_id,t4.user_id;


Drop table if exists temp_tg_im_cl_0626_02;
create table temp_tg_im_cl_0626_02 as
select t1.dt,coalesce(t1.is_new_user,2)is_new_user
    ,coalesce(case when rel_cnt>50 then '(50+)'
        when rel_cnt>20 then '(20-50]'
        when rel_cnt>10 then '(10-20]'
        else rel_cnt end,'总体') as rea_cnt
    ,coalesce(case when avg_group is null then 0
        when avg_group=1 then '1'
        when avg_group<=2 then '(1,2]'
        when avg_group<=3 then '(2,3]'
        when avg_group<=4 then '(3,4]'
        when avg_group<=5 then '(4,5]'
        when avg_group<=10 then '(5,10]'
        else '(10+)' end,'总体') as avg_group
    ,coalesce(case when max_gooup is null then '0'
        when max_gooup=1 then '1'
        when max_gooup=2 then '2'
        when max_gooup=3 then '3'
        when max_gooup=4 then '4'
        when max_gooup=5 then '5'
        when max_gooup<=10 then '(5,10]'
        when max_gooup<=20 then '(10,20]'
        when max_gooup<=30 then '(20,30]'
        when max_gooup<=40 then '(30,40]'
        when max_gooup<=50 then '(40,50]'
        else '(50+)' end,'总体') as max_gooup
    ,count(distinct user_id) user_cnt
    ,count(distinct cr_user_id) 2d_user_cnt
    ,count(distinct cr_user_id)/count(distinct user_id) cl_rate
from temp_tg_im_cl_0626_01 t1 
group by t1.dt,cube(t1.is_new_user
    ,case when rel_cnt>50 then '(50+)'
        when rel_cnt>20 then '(20-50]'
        when rel_cnt>10 then '(10-20]'
        else rel_cnt end 
    ,case when avg_group=1 then '1'
        when avg_group<=2 then '(1,2]'
        when avg_group<=3 then '(2,3]'
        when avg_group<=4 then '(3,4]'
        when avg_group<=5 then '(4,5]'
        when avg_group<=10 then '(5,10]'
        else '(10+)' end
    ,case when max_gooup=1 then '1'
        when max_gooup=2 then '2'
        when max_gooup=3 then '3'
        when max_gooup=4 then '4'
        when max_gooup=5 then '5'
        when max_gooup<=10 then '(5,10]'
        when max_gooup<=20 then '(10,20]'
        when max_gooup<=30 then '(20,30]'
        when max_gooup<=40 then '(30,40]'
        when max_gooup<=50 then '(40,50]'
        else '(50+)' end); 

Drop table if exists dm_tg_im_liucun_rl;
create table dm_tg_im_liucun_rl as
select *
from temp_tg_im_cl_0626_02 t;


Drop table if exists temp_tg_im_cl_0626_03;
create table temp_tg_im_cl_0626_03 as
-- Drop table if exists dm_tg_im_liucun_rl_new;
-- create table dm_tg_im_liucun_rl_new as
SELECT t0.dt,t0.user_id,t0.is_new_user,t4.user_id cr_user_id
    ,count(distinct t1.relation_id)message_rl_cnt
    ,count(distinct t2.relation_id)success_rl_cnt
    ,avg(t2.group_count)avg_group
    ,max(t2.group_count)max_gooup
from dwd_tg_user_login_detail_day t0
left join (
    select dt,fromuid,touid,concat(case when fromuid<touid then fromuid else touid end
        ,'_',case when fromuid<touid then touid else fromuid end) relation_id
    from ods_kafka_chat_service_message t1
    where t1.dt>='20220601'
    and t1.appid=80
    and t1.templateid in('1','15318','15316')
    group by dt,fromuid,touid
    )t1 on t0.user_id=t1.fromuid and t0.dt=t1.dt
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count,dt
        ,to_char(create_time,'yyyymmdd') relation_dt
        -- ,new_uid
    from ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt>='20220601'
    -- dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
    -- to_date(to_date(dt,'YYYYMMDD'))=to_date(create_time)   
    -- and create_time>='2022-06-01 00:00:00'   
    )t2 on t1.relation_id=t2.relation_id and t1.dt=t2.dt
left join dwd_tg_user_login_detail_day t4 on t0.user_id=t4.user_id 
    and to_char(dateadd(to_date(t0.dt,'yyyymmdd'),1,'dd'),'yyyymmdd')=t4.dt and t4.dt>='20220602'
where t0.dt>='20220601'
group by t0.dt,t0.user_id,t0.is_new_user,t4.user_id;







Drop table if exists temp_tg_im_cl_0626_04;
create table temp_tg_im_cl_0626_04 as
select t1.dt,coalesce(t1.is_new_user,2)is_new_user
    ,coalesce(case when message_rl_cnt>60 then '(60+)'
        when message_rl_cnt>30 then '(30,60]'
        when message_rl_cnt>20 then '(20,30]'
        when message_rl_cnt>10 then '(10,20]'
        else message_rl_cnt end,'总体') as message_rl_cnt   
    ,coalesce(case when success_rl_cnt>50 then '(50+)'
        when success_rl_cnt>20 then '(20-50]'
        when success_rl_cnt>10 then '(10-20]'
        else success_rl_cnt end,'总体') as success_rl_cnt
    ,coalesce(case when (avg_group is null or avg_group=0) then '0'
        when avg_group=1 then '1'
        when avg_group<=2 then '(1,2]'
        when avg_group<=3 then '(2,3]'
        when avg_group<=4 then '(3,4]'
        when avg_group<=5 then '(4,5]'
        when avg_group<=10 then '(5,10]'
        else '(10+)' end,'总体') as avg_group
    ,coalesce(case when (max_gooup is null or max_gooup=0) then '0'
        when max_gooup=1 then '1'
        when max_gooup=2 then '2'
        when max_gooup=3 then '3'
        when max_gooup=4 then '4'
        when max_gooup=5 then '5'
        when max_gooup<=10 then '(5,10]'
        when max_gooup<=20 then '(10,20]'
        when max_gooup<=30 then '(20,30]'
        when max_gooup<=40 then '(30,40]'
        when max_gooup<=50 then '(40,50]'
        else '(50+)' end,'总体') as max_gooup
    ,count(distinct user_id) user_cnt
    ,count(distinct cr_user_id) 2d_user_cnt
    ,count(distinct cr_user_id)/count(distinct user_id) cl_rate
from temp_tg_im_cl_0626_03 t1 
group by t1.dt,cube(t1.is_new_user
    ,case when message_rl_cnt>60 then '(60+)'
        when message_rl_cnt>30 then '(30,60]'
        when message_rl_cnt>20 then '(20,30]'
        when message_rl_cnt>10 then '(10,20]'
        else message_rl_cnt end
    ,case when success_rl_cnt>50 then '(50+)'
        when success_rl_cnt>20 then '(20-50]'
        when success_rl_cnt>10 then '(10-20]'
        else success_rl_cnt end 
    ,case when (avg_group is null or avg_group=0) then '0'
        when avg_group=1 then '1'
        when avg_group<=2 then '(1,2]'
        when avg_group<=3 then '(2,3]'
        when avg_group<=4 then '(3,4]'
        when avg_group<=5 then '(4,5]'
        when avg_group<=10 then '(5,10]'
        else '(10+)' end
    ,case when (max_gooup is null or max_gooup=0) then '0'
        when max_gooup=1 then '1'
        when max_gooup=2 then '2'
        when max_gooup=3 then '3'
        when max_gooup=4 then '4'
        when max_gooup=5 then '5'
        when max_gooup<=10 then '(5,10]'
        when max_gooup<=20 then '(10,20]'
        when max_gooup<=30 then '(20,30]'
        when max_gooup<=40 then '(30,40]'
        when max_gooup<=50 then '(40,50]'
        else '(50+)' end); 


Drop table if exists temp_tg_im_cl_0626_05;
create table temp_tg_im_cl_0626_05 as
select dt,coalesce(is_new_user,2)is_new_user
    ,count(distinct user_id)app_cnt 
    ,count(distinct case when message_rl_cnt>0 then user_id end) mess_user_cnt
    ,count(distinct case when success_rl_cnt>0 then user_id end) succ_user_cnt
    ,sum(message_rl_cnt)message_rl_cnt
    ,sum(success_rl_cnt)success_rl_cnt
from temp_tg_im_cl_0626_03 t 
group by dt,cube(is_new_user);

Drop table if exists dm_tg_im_liucun_rl_new;
create table dm_tg_im_liucun_rl_new as
SELECT dt
    ,case when is_new_user=2 then '总体'
        when is_new_user=1 then '新用户'
        when is_new_user=0 then '老用户' end as is_new_user
    ,message_rl_cnt 
    ,success_rl_cnt
    ,avg_group
    ,max_gooup
    ,user_cnt
    ,2d_user_cnt
    ,cl_rate
from temp_tg_im_cl_0626_04;








-- --全部IM
-- SELECT t0.dt,t0.user_id
--     ,count(distinct t1.relation_id)message_rl_cnt
--     ,count(distinct t2.relation_id)success_rl_cnt
--     ,avg(t2.group_count)avg_group
--     ,max(t2.group_count)max_gooup
-- from dwd_tg_user_login_detail_day t0
-- left join (
--     select dt,fromuid,touid,concat(case when fromuid<touid then fromuid else touid end
--         ,'_',case when fromuid<touid then touid else fromuid end) relation_id
--     from ods_kafka_chat_service_message t1
--     where t1.dt>='20220601'
--     and t1.appid=80
--     and t1.templateid in('1','15318','15316')
--     group by dt,fromuid,touid
--     )t1 on t0.user_id=t1.fromuid and t0.dt=t1.dt
-- -- left join(
-- --     select dt,id,fromuid,touid,concat(case when fromuid<touid then fromuid else touid end
-- --         ,'_',case when fromuid<touid then touid else fromuid end) relation_id
-- --     from ods_kafka_chat_service_message t1
-- --     where t1.dt>='20220601'
-- --     and t1.appid=80
-- --     and t1.templateid in('1','15318','15316')
-- --     ) t2 on t1.fromuid=t2.touid and t1.touid=t2.fromuid and t1.dt=t2.dt
-- -- left join(
-- --     select dt,id,from_uid,touid,concat(case when fromuid<touid then fromuid else touid end
-- --         ,'_',case when fromuid<touid then touid else fromuid end) relation_id
-- --     from ods_kafka_chat_service_message t1
-- --     where t1.dt>='20220601'
-- --     and t1.appid=80
-- --     and t1.templateid in('1','15318','15316')
-- --     ) t3 on t1.fromuid=t3.fromuid and t1.touid=t3.touid and t1.dt=t3.dt and t3.id>t2.id
-- left join(
--     SELECT to_date(create_time)date,relation_id,tt.group_count
--         ,to_char(create_time,'yyyymmdd') relation_dt
--         -- ,new_uid
--     from ods_mtapp_im_together_chat_stat_df tt
--     -- lateral view explode(split(relation_id,'_'))tablead as new_uid
--     where dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
--     -- to_date(to_date(dt,'YYYYMMDD'))=to_date(create_time)   
--     -- and create_time>='2022-06-01 00:00:00'
--     -- and dt>='20220601'
--     )t4 on t1.relation_dt=t4.relation_dt
-- group by case when tt.group_count<=1 then '(0,1]'
--     when tt.group_count<=2 then '(1,2]'
--     when tt.group_count<=5 then '(2，5]'
--     when tt.group_count<=10 then '(5,10]'
--     when tt.group_count<=20 then '(10,20]'
--     when tt.group_count>20 then '(20+)' end;






-- 分功能IM发起达成
Drop table if exists temp_tg_im_dc_0619_02;
create table temp_tg_im_dc_0619_02 as
select t3.dt,t3.is_new_user
    ,case when t1.fromSource in('candy_square','candy_square_double') then '糖果广场'
        when t1.fromSource in('heart_lab') then '心动实验室'
        when t1.fromSource in('dtSquare','post_square') then '动态广场'
        when t1.fromSource in('love_sign') then '恋爱上上签'
        when t1.fromSource in('confess_train') then '脱单墙'
        when t1.fromSource in('pw_window') then '交友雷达'
        when t1.fromSource in('bb_machine') then '哔哔机' end as fromSource
    -- ,count(distinct t3.user_id)app_user
    ,count(distinct t1.uid) im_user
    ,count(distinct case when t2.relation_id is not null then t1.uid end) today_im_respond_user
    ,count(distinct case when t4.relation_id is not null then t1.uid end) all_im_respond_user
from dwd_tg_user_login_detail_day t3 
join(
    select ds,uid,reach_time
            ,fromSource
            ,to_uid
            ,relation_id
            ,row_number() over(partition by relation_id order by reach_time)rk
    from(
        SELECT ds,uid,reach_time
            ,getmapvalue(args,'fromSource') fromSource
            ,getmapvalue(args,'to_uid') to_uid
            ,concat(case when uid<getmapvalue(args,'to_uid') then uid else getmapvalue(args,'to_uid') end
                ,'_',case when uid<getmapvalue(args,'to_uid') then getmapvalue(args,'to_uid') else uid end) relation_id
            -- ,row_number() over(partition by uid,getmapvalue(args,'to_uid') order by local_time) rk
        FROM ods_all_mobile_log
        where ds>='20220601'
        and event_id = '19999'   --'19999'代表事件，'2001'代表页面曝光，'1010'代表页面停留时长
        and pageid = 'PageId-545A8852' -- h5页面要用 page ,app用 pageid
        and elementid = 'ElementId-52GG26HB' -- h5页面要用arg1 ,app用elementid
        and getmapvalue(args,'fromSource') in('candy_square','heart_lab','post_square','dtSquare','love_sign','confess_train','pw_window')
        union all 
        select dt,uid,reach_time
            ,getmapvalue(args,'fromSource') fromSource
            ,getmapvalue(args,'to_uid') to_uid
            ,concat(case when uid<getmapvalue(args,'to_uid') then uid else getmapvalue(args,'to_uid') end
                ,'_',case when uid<getmapvalue(args,'to_uid') then getmapvalue(args,'to_uid') else uid end) relation_id
        from ods_all_server_burying_point
        where app_id=80
        and dt>='20220601'
        and pageid='PageId-545A8852'
        and elementid='ElementId-52GG26HB'
        )tt
    )t1 on cast(t3.user_id as string)=t1.uid and t3.dt=t1.ds and t1.rk=1
-- 当天达成
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
        ,to_char(create_time,'yyyymmdd') relation_dt
    from ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
    and to_date(create_time)>='2022-06-01'
    )t2 on t1.relation_id=t2.relation_id and t1.ds=t2.relation_dt
-- 不分时间达成
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
    )t4 on t1.relation_id=t4.relation_id
where t3.dt>='20220601'
group by t3.dt,t3.is_new_user
    ,case when t1.fromSource in('candy_square','candy_square_double') then '糖果广场'
        when t1.fromSource in('heart_lab') then '心动实验室'
        when t1.fromSource in('dtSquare','post_square') then '动态广场'
        when t1.fromSource in('love_sign') then '恋爱上上签'
        when t1.fromSource in('confess_train') then '脱单墙'
        when t1.fromSource in('pw_window') then '交友雷达'
        when t1.fromSource in('bb_machine') then '哔哔机' end;










SELECT uid
    ,getmapvalue(args,'fromSource') fromSource
    ,getmapvalue(args,'to_uid') to_uid
FROM ods_all_mobile_log
where ds='20220601'
and event_id = '19999'   --'19999'代表事件，'2001'代表页面曝光，'1010'代表页面停留时长
and pageid = 'PageId-545A8852' -- h5页面要用 page ,app用 pageid
and elementid = 'ElementId-52GG26HB' -- h5页面要用arg1 ,app用elementid
;


-- -- 舞团活动数据
-- select *
-- from data_platform.ods_all_server_burying_point
-- where app_id=80
-- and dt>='20220601'
-- and arg1 in('ElementId-52GG26HB');




-- -- 分功能IM达成
-- select t3.dt,t3.is_new_user
--     ,count(distinct t3.user_id)app_user
-- from dwd_tg_user_login_detail_day t3 
-- where dt>='20220601'
-- group by t3.dt,t3.is_new_user


Drop table if exists temp_tg_im_dc_0619_01;
create table temp_tg_im_dc_0619_01 as
select dt,is_new_user
    ,count(distinct user_id) app_user
    ,count(distinct new_uid) im_dc_user
    ,count(distinct new_uid)/count(distinct user_id) nd_im_dc_rate
from(
    select t2.dt,t2.is_new_user,t2.user_id,t1.new_uid
    from dwd_tg_user_login_detail_day t2 
    left join(
        SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
        from data_platform.ods_mtapp_im_together_chat_stat_df tt
        lateral view explode(split(relation_id,'_'))tablead as new_uid
        where dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
        and to_date(create_time)>='2022-06-01'
        )t1 on t1.date=t2.login_date and t1.new_uid=t2.user_id 
    where t2.dt>='20220601'
    )tt
group by dt,is_new_user;


-- select date,is_new_user
--     ,count(distinct biz_id)cnt
--     ,count(distinct new_uid)user_cnt
--     ,sum(group_count)group_count
-- from(
--     select t1.date,t1.biz_id,t1.new_uid,max(t2.is_new_user)is_new_user
--         ,max(group_count)group_count
--     from(
--         SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
--         from data_platform.ods_mtapp_im_together_chat_stat_df tt
--         lateral view explode(split(relation_id,'_'))tablead as new_uid
--         where dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
--         and to_date(create_time)>'2022-06-01'
--         )t1
--     left join dwd_tg_user_login_detail_day t2 on t1.date=t2.login_date and t1.new_uid=t2.user_id 
--         and t2.dt>='20220510'
--     group by t1.date,t1.biz_id,t1.new_uid
--     )tt
-- group by date,is_new_user;




-- 分功能IM发起达成
Drop table if exists temp_tg_im_dc_0619_02;
create table temp_tg_im_dc_0619_02 as
select t3.dt,t3.is_new_user
    ,case when t1.fromSource in('candy_square','candy_square_double') then '糖果广场'
        when t1.fromSource in('heart_lab') then '心动实验室'
        when t1.fromSource in('dtSquare','post_square') then '动态广场'
        when t1.fromSource in('love_sign') then '恋爱上上签'
        when t1.fromSource in('confess_train') then '脱单墙'
        when t1.fromSource in('pw_window') then '交友雷达'
        when t1.fromSource in('bb_machine') then '哔哔机' end as fromSource
    -- ,count(distinct t3.user_id)app_user
    ,count(distinct t1.uid) im_user
    ,count(distinct case when t2.relation_id is not null then t1.uid end) today_im_respond_user
    ,count(distinct case when t4.relation_id is not null then t1.uid end) all_im_respond_user
from dwd_tg_user_login_detail_day t3 
join(
    select ds,uid,reach_time
            ,fromSource
            ,to_uid
            ,relation_id
            ,row_number() over(partition by relation_id order by reach_time)rk
    from(
        SELECT ds,uid,reach_time
            ,getmapvalue(args,'fromSource') fromSource
            ,getmapvalue(args,'to_uid') to_uid
            ,concat(case when uid<getmapvalue(args,'to_uid') then uid else getmapvalue(args,'to_uid') end
                ,'_',case when uid<getmapvalue(args,'to_uid') then getmapvalue(args,'to_uid') else uid end) relation_id
            -- ,row_number() over(partition by uid,getmapvalue(args,'to_uid') order by local_time) rk
        FROM ods_all_mobile_log
        where ds>='20220601'
        and event_id = '19999'   --'19999'代表事件，'2001'代表页面曝光，'1010'代表页面停留时长
        and pageid = 'PageId-545A8852' -- h5页面要用 page ,app用 pageid
        and elementid = 'ElementId-52GG26HB' -- h5页面要用arg1 ,app用elementid
        and getmapvalue(args,'fromSource') in('candy_square','heart_lab','post_square','dtSquare','love_sign','confess_train','pw_window')
        union all 
        select dt,uid,reach_time
            ,getmapvalue(args,'fromSource') fromSource
            ,getmapvalue(args,'to_uid') to_uid
            ,concat(case when uid<getmapvalue(args,'to_uid') then uid else getmapvalue(args,'to_uid') end
                ,'_',case when uid<getmapvalue(args,'to_uid') then getmapvalue(args,'to_uid') else uid end) relation_id
        from ods_all_server_burying_point
        where app_id=80
        and dt>='20220601'
        and pageid='PageId-545A8852'
        and elementid='ElementId-52GG26HB'
        )tt
    )t1 on cast(t3.user_id as string)=t1.uid and t3.dt=t1.ds and t1.rk=1
-- 当天达成
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
        ,to_char(create_time,'yyyymmdd') relation_dt
    from ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
    and to_date(create_time)>='2022-06-01'
    )t2 on t1.relation_id=t2.relation_id and t1.ds=t2.relation_dt
-- 不分时间达成
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
    )t4 on t1.relation_id=t4.relation_id
where t3.dt>='20220601'
group by t3.dt,t3.is_new_user
    ,case when t1.fromSource in('candy_square','candy_square_double') then '糖果广场'
        when t1.fromSource in('heart_lab') then '心动实验室'
        when t1.fromSource in('dtSquare','post_square') then '动态广场'
        when t1.fromSource in('love_sign') then '恋爱上上签'
        when t1.fromSource in('confess_train') then '脱单墙'
        when t1.fromSource in('pw_window') then '交友雷达'
        when t1.fromSource in('bb_machine') then '哔哔机' end;


Drop table if exists dm_tg_im_fun_reach;
create table dm_tg_im_fun_reach as
select t1.dt,t1.is_new_user
    ,t1.app_user
    ,t1.im_dc_user
    ,t1.im_dc_user/t1.app_user im_dc_rate
    ,t2.fromSource
    ,t2.im_user
    ,t2.im_user/t1.app_user fun_im_fq_rate

    ,t2.today_im_respond_user      
    ,t2.today_im_respond_user/t1.app_user fun_im_dc_rate
    ,t2.today_im_respond_user/t2.im_user fun_im_resp_rate

    ,t2.all_im_respond_user
    ,t2.all_im_respond_user/t1.app_user fun_all_dc_rate
    ,t2.all_im_respond_user/t2.im_user fun_all_resp_rate
from temp_tg_im_dc_0619_01 t1
left join temp_tg_im_dc_0619_02 t2 on t1.dt=t2.dt and t1.is_new_user=t2.is_new_user;


















-- 分功能IM达成（不分时间达成） 
select t3.dt,t3.is_new_user,t1.fromSource
    ,count(distinct t3.user_id)app_user
    ,count(distinct t1.uid) im_user
    ,count(distinct case when t2.relation_id is not null then t1.uid end) im_respond_user
from dwd_tg_user_login_detail_day t3 
left join(
    select ds,uid,reach_time
            ,fromSource
            ,to_uid
            ,relation_id
            ,row_number() over(partition by relation_id order by reach_time)rk
    from(
        SELECT ds,uid,reach_time
            ,getmapvalue(args,'fromSource') fromSource
            ,getmapvalue(args,'to_uid') to_uid
            ,concat(case when uid<getmapvalue(args,'to_uid') then uid else getmapvalue(args,'to_uid') end
                ,'_',case when uid<getmapvalue(args,'to_uid') then getmapvalue(args,'to_uid') else uid end) relation_id
            -- ,row_number() over(partition by uid,getmapvalue(args,'to_uid') order by local_time) rk
        FROM ods_all_mobile_log
        where ds>='20220601'
        and event_id = '19999'   --'19999'代表事件，'2001'代表页面曝光，'1010'代表页面停留时长
        and pageid = 'PageId-545A8852' -- h5页面要用 page ,app用 pageid
        and elementid = 'ElementId-52GG26HB' -- h5页面要用arg1 ,app用elementid
        and getmapvalue(args,'fromSource') in('candy_square','heart_lab','post_square','dtSquare','candy_square','love_sign','confess_train')
        union all 
        select dt,uid,reach_time
            ,getmapvalue(args,'fromSource') fromSource
            ,getmapvalue(args,'to_uid') to_uid
            ,concat(case when uid<getmapvalue(args,'to_uid') then uid else getmapvalue(args,'to_uid') end
                ,'_',case when uid<getmapvalue(args,'to_uid') then getmapvalue(args,'to_uid') else uid end) relation_id
        from ods_all_server_burying_point
        where app_id=80
        and dt>='20220601'
        and pageid='PageId-545A8852'
        and elementid='ElementId-52GG26HB'
        )tt
    )t1 on cast(t3.user_id as string)=t1.uid and t3.dt=t1.ds and t1.rk=1
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt=to_char(dateadd(datetime(CURRENT_DATE()),-1,'dd'),'yyyymmdd')
    )t2 on t1.relation_id=t2.relation_id
where t3.dt>='20220601'
group by t3.dt,t3.is_new_user,t1.fromSource;



SELECT ds
    ,count(uid)pv
    ,count(distinct uid) uv
FROM ods_all_mobile_log
where ds>='20220520'
and event_id = '19999'   --'19999'代表事件，'2001'代表页面曝光，'1010'代表页面停留时长
and pageid = 'PageId-545A8852' -- h5页面要用 page ,app用 pageid
and elementid = 'ElementId-52GG26HB' -- h5页面要用arg1 ,app用elementid
group by ds;

页面page,h5用page, app用page_id
事件element,h5用arg1, app用element_id

and event_id = '19999'   --'19999'代表事件，'2001'代表页面曝光，'1010'代表页面停留时长
and page = 'PageId-7E2B8B43' -- h5页面要用 page ,app用 pageid
and arg1 = 'ElementId-F783A3H3' -- h5页面要用arg1 ,app用elementid























SELECT 
        to_date(login_date,'yyyy-mm-dd') as login_dt,
        login_date,
        replace(login_date,'-','') AS DT,
        user_id,
        count(*) 
    FROM dwd_tg_user_login_detail_day
    group by login_date,user_id,to_date




这段时间的排查发现，实际在tg_userId、tg_udid属性中是有值的，而tg_uid并没有值进行上报（tg_uid也有修改类型的痕迹，但是查不到上报的数据） 
tg_age、tg_gender在表里查到有根据int类型上报过数据，但当前元数据是float类型，我们希望您内部确认下这里否有过更改。





1.tg_age、tg_gender未生效的问题
2.是之前说过的用户属性的问题，我们并未上报的属性tea_event_index、nt从哪里来的
3.目前还有部分事件一直上报1030007事件公共属性元数据不存在，导致部分数据被抛弃，比如「首页」这个事件
主要字段集中在udid、user_id




-- 暖评数据
drop table if exists temp_zjj_tg_comment_showno;
CREATE TABLE IF NOT EXISTS temp_zjj_tg_comment_showno
(
show_no bigint
,nick_name string
);

select t.dt,t.uid,t.show_no,count(distinct t.content_id)comment_cnt
from(
    select t3.dt,t1.show_no,t3.content_id,t3.uid
    from temp_zjj_tg_comment_showno t1 
    join dwd_app_user_info t2 on t1.show_no=t2.show_no
    left join(
        select t1.dt,t1.content_id,t1.author_id,t2.uid,t2.id
            ,row_number() over(partition by t1.content_id order by t2.create_time) rk
        from ods_content_interaction_t_comment_day t2
        join ods_app_content_main_day t1 on cast(t1.content_id as string)=t2.biz_id 
            and t1.dt=t2.dt and t2.biz_type='220' and t1.content_type = '139' -- 糖果的动态
            and t1.dt>='20220530' and t1.status in(3,5)
        left join ods_content_interaction_t_praise_day t3 on t2.biz_id=t3.biz_id -- 点赞
            and t2.dt=t3.dt and t2.uid=t3.subject_id and t3.biz_type = '333' and t3.status = 1 
        left join dwd_app_user_info t4 on t1.author_id=t4.user_id
        left join temp_zjj_tg_comment_showno t5 on t4.show_no=t5.show_no
        where t2.type=1  
        and t2.status=1 -- 评论
        and t2.dt>='20220530'
        and t1.author_id<>t2.uid
        and t3.biz_id is not null
        and t5.show_no is null
        )t3 on t2.user_id=t3.uid and t3.rk<=3
    group by t3.dt,t1.show_no,t3.content_id,t3.uid
    )t
group by t.dt,t.uid,t.show_no;









select t1.*,t3.*
from temp_zjj_tg_comment_showno t1 
join dwd_app_user_info t2 on t1.show_no=t2.show_no
left join(
    select t1.dt,t1.content_id,t1.author_id,t2.uid,t2.id
        ,t2.create_time
        ,t2.content
        ,t3.biz_id if_praise
        ,t5.show_no if_comment_pw
        ,row_number() over(partition by t1.content_id order by t2.create_time) rk
    from ods_content_interaction_t_comment_day t2
    join ods_app_content_main_day t1 on cast(t1.content_id as string)=t2.biz_id 
        and t1.dt=t2.dt and t2.biz_type='220' and t1.content_type = '139' -- 糖果的动态
        and t1.dt>='20220530' and t1.status=5 
    left join ods_content_interaction_t_praise_day t3 on t2.biz_id=t3.biz_id -- 点赞
        and t2.dt=t3.dt and t2.uid=t3.subject_id and t3.biz_type = '333' and t3.status = 1 
    left join dwd_app_user_info t4 on t1.author_id=t4.user_id
    left join temp_zjj_tg_comment_showno t5 on t4.show_no=t5.show_no
    where t2.type=1  
    and t2.status=1 -- 评论
    and t2.dt>='20220530'
    and t1.author_id<>t2.uid
    -- and t3.biz_id is not null
    -- and t5.show_no is null
     )t3 on t2.user_id=t3.uid -- and t3.rk<=3
where t1.show_no=143638294;











-- select t1.dt,t1.content_id,t1.author_id,t2.uid,t2.id
--     ,row_number() over(partition by t1.content_id order by t2.create_time) rk
-- from ods_content_interaction_t_comment_day t2
-- join ods_app_content_main_day t1 on cast(t1.content_id as string)=t2.biz_id 
--     and t1.dt=t2.dt and t2.biz_type='220' and t1.content_type = '139' -- 糖果的动态
--     and t1.dt>='20220511' and t1.status=5 
-- left join ods_content_interaction_t_praise_day t3 on t2.biz_id=t3.biz_id -- 点赞
--     and t2.dt=t3.dt and t2.uid=t3.subject_id and t3.biz_type = '333' and t3.status = 1 
-- where t2.type=1 
-- and t2.status=1 -- 评论
-- and t2.dt>='20220511'
-- and t1.author_id<>t2.uid
-- and t3.biz_id is not null; 






-- select t3.dt,t3.uid,t3.rk,count(distinct t3.content_id)comment_cnt
-- from temp_zjj_tg_comment_showno t1 
-- join dwd_app_user_info t2 on t1.show_no=t2.show_no
-- left join(
--     select t1.dt,t1.content_id,t1.author_id,t2.uid
--         ,row_number() over(partition by t1.content_id order by t2.create_time ) rk
--     from ods_app_content_main_day t1
--     left join ods_content_interaction_t_comment_day t2 on cast(t1.content_id as string)=t2.biz_id 
--         and t1.dt=t2.dt and t2.biz_type='220' and t2.type=1 and t2.status=1 -- 评论
--     left join ods_content_interaction_t_praise_day t3 on t2.biz_id=t3.biz_id -- 点赞
--         and t2.dt=t3.dt and t2.uid=t3.subject_id and t3.biz_type = '333' and t3.status = 1 
--     -- left join dwd_app_user_info t4 on t1.author_id=t4.user_id
--     -- left join temp_zjj_tg_comment_showno t5 on t4.show_no=t5.show_no
--     where t1.content_type = '139' -- 糖果的动态
--     and t1.dt>='20220511'
--     and t1.status=5
--     and t1.author_id<>t2.uid
--     and t3.biz_id is not null
--     )t3 on t2.user_id=t3.uid and t3.rk<=3
-- group by t3.dt,t3.uid,t3.rk;





-- select t3.dt,t3.uid,t3.rk,count(distinct t3.content_id)comment_cnt
-- from temp_zjj_tg_comment_showno t1 
-- join dwd_app_user_info t2 on t1.show_no=t2.show_no
-- left join(
--     select t1.dt,t1.content_id,t1.author_id,t2.uid
--         ,row_number() over(partition by t1.content_id order by t2.create_time ) rk
--     from ods_app_content_main_day t1
--     left join ods_content_interaction_t_comment_day t2 on cast(t1.content_id as string)=t2.biz_id 
--         and t1.dt=t2.dt and t2.biz_type='220' and t2.type=1 and t2.status=1 -- 评论
--     left join ods_content_interaction_t_praise_day t3 on t2.biz_id=t3.biz_id -- 点赞
--         and t2.dt=t3.dt and t2.uid=t3.subject_id and t3.biz_type = '333' and t3.status = 1 
--     -- left join dwd_app_user_info t4 on t1.author_id=t4.user_id
--     -- left join temp_zjj_tg_comment_showno t5 on t4.show_no=t5.show_no
--     where t1.content_type = '139' -- 糖果的动态
--     and t1.dt>='20220511'
--     and t1.status=5
--     and t1.author_id<>t2.uid
--     and t3.biz_id is not null
--     )t3 on t2.user_id=t3.uid and t3.rk<=3
-- group by t3.dt,t3.uid,t3.rk;