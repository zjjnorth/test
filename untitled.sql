


SELECT a.dt
    ,count(distinct a.user_id) cnt 
    ,count(distinct b.user_id) liucun_cnt
from dwd_tg_user_login_detail_day a
left join dwd_tg_user_login_detail_day b on a.user_id=b.user_id 
    and b.dt>a.dt and b.dt<=to_char(dateadd(to_date(a.dt,'yyyymmdd'),7,'dd'),'yyyymmdd')
where a.dt>='20220609'
and a.dt<='20220616'
group by a.dt;



SELECT a.dt
    ,count(distinct a.user_id) cnt 
    ,count(distinct b.user_id) liucun_cnt
from dwd_tg_user_login_detail_day a
left join dwd_tg_user_login_detail_day b on a.user_id=b.user_id 
    and b.dt>a.dt and b.dt<=to_char(dateadd(to_date(a.dt,'yyyymmdd'),7,'dd'),'yyyymmdd')
where a.dt>='20220609'
and a.dt<='20220616'
group by a.dt;



-- 测试取数
select count(0),count(distinct id)
from ods_mtapp_im_together_chat_stat_df t1
where create_time>='2022-06-01 00:00:00'
and create_time<'2022-06-26 00:00:00'
and t1.dt='20220625' --历史 1204524
-- and to_date(to_date(t1.dt,'YYYYMMDD'))=to_date(t1.create_time) --当天 1204524

select count(0),count(distinct id)
from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
where 
-- t1.dt='20220620' --3128193
-- to_date(to_date(t1.dt,'YYYYMMDD'))=to_date(t1.send_time) and dt<='20220620' --3108014



select to_date(t1.send_time)dt_date,t2.is_new_user
    ,count(distinct t1.id)receive_cnt
    ,count(distinct case when to_date(t1.update_time)=to_date(t1.send_time)
        and t1.status=2 then t1.id end)dr_respond_cnt
    ,count(distinct case when t1.status=2 then t1.id end)all_respond_cnt
    ,count(distinct t1.receiver_uid)receive_user
    ,count(distinct case when to_date(t1.update_time)=to_date(t1.send_time)
        and t1.status=2 then t1.receiver_uid end) dr_respond_user
    ,count(distinct case when t1.status=2 then t1.receiver_uid end)all_respond_user
    -- ,count(distinct t3.user_id)receive_cl_user
    -- ,count(distinct case when t1.status=2 then t3.user_id end)respond_cl_user
    -- ,count(distinct case when t1.status=1 then t3.user_id end)respond_cl_user
from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
left join(
    select user_id,max(is_new_user)is_new_user
    from dwd_tg_user_login_detail_day 
    where dt>='20220617'
    and dt<='20220623'
    group by user_id
    )t2 on t1.receiver_uid=t2.user_id
where t1.dt='20220627'
and send_time>='2022-06-23 00:00:00'
group by to_date(t1.send_time),t2.is_new_user;



select to_date(t1.send_time)dt_date,t2.is_new_user
    ,receiver_uid
    ,count(distinct t1.id)receive_cnt
    -- ,count(distinct case when to_date(t1.update_time)=to_date(t1.send_time)
    --     and t1.status=2 then t1.id end)dr_respond_cnt
    -- ,count(distinct case when t1.status=2 then t1.id end)all_respond_cnt
    -- ,count(distinct t1.receiver_uid)receive_user
    -- ,count(distinct case when to_date(t1.update_time)=to_date(t1.send_time)
    --     and t1.status=2 then t1.receiver_uid end) dr_respond_user
    -- ,count(distinct case when t1.status=2 then t1.receiver_uid end)all_respond_user
from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
left join dwd_tg_user_login_detail_day t2 on to_date(t1.send_time)=t2.login_date and t1.receiver_uid=t2.user_id
    and t2.dt='20220623'
where t1.dt='20220623'
and send_time>='2022-06-23 00:00:00'
and send_time<'2022-06-24 00:00:00'
group by to_date(t1.send_time),t2.is_new_user,receiver_uid
having count(distinct t1.id)>5;,t3.*


mtapp-im    beep_chicken_msg_receiver_record


ods_kafka_chat_service_message



-- --IM总
-- SELECT case when tt.group_count<=1 then '(0,1]'
--     when tt.group_count<=2 then '(1,2]'
--     when tt.group_count<=5 then '(2，5]'
--     when tt.group_count<=10 then '(5,10]'
--     when tt.group_count<=20 then '(10,20]'
--     when tt.group_count>20 then '(20+)' end as group_count
--     ,count(0)
-- from data_platform.ods_mtapp_im_together_chat_stat_df tt
-- -- lateral view explode(split(relation_id,'_'))tablead as new_uid
-- where dt='20220616'
-- and to_date(create_time)>='2022-06-02'
-- and to_date(create_time)<='2022-06-15'
-- group by case when tt.group_count<=1 then '(0,1]'
--     when tt.group_count<=2 then '(1,2]'
--     when tt.group_count<=5 then '(2，5]'
--     when tt.group_count<=10 then '(5,10]'
--     when tt.group_count<=20 then '(10,20]'
--     when tt.group_count>20 then '(20+)'
--     end;


--全部IM
SELECT case when tt.group_count<=1 then '(0,1]'
    when tt.group_count<=2 then '(1,2]'
    when tt.group_count<=5 then '(2，5]'
    when tt.group_count<=10 then '(5,10]'
    when tt.group_count<=20 then '(10,20]'
    when tt.group_count>20 then '(20+)' end as group_count
    ,count(distinct t1.relation_id)cnt1
    ,count(distinct tt.relation_id)cnt2
from(
    select dt,concat(case when fromuid<touid then fromuid else touid end
        ,'_',case when fromuid<touid then touid else fromuid end) relation_id
    from data_platform.ods_kafka_chat_service_message t1
    where t1.dt>='20220602'
    and t1.dt<='20220615'
    and t1.appid=80
    -- and t1.templateid in('1','15318','15316')
    )t1
-- 全部达成IM用户
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt='20220616' 
    ) tt on t1.relation_id=tt.relation_id
group by case when tt.group_count<=1 then '(0,1]'
    when tt.group_count<=2 then '(1,2]'
    when tt.group_count<=5 then '(2，5]'
    when tt.group_count<=10 then '(5,10]'
    when tt.group_count<=20 then '(10,20]'
    when tt.group_count>20 then '(20+)' end;

--bbji 
SELECT case when tt.group_count<=1 then '(0,1]'
    when tt.group_count<=2 then '(1,2]'
    when tt.group_count<=5 then '(2，5]'
    when tt.group_count<=10 then '(5,10]'
    when tt.group_count<=20 then '(10,20]'
    when tt.group_count>20 then '(20+)' end as group_count
    ,count(distinct t1.relation_id)cnt1
    ,count(distinct tt.relation_id)cnt2
from(
    select dt,concat(case when fromuid<touid then fromuid else touid end
        ,'_',case when fromuid<touid then touid else fromuid end) relation_id
    from data_platform.ods_kafka_chat_service_message t1
    where t1.dt>='20220602'
    and t1.dt<='20220615'
    and t1.appid=80
    and t1.templateid in('15318','15316')
    )t1
-- 全部达成IM用户
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt='20220616' 
    ) tt on t1.relation_id=tt.relation_id
group by case when tt.group_count<=1 then '(0,1]'
    when tt.group_count<=2 then '(1,2]'
    when tt.group_count<=5 then '(2，5]'
    when tt.group_count<=10 then '(5,10]'
    when tt.group_count<=20 then '(10,20]'
    when tt.group_count>20 then '(20+)' end;


-- 当日收到bb机消息人数达成消息人数
select to_date(t1.send_time)dt_date,t2.is_new_user
    ,count(distinct t1.id)receive_cnt
    ,count(distinct case when to_date(t1.update_time)=to_date(t1.send_time)
        and t1.status=2 then t1.id end)dr_respond_cnt
    ,count(distinct case when t1.status=2 then t1.id end)all_respond_cnt
    ,count(distinct t1.receiver_uid)receive_user
    ,count(distinct case when to_date(t1.update_time)=to_date(t1.send_time)
        and t1.status=2 then t1.receiver_uid end) dr_respond_user
    ,count(distinct case when t1.status=2 then t1.receiver_uid end)all_respond_user
    -- ,count(distinct t3.user_id)receive_cl_user
    -- ,count(distinct case when t1.status=2 then t3.user_id end)respond_cl_user
    -- ,count(distinct case when t1.status=1 then t3.user_id end)respond_cl_user
from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
left join dwd_tg_user_login_detail_day t2 on to_date(t1.send_time)=t2.login_date and t1.receiver_uid=t2.user_id
    and t2.dt>='20220601'
-- left join dwd_tg_user_login_detail_day t3 on to_date(dateadd(t1.send_time,1,'dd'))=to_date(to_date(t3.dt,'yyyymmdd')) and t1.receiver_uid = t3.user_id
--     and t3.dt>='20220601'
where t1.dt='20220616'
-- and to_date(to_date(t1.dt,'YYYYMMDD'))=to_date(t1.send_time)
-- and to_date(t1.update_time)=to_date(t1.send_time)
and t1.send_time>='2022-06-01 00:00:00'
group by to_date(t1.send_time),t2.is_new_user;



-- select to_date(t1.send_time)dt_date,t2.is_new_user
--     ,count(distinct t1.id)receive_cnt
--     ,count(distinct case when t1.status=2 then t1.id end)respond_cnt
--     ,count(distinct t1.receiver_uid)receive_user
--     ,count(distinct case when t1.status=2 then t1.receiver_uid end)respond_user
--     ,count(distinct t3.user_id)receive_cl_user
--     ,count(distinct case when t1.status=2 then t3.user_id end)respond_cl_user
--     ,count(distinct case when t1.status=1 then t3.user_id end)respond_cl_user
-- from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
-- left join dwd_tg_user_login_detail_day t2 on to_date(t1.send_time)=t2.login_date and t1.receiver_uid=t2.user_id
--     and t2.dt>='20220601'
-- left join dwd_tg_user_login_detail_day t3 on to_date(dateadd(t1.send_time,1,'dd'))=to_date(to_date(t3.dt,'yyyymmdd')) and t1.receiver_uid = t3.user_id
--     and t3.dt>='20220601'
-- where t1.dt='20220616'
-- -- and to_date(to_date(t1.dt,'YYYYMMDD'))=to_date(t1.send_time)
-- and to_date(t1.update_time)=to_date(t1.send_time)
-- group by to_date(t1.send_time),t2.is_new_user;


-- select count(distinct t1.relation_id)cnt  
--     ,count(distinct t2.relation_id) xz
-- from(
--     select dt,concat(case when fromuid<touid then fromuid else touid end
--         ,'_',case when fromuid<touid then touid else fromuid end) relation_id
--     from data_platform.ods_kafka_chat_service_message t1
--     where t1.dt>='20220602'
--     and t1.dt<='20220615'
--     and t1.appid=80
--     )t1
-- left join(
--     SELECT to_date(create_time)date,relation_id,tt.group_count
--     from data_platform.ods_mtapp_im_together_chat_stat_df tt
--     -- lateral view explode(split(relation_id,'_'))tablead as new_uid
--     where dt='20220616'
--     ) t2 on t1.relation_id=t2.relation_id;

-- select count(distinct t1.relation_id)cnt  
--     ,count(distinct t2.relation_id) yq
-- from(
--     select dt,concat(case when fromuid<touid then fromuid else touid end
--         ,'_',case when fromuid<touid then touid else fromuid end) relation_id
--     from data_platform.ods_kafka_chat_service_message t1
--     where t1.dt>='20220602'
--     and t1.dt<='20220615'
--     and t1.appid=80
--     )t1
-- left join(
--     SELECT to_date(create_time)date,relation_id,tt.group_count
--     from data_platform.ods_mtapp_im_together_chat_stat_df tt
--     -- lateral view explode(split(relation_id,'_'))tablead as new_uid
--     where dt='20220601'
--     ) t2 on t1.relation_id=t2.relation_id;



-- select count(distinct t1.relation_id)cnt  
--     ,count(distinct t2.relation_id) yq
-- from(
--     select dt,concat(case when fromuid<touid then fromuid else touid end
--         ,'_',case when fromuid<touid then touid else fromuid end) relation_id
--     from data_platform.ods_kafka_chat_service_message t1
--     where t1.dt>='20220602'
--     and t1.dt<='20220615'
--     and t1.appid=80
--     )t1
-- left join(
--     SELECT to_date(create_time)date,relation_id,tt.group_count
--     from data_platform.ods_mtapp_im_together_chat_stat_df tt
--     -- lateral view explode(split(relation_id,'_'))tablead as new_uid
--     where dt='20220616'
--     ) t2 on t1.relation_id=t2.relation_id;







SELECT count(distinct t1.relation_id)cnt
    ,count(distinct t2.relation_id) respond_cnt
from(
    select dt,concat(case when uid<receiver_uid then uid else receiver_uid end
        ,'_',case when uid<receiver_uid then receiver_uid else uid end) relation_id
    from data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
    where t1.dt='20220616'
    and to_date(t1.send_time)>='2022-06-02'
    and to_date(t1.send_time)<='2022-06-15'
    )t1
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt='20220616'
    ) t2 on t1.relation_id=t2.relation_id;



SELECT case when t2.group_count<=1 then '(0,1]'
    when t2.group_count<=2 then '(1,2]'
    when t2.group_count<=5 then '(2，5]'
    when t2.group_count<=10 then '(5,10]'
    when t2.group_count<=20 then '(10,20]'
    when t2.group_count>20 then '(20+)' end as group_count
    ,count(distinct t2.relation_id) cnt
from(
    select dt,concat(case when uid<receiver_uid then uid else receiver_uid end
        ,'_',case when uid<receiver_uid then receiver_uid else uid end) relation_id
    from data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
    where t1.dt='20220616'
    and to_date(t1.send_time)>='2022-06-02'
    and to_date(t1.send_time)<='2022-06-15'
    )t1
join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt='20220616'
    ) t2 on t1.relation_id=t2.relation_id
group by case when t2.group_count<=1 then '(0,1]'
    when t2.group_count<=2 then '(1,2]'
    when t2.group_count<=5 then '(2，5]'
    when t2.group_count<=10 then '(5,10]'
    when t2.group_count<=20 then '(10,20]'
    when t2.group_count>20 then '(20+)' end;







-- 当日收到bb机消息人数达成消息人数
select to_date(t1.send_time)dt_date,t2.is_new_user
    ,count(distinct t1.id)receive_cnt
    ,count(distinct case when to_date(t1.update_time)=to_date(t1.send_time)
        and t1.status=2 then t1.id end)dr_respond_cnt
    ,count(distinct case when t1.status=2 then t1.id end)all_respond_cnt
    ,count(distinct t1.receiver_uid)receive_user
    ,count(distinct case when to_date(t1.update_time)=to_date(t1.send_time)
        and t1.status=2 then t1.receiver_uid end) dr_respond_user
    ,count(distinct case when t1.status=2 then t1.receiver_uid end)all_respond_user
    -- ,count(distinct t3.user_id)receive_cl_user
    -- ,count(distinct case when t1.status=2 then t3.user_id end)respond_cl_user
    -- ,count(distinct case when t1.status=1 then t3.user_id end)respond_cl_user
from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
left join dwd_tg_user_login_detail_day t2 on to_date(t1.send_time)=t2.login_date and t1.receiver_uid=t2.user_id
    and t2.dt>='20220601'
-- left join dwd_tg_user_login_detail_day t3 on to_date(dateadd(t1.send_time,1,'dd'))=to_date(to_date(t3.dt,'yyyymmdd')) and t1.receiver_uid = t3.user_id
--     and t3.dt>='20220601'
where t1.dt='20220616'
-- and to_date(to_date(t1.dt,'YYYYMMDD'))=to_date(t1.send_time)
-- and to_date(t1.update_time)=to_date(t1.send_time)
and t1.send_time>='2022-06-01 00:00:00'
group by to_date(t1.send_time),t2.is_new_user;




-- 当日收到bb机消息人数达成消息人数
select to_date(t1.send_time)dt_date,t2.is_new_user
    ,count(distinct t1.id)receive_cnt
    ,count(distinct case when t1.status=2 then t1.id end)respond_cnt
    ,count(distinct t1.receiver_uid)receive_user
    ,count(distinct case when t1.status=2 then t1.receiver_uid end)respond_user
    ,count(distinct t3.user_id)receive_cl_user
    ,count(distinct case when t1.status=2 then t3.user_id end)respond_cl_user
    ,count(distinct case when t1.status=1 then t3.user_id end)respond_cl_user
from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
left join dwd_tg_user_login_detail_day t2 on to_date(t1.send_time)=t2.login_date and t1.receiver_uid=t2.user_id
    and t2.dt>='20220601'
left join dwd_tg_user_login_detail_day t3 on to_date(dateadd(t1.send_time,1,'dd'))=to_date(to_date(t3.dt,'yyyymmdd')) and t1.receiver_uid = t3.user_id
    and t3.dt>='20220601'
where t1.dt='20220616'
-- and to_date(to_date(t1.dt,'YYYYMMDD'))=to_date(t1.send_time)
and to_date(t1.update_time)=to_date(t1.send_time)
group by to_date(t1.send_time),t2.is_new_user;


-- select to_date(t1.send_time)dt_date,t2.is_new_user
--     ,count(distinct t1.receiver_uid)receive_user
--     ,count(distinct case when t1.status=2 then t1.receiver_uid end)respond_user
--     ,count(distinct t3.user_id)receive_cl_user
--     ,count(distinct case when t1.status=2 then t3.user_id end)respond_cl_user
-- from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
-- left join dwd_tg_user_login_detail_day t2 on to_date(t1.send_time)=t2.login_date and t1.receiver_uid=t2.user_id
--     and t2.dt>='20220601'
-- left join dwd_tg_user_login_detail_day t3 on to_date(dateadd(t1.send_time,1,'dd'))=to_date(to_date(t3.dt,'yyyymmdd')) and t1.receiver_uid = t3.user_id
--     and t3.dt>='20220601'
-- where t1.dt>='20220601'
-- and to_date(to_date(t1.dt,'YYYYMMDD'))=to_date(t1.send_time)
-- group by to_date(t1.send_time),t2.is_new_user;


-- on dateadd(to_date(t1.dt,'yyyymmdd'),1,'dd') = to_date(t2.dt,'yyyymmdd')
-- and t1.user_id = t2.user_id


-- 当日收到bb机消息组数达成消息组数
select to_date(send_time)dt_date,t2.is_new_user
    ,count(distinct t1.id)receive_cnt
    ,count(distinct case when t1.status=2 then t1.id end)respond_cnt
from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
left join dwd_tg_user_login_detail_day t2 on to_date(t1.send_time)=t2.login_date and t1.receiver_uid=t2.user_id
    and t2.dt>='20220601'
where t1.dt>='20220601'
and to_date(to_date(t1.dt,'YYYYMMDD'))=to_date(t1.send_time)
group by to_date(t1.send_time),t2.is_new_user;

-- 当日收到bb机消息人数达成消息人数
select to_date(t1.send_time)dt_date,t2.is_new_user
    ,count(distinct t1.receiver_uid)receive_user
    ,count(distinct case when t1.status=2 then t1.receiver_uid end)respond_user
from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
left join dwd_tg_user_login_detail_day t2 on to_date(t1.send_time)=t2.login_date and t1.receiver_uid=t2.user_id
    and t2.dt>='20220601'
where t1.dt>='20220601'
and to_date(to_date(t1.dt,'YYYYMMDD'))=to_date(t1.send_time)
group by to_date(t1.send_time),t2.is_new_user;


-- select to_date(send_time)dt_date,count(distinct receiver_uid)receive_user
--     ,count(distinct case when status=2 then receiver_uid end)respond_user
-- from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
-- where to_date(to_date(dt,'YYYYMMDD'))=to_date(send_time)
-- group by to_date(send_time);

-- select to_date(send_time)dt_date,receiver_uid,count(distinct id)receive_cnt
--     ,count(distinct case when status=2 then id end)respond_cnt
-- from ods_mtapp_im_beep_chicken_msg_receiver_record_df t1
-- where dt='20220616' 
-- group by to_date(send_time)


SELECT split(relation_id,'_')[0] uid1,split(relation_id,'_')[1] uid2,*
from ods_mtapp_im_together_chat_stat_df
where dt='20220601';



select dt,concat(case when fromuid<touid then fromuid else touid end
    ,'_',case when fromuid<touid then touid else fromuid end) relation_id
from data_platform.ods_kafka_chat_service_message t1
where t1.dt>='20220530'
and t1.dt<='20220605'
and t1.appid=80



select t1.date
    ,count(distinct biz_id) relation_cnt
    ,count(distinct new_uid)user_cnt
    ,sum(group_count)/2 group_count
from(
    SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    lateral view explode(split(relation_id,'_'))tablead as new_uid
    where to_date(create_time)>='2022-05-30'
    and dt='20220605'
    -- and to_date(to_date(Dt,'YYYYMMDD'))=to_date(create_time)
    )t1
group by t1.date;

select count(distinct t1.relation_id)cnt  
    ,count(distinct t2.relation_id) yq
from(
    select dt,concat(case when fromuid<touid then fromuid else touid end
        ,'_',case when fromuid<touid then touid else fromuid end) relation_id
    from data_platform.ods_kafka_chat_service_message t1
    where t1.dt>='20220530'
    and t1.dt<='20220605'
    and t1.appid=80
    )t1
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt='20220529'
    ) t2 on t1.relation_id=t2.relation_id;


select t1.mobile,t1.show_no,concat('A',t1.uid)uid,t2.status
    ,count(distinct t2.id)input_cnt
from temp_zjj_tg_uid_0505 t1
left join data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t2 on t1.uid=t2.uid
    and t2.dt>='20220429' and t2.dt<='20220506'
group by t1.mobile,t1.show_no,concat('A',t1.uid),t2.status;


select t1.mobile,t1.show_no,concat('A',t1.uid)uid
    ,count(distinct t2.id)input_cnt
    ,count(distinct t2.rocord_id)input_click_cnt
from temp_zjj_tg_uid_0505 t1
left join data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t2 on t1.uid=t2.uid
    and t2.dt>='20220429' and t2.dt<='20220506'
group by t1.mobile,t1.show_no,concat('A',t1.uid);


select t1.mobile,concat('A',t1.uid)uid,t2.status,count(distinct t2.id)respond_cnt
from temp_zjj_tg_uid_0505 t1
left join data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t2 on t1.uid=t2.receiver_uid and t2.status=2
    and t2.dt>='20220429' and t2.dt<='20220506'
group by t1.mobile,concat('A',t1.uid),t2.status;













select count(distinct user_id) MAU
    ,count(distinct case when is_new_user=1 then user_id end)MNU
from dwd_tg_user_login_detail_day t
where dt>='20220501'
and dt<='20220601'


ods_user_relation_t_relation_counter
粉丝表 t_user_be_relation
那就relation_key =follow relationvalue = 1
看某个用户每日新增的粉丝数，只能看这个表： ods_app_relation_domain_t_user_relation_day


select use
from ods_app_relation_domain_t_user_relation_day  relation_key ='friend' and relationValue =1






-- select follow_cnt,count(0)
-- from(
-- select from_id,count(distinct to_id)follow_cnt
-- from data_platform.ods_app_relation_domain_t_user_relation_day
-- where app_id='80'
-- and relation_key ='follow' 
-- and relation_Value =1
-- group by from_id
-- )t 
-- group by follow_cnt;

select follow_cnt,count(0)
from(
    select from_id,count(distinct to_id)follow_cnt
    from ods_app_relation_domain_t_user_relation_day t1
    join(
        select user_id
        from dwd_tg_user_login_detail_day t
        where dt>='20220501'
        group by user_id
        )t2 on t1.from_id=t2.user_id 
    where app_id='80'
    and relation_key ='follow' 
    and relation_Value =1
    group by from_id
    )t 
group by follow_cnt;



select follow_cnt,count(0)
from(
    select to_id,count(distinct to_id)follow_cnt
    from ods_app_relation_domain_t_user_relation_day t1
    join(
        select user_id
        from dwd_tg_user_login_detail_day t
        where dt>='20220501'
        group by user_id
        )t2 on t1.from_id=t2.user_id 
    where app_id='80'
    and relation_key ='follow' 
    and relation_Value =1
    group by to_id
    )t 
group by follow_cnt;




select friend_cnt,count(0)
from(
    select from_id,count(distinct to_id)friend_cnt
    from ods_app_relation_domain_t_user_relation_day t1 
    join(
        select user_id
        from dwd_tg_user_login_detail_day t
        where dt>='20220501'
        group by user_id
        )t2 on t1.from_id=t2.user_id 
    where app_id='80'
    and relation_key ='friend' 
    and relation_Value =1
    group by from_id
    )t
group by friend_cnt;






select t1.*,t3.*
from ods_kafka_chat_service_message t1
join dwd_app_user_info t2 on t1.fromuid=t2.user_id
join temp_zjj_tg_comment_showno1 t3 on t2.show_no=t3.show_no
where t1.dt='20220605'
and t1.appid=80





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
            and t1.dt>='20220511' and t1.status=5 
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

---- 暖评数据







--IM相关
SELECT case when tt.group_count<=1 then '(0,1]'
    when tt.group_count<=2 then '(1,2]'
    when tt.group_count<=5 then '(2，5]'
    when tt.group_count<=10 then '(5,10]'
    when tt.group_count<=20 then '(10,20]'
    when tt.group_count>20 then '(20+)' end as group_count
    ,count(0)
from data_platform.ods_mtapp_im_together_chat_stat_df tt
-- lateral view explode(split(relation_id,'_'))tablead as new_uid
where dt='20220605'
and to_date(create_time)>='2022-05-30'
and to_date(create_time)<='2022-06-05'
group by case when tt.group_count<=1 then '(0,1]'
    when tt.group_count<=2 then '(1,2]'
    when tt.group_count<=5 then '(2，5]'
    when tt.group_count<=10 then '(5,10]'
    when tt.group_count<=20 then '(10,20]'
    when tt.group_count>20 then '(20+)'
    end;

select t1.date
    ,count(distinct biz_id) relation_cnt
    ,count(distinct new_uid)user_cnt
    ,sum(group_count)/2 group_count
from(
    SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    lateral view explode(split(relation_id,'_'))tablead as new_uid
    where to_date(create_time)>='2022-05-30'
    and dt='20220605'
    -- and to_date(to_date(Dt,'YYYYMMDD'))=to_date(create_time)
    )t1
group by t1.date;

select count(distinct t1.relation_id)cnt  
    ,count(distinct t2.relation_id) yq
from(
    select dt,concat(case when fromuid<touid then fromuid else touid end
        ,'_',case when fromuid<touid then touid else fromuid end) relation_id
    from data_platform.ods_kafka_chat_service_message t1
    where t1.dt>='20220530'
    and t1.dt<='20220605'
    and t1.appid=80
    )t1
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt='20220529'
    ) t2 on t1.relation_id=t2.relation_id;











-- select t1.date
--     ,count(distinct biz_id) relation_cnt
--     ,count(distinct new_uid)user_cnt
--     ,sum(group_count)/2 group_count
-- from(
--     SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
--     from data_platform.ods_mtapp_im_together_chat_stat_df tt
--     lateral view explode(split(relation_id,'_'))tablead as new_uid
--     where to_date(create_time)>='2022-05-30'
--     and dt='20220605'
--     -- and to_date(to_date(Dt,'YYYYMMDD'))=to_date(create_time)
--     )t1
-- group by t1.date;

上下文是想衡量一下强更的影响

我理解可以看下部分低版本用户的注册成功率和之前对比基本就是损失的比例吧
具体

-- select
--     count(distinct concat(case when fromuid<touid then fromuid else touid end
--         ,case when fromuid<touid then touid else fromuid end,'_'))cnt  
--     ,count(distinct t2.relation_id) yq
-- from data_platform.ods_kafka_chat_service_message t1
-- left join(
--     SELECT to_date(create_time)date,relation_id,tt.group_count
--     from data_platform.ods_mtapp_im_together_chat_stat_df tt
--     -- lateral view explode(split(relation_id,'_'))tablead as new_uid
--     where dt='20220529'
--     ) t2 on t2.relation_id=concat(case when fromuid<touid then fromuid else touid end
--         ,case when fromuid<touid then touid else fromuid end,'_')
-- where t1.dt>='20220530'
-- and t1.dt<='20220605'
-- and t1.appid=80;

-- select count(distinct t1.relation_id)cnt  
--     ,count(distinct t2.relation_id) yq
-- from(
--     select dt,concat(case when fromuid<touid then fromuid else touid end
--             ,case when fromuid<touid then touid else fromuid end,'_') relation_id
--     from data_platform.ods_kafka_chat_service_message t1
--     where t1.dt>='20220530'
--     and t1.dt<='20220605'
--     and t1.appid=80
--     )t1
-- left join(
--     SELECT to_date(create_time)date,relation_id,tt.group_count
--     from data_platform.ods_mtapp_im_together_chat_stat_df tt
--     -- lateral view explode(split(relation_id,'_'))tablead as new_uid
--     where dt='20220529'
--     ) t2 on t1.relation_id=t2.relation_id;


select count(distinct t1.relation_id)cnt  
    ,count(distinct t2.relation_id) yq
from(
    select dt,concat(case when fromuid<touid then fromuid else touid end
        ,'_',case when fromuid<touid then touid else fromuid end) relation_id
    from data_platform.ods_kafka_chat_service_message t1
    where t1.dt>='20220530'
    and t1.dt<='20220605'
    and t1.appid=80
    )t1
left join(
    SELECT to_date(create_time)date,relation_id,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    -- lateral view explode(split(relation_id,'_'))tablead as new_uid
    where dt='20220529'
    ) t2 on t1.relation_id=t2.relation_id;

SELECT case when tt.group_count<=1 then '(0,1]'
    when tt.group_count<=2 then '(1,2]'
    when tt.group_count<=5 then '(2，5]'
    when tt.group_count<=10 then '(5,10]'
    when tt.group_count<=20 then '(10,20]'
    when tt.group_count>20 then '(20+)' end as group_count
    ,count(0)
from data_platform.ods_mtapp_im_together_chat_stat_df tt
-- lateral view explode(split(relation_id,'_'))tablead as new_uid
where dt='20220605'
and to_date(create_time)>='2022-05-30'
and to_date(create_time)<='2022-06-05'
group by case when tt.group_count<=1 then '(0,1]'
    when tt.group_count<=2 then '(1,2]'
    when tt.group_count<=5 then '(2，5]'
    when tt.group_count<=10 then '(5,10]'
    when tt.group_count<=20 then '(10,20]'
    when tt.group_count>20 then '(20+)'
    end;


select t1.date
    ,count(distinct biz_id) relation_cnt
    ,count(distinct new_uid)user_cnt
    ,sum(group_count)/2 group_count
from(
    SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    lateral view explode(split(relation_id,'_'))tablead as new_uid
    where to_date(create_time)>='2022-05-30'
    and dt>='20220530'
    and to_date(to_date(Dt,'YYYYMMDD'))=to_date(create_time)
    )t1
group by t1.date;


-- ods_kafka_chat_service_message




select date
    ,count(distinct user_id)dau
    ,count(distinct biz_id)relation_cnt
    ,count(distinct new_uid)user_cnt
    ,sum(group_count)group_count
from(
    select t1.date,t2.user_id,t1.biz_id,t1.new_uid
        ,max(group_count)group_count
    from dwd_tg_user_login_detail_day t2
    left join(
        SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
        from data_platform.ods_mtapp_im_together_chat_stat_df tt
        lateral view explode(split(relation_id,'_'))tablead as new_uid
        where to_date(create_time)>='2022-05-30'
        and dt>='20220530'
        )t1 on t1.date=t2.login_date and t1.new_uid=t2.user_id 
    where t2.dt>='20220530'
    group by t1.date,t2.user_id,t1.biz_id,t1.new_uid
    )tt
group by date;




select date,is_new_user
    ,count(distinct biz_id)cnt
    ,count(distinct new_uid)user_cnt
    ,sum(group_count)group_count
from(
    select t1.date,t1.biz_id,t1.new_uid,max(t2.is_new_user)is_new_user
        ,max(group_count)group_count
    from(
        SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
        from data_platform.ods_mtapp_im_together_chat_stat_df tt
        lateral view explode(split(relation_id,'_'))tablead as new_uid
        where to_date(create_time)>'2022-05-10'
        )t1
    join dwd_tg_user_login_detail_day t2 on t1.date=t2.login_date and t1.new_uid=t2.user_id 
        and t2.dt>='20220510'
    group by t1.date,t1.biz_id,t1.new_uid
    )tt
group by date,is_new_user;





SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
from data_platform.ods_mtapp_im_together_chat_stat_df tt
lateral view explode(split(relation_id,'_'))tablead as new_uid
where to_date(create_time)>'2022-05-30'



select t3.dt,t1.show_no,t3.uid,t3.rk,count(distinct t3.content_id)comment_cnt
from temp_zjj_tg_comment_showno t1 
join dwd_app_user_info t2 on t1.show_no=t2.show_no
left join(
    select t1.dt,t1.content_id,t1.author_id,t2.uid
        ,row_number() over(partition by t1.content_id order by t2.create_time ) rk
    from data_platform.ods_app_content_main_day t1
    left join data_platform.ods_content_interaction_t_comment_day t2 on cast(t1.content_id as string)=t2.biz_id 
        and t1.dt=t2.dt and t2.biz_type='220' and t2.type=1 and t2.status=1-- 评论
    where t1.content_type = '139' -- 糖果的动态
    and t1.dt>='20220511'
    and t1.status=5
    and t1.author_id<>t2.uid
    )t3 on t2.user_id=t3.uid and t3.rk<=3
group by t3.dt,t1.show_no,t3.uid,t3.rk;


-- SELECT to_date(create_time)date
--     ,case when tt.group_count<=2 then '(0,2]'
--             when tt.group_count<=5 then '（2，5]'
--             when tt.group_count<=10 then '(5,10]'
--             when tt.group_count<=20 then '(10,20]'
--             when tt.group_count>20 then '(20+)'
--             end as group_count
--     ,count(0)
-- from data_platform.ods_mtapp_im_together_chat_stat_df tt
-- -- lateral view explode(split(relation_id,'_'))tablead as new_uid
-- where dt='20220605'
-- and to_date(create_time)>='2022-05-30'
-- and to_date(create_time)<='2022-06-05'
-- group by to_date(create_time),case when tt.group_count<=2 then '(0,2]'
--             when tt.group_count<=5 then '（2，5]'
--             when tt.group_count<=10 then '(5,10]'
--             when tt.group_count<=20 then '(10,20]'
--             when tt.group_count>20 then '(20+)'
--             end;

-- SELECT case when tt.group_count<=2 then '(0,2]'
--             when tt.group_count<=5 then '（2，5]'
--             when tt.group_count<=10 then '(5,10]'
--             when tt.group_count<=20 then '(10,20]'
--             when tt.group_count>20 then '(20+)'
--             end as group_count
--     ,count(0)
-- from data_platform.ods_mtapp_im_together_chat_stat_df tt
-- -- lateral view explode(split(relation_id,'_'))tablead as new_uid
-- where to_date(create_time)>='2022-05-30'
-- and to_date(create_time)<='2022-06-05'
-- group by case when tt.group_count<=2 then '(0,2]'
--             when tt.group_count<=5 then '（2，5]'
--             when tt.group_count<=10 then '(5,10]'
--             when tt.group_count<=20 then '(10,20]'
--             when tt.group_count>20 then '(20+)'
--             end;



select t1.date
    ,count(distinct biz_id) cnt
    ,count(distinct new_uid)user_cnt
    ,sum(group_count)/2 group_count
from(
    SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    lateral view explode(split(relation_id,'_'))tablead as new_uid
    where to_date(create_time)>'2022-05-30'
    )t1
group by t1.date;



-- SELECT to_date(create_time)date
--     ,count(distinct )
--     ,count(distinct relation_id)cnt,sum(group_count)group_count
-- from data_platform.ods_mtapp_im_together_chat_stat_df
-- where to_date(create_time)>'2022-05-01'
-- group by to_date(create_time);

select t1.date
    ,count(distinct biz_id) cnt
    ,count(distinct new_uid)user_cnt
    ,sum(group_count)/2 group_count
from(
    SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    lateral view explode(split(relation_id,'_'))tablead as new_uid
    where to_date(create_time)>'2022-05-10'
    )t1
group by t1.date;

select date,is_new_user
    ,count(distinct biz_id)cnt
    ,count(distinct new_uid)user_cnt
    ,sum(group_count)group_count
from(
    select t1.date,t1.biz_id,t1.new_uid,max(t2.is_new_user)is_new_user
        ,max(group_count)group_count
    from(
        SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
        from data_platform.ods_mtapp_im_together_chat_stat_df tt
        lateral view explode(split(relation_id,'_'))tablead as new_uid
        where to_date(create_time)>'2022-05-10'
        )t1
    left join dwd_tg_user_login_detail_day t2 on t1.date=t2.login_date and t1.new_uid=t2.user_id 
        and t2.dt>='20220510'
    group by t1.date,t1.biz_id,t1.new_uid
    )tt
group by date,is_new_user;




select t1.date
    ,count(distinct biz_id) cnt
    ,count(distinct new_uid)user_cnt
    ,sum(group_count)/2 group_count
from(
    SELECT to_date(create_time)date,biz_id,relation_id,new_uid,tt.group_count
    from data_platform.ods_mtapp_im_together_chat_stat_df tt
    lateral view explode(split(relation_id,'_'))tablead as new_uid
    where to_date(create_time)>'2022-05-10'
    )t1
group by t1.date;


select t1.uid,t1.new,t2.tag_name
from(
    select *
    from data_platform.ods_mt_user_info_user_own_tag_v2_df tt
    lateral view explode(split(tags,','))tablead as new
    where tt.dt='20220518'
    )t1
left join data_platform.ods_mt_user_info_user_tag_config_v2_df t2 
    on t1.new=t2.tag_id and t2.dt='20220518'





SEC67a996c35f6901ee79de54e2bee6456dc6defbadee9df0d713c1ab8481f20b59



ods_mt_user_info_user_own_tag_v2_df
ods_mt_user_info_user_tag_config_v2_df



select t3.dt,t3.uid,t3.rk,count(distinct t3.content_id)comment_cnt
from temp_zjj_tg_comment_showno t1 
join dwd_app_user_info t2 on t1.show_no=t2.show_no
left join(
    select t1.dt,t1.content_id,t1.author_id,t2.uid
        ,row_number() over(partition by t1.content_id order by t2.create_time ) rk
    from data_platform.ods_app_content_main_day t1
    left join data_platform.ods_content_interaction_t_comment_day t2 on cast(t1.content_id as string)=t2.biz_id 
        and t1.dt=t2.dt and t2.biz_type='220' and t2.type=1 and t2.status=1-- 评论
    where t1.content_type = '139' -- 糖果的动态
    and t1.dt>='20220511'
    and t1.status=5
    and t1.author_id<>t2.uid
    )t3 on t2.user_id=t3.uid and t3.rk<=3
group by t3.dt,t3.uid,t3.rk;















select t1.uid,t1.new,t2.tag_name
from(
    select *
    from data_platform.ods_mt_user_info_user_own_tag_v2_df tt
    lateral view explode(split(tags,','))tablead as new
    where tt.dt='20220518'
    )t1
left join data_platform.ods_mt_user_info_user_tag_config_v2_df t2 
    on t1.new=t2.tag_id and t2.dt='20220518'





select case when t2.age<=15 then '10--15'
    when t2.age<=17 then '16--17'
    when t2.age<=23 then '18--23'
    when t2.age<=30 then '24--30'
    when t2.age<=40 then '31--40'
    when t2.age<=50 then '41--50'
    when t2.age>50 then '51+' end as age
    ,count(distinct t1.user_id)cnt
    ,count(distinct t3.uid)student_cnt
from dwd_tg_user_login_detail_day t1 
left join dwd_app_user_info t2 on t1.user_id=t2.user_id
left join(
    select tt.uid,tt.new
    from(
        select *
        from data_platform.ods_mt_user_info_user_own_tag_v2_df tt
        lateral view explode(split(tags,','))tablead as new
        where tt.dt='20220518'
        )tt
    where tt.new=5
    )t3 on t1.user_id=t3.uid 
where t1.dt >= '20220513'
and t1.dt<='20220518'
and t1.is_new_user=1
group by case when t2.age<=15 then '10--15'
    when t2.age<=17 then '16--17'
    when t2.age<=23 then '18--23'
    when t2.age<=30 then '24--30'
    when t2.age<=40 then '31--40'
    when t2.age<=50 then '41--50'
    when t2.age>50 then '51+' end;


-- select case when t2.age<=15 then '10--15'
--     when t2.age<=17 then '16--17'
--     when t2.age<=23 then '18--23'
--     when t2.age<=30 then '24--30'
--     when t2.age<=40 then '31--40'
--     when t2.age<=50 then '41--50'
--     else '50+' end as age
--     ,count(distinct t1.user_id)cnt
--     ,count(distinct t3.uid)student_cnt
-- from dwd_tg_user_login_detail_day t1 
-- left join dwd_app_user_info t2 on t1.user_id=t2.user_id
-- left join(
--     select t1.uid,t1.new,t2.tag_name
--     from(
--         select *
--         from data_platform.ods_mt_user_info_user_own_tag_v2_df tt
--         lateral view explode(split(tags,','))tablead as new
--         where tt.dt='20220518'
--         )t1
--     left join data_platform.ods_mt_user_info_user_tag_config_v2_df t2 
--         on t1.new=t2.tag_id and t2.dt='20220518'
--     )t3 on t1.user_id=t3.uid and t3.new=5
-- where t1.dt >= '20220513'
-- and t1.dt<='20220518'
-- and t1.is_new_user=1
-- group by case when t2.age<=15 then '10--15'
--     when t2.age<=17 then '16--17'
--     when t2.age<=23 then '18--23'
--     when t2.age<=30 then '24--30'
--     when t2.age<=40 then '31--40'
--     when t2.age<=50 then '41--50'
--     else '50+' end;



-- select *
-- from ods_mt_user_info_user_own_tag_v2_df t1 
-- left join ods_mt_user_info_user_tag_config_v2_df t2 on t1.tag_id=t2.tag_id

-- select *,new 
-- from data_platform.ods_mt_user_info_user_own_tag_v2_df 
-- lateral view explode(split(tags,','))tablead as new;



select case when t2.age<=15 then '10--15'
    when t2.age<=17 then '16--17'
    when t2.age<=23 then '18--23'
    when t2.age<=30 then '24--30'
    when t2.age<=40 then '31--40'
    when t2.age<=50 then '41--50'
    else '50+' end as age
    ,count(distinct t1.user_id)cnt
    ,count(distinct t3.uis)ciliu_cnt
from dwd_tg_user_login_detail_day t1 
left join dwd_app_user_info t2 on t1.user_id=t2.user_id
left join dwd_tg_user_login_detail_day t3 on dateadd(to_date(t1.dt,'yyyymmdd'),1,'dd') = to_date(t3.dt,'yyyymmdd')
    and t1.user_id = t3.user_id and t3.dt='20220515'
where t1.dt = '20220514'
and t1.is_new_user=1
group by case when t2.age<=15 then '10--15'
    when t2.age<=17 then '16--17'
    when t2.age<=23 then '18--23'
    when t2.age<=30 then '24--30'
    when t2.age<=40 then '31--40'
    when t2.age<=50 then '41--50'
    else '50+' end;



drop table if exists tg_bi_login_user_info_0510_01;
create table tg_bi_login_user_info_0510_01 as 
select t1.dt 
, t1.user_id
, t1.is_new_user
, t2.user_id as ciri_user_id
, t2.dt as ciri_dt
, t3.user_id as 7d_user_id
, t3.dt as 7d_ciri_dt
from tg_bi_login_user_info_0510 t1 
left join tg_bi_login_user_info_0510 t2
on dateadd(to_date(t1.dt,'yyyymmdd'),1,'dd') = to_date(t2.dt,'yyyymmdd')
and t1.user_id = t2.user_id
left join tg_bi_login_user_info_0510 t3
on dateadd(to_date(t1.dt,'yyyymmdd'),7,'dd') = to_date(t3.dt,'yyyymmdd')
and t1.user_id = t3.user_id;




select user_id ,to_char(create_time,'yyyymmdd') as create_day
    , gender,status,device 
from dwd_app_user_info
where which_app='TG'
and to_char(create_time,'yyyymmdd') >= '20220501';









SELECT t1.ds,t2.is_new_user
    ,count(distinct uid)sy_cnt
    ,count(distinct case when getmapvalue(args,'topic_id')='1401239663926741124' then uid end)lianai_cnt
FROM ods_all_mobile_log t1
left join data_platform.dwd_tg_user_login_detail_day t2 on t1.uid=t2.user_id and t1.ds=t2.dt and t2.dt>='20220515'
where t1.page='PageId-EA8DA4GH' -- 14351 mobile 标签选择页面
and t1.arg1='ElementId-37A6D3HA'
and t1.ds>='20220515'
-- and getmapvalue(args,'topic_id')='1401239663926741124'
group by t1.ds,t2.is_new_user;



select *
from ods_mtapp_im_beep_chicken_msg_record_df
where dt=20220510
and to_date(send_time)>='2022-05-01'
and msg_content not like 'https://%';

select *
from ods_all_p2p_message 
where appid=80
and ds>=20220501;




content_manage_polar.content_main查糖果的动态id
content_manage_polar.content_anchor查动态id对应的素材id
material_center_polar.t_material_text根据素材id查具体文本



select *
from data_platform.ods_app_content_main_day t1  
left join data_platform.ods_app_content_anchor_day t2 on t1.content_id=t2.content_id
left join data_platform.ods_material_center_t_material_text_day t3 on t2.material_id=t3.id
where t1.content_type = '139' 
and t1.dt='20220505' 











select t1.mobile,t1.show_no,concat('A',t1.uid)uid,t2.status
    ,count(distinct t2.id)input_cnt
from temp_zjj_tg_uid_0505 t1
left join data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t2 on t1.uid=t2.uid
    and t2.dt>='20220429' and t2.dt<='20220506'
group by t1.mobile,t1.show_no,concat('A',t1.uid),t2.status;


select t1.mobile,t1.show_no,concat('A',t1.uid)uid
    ,count(distinct t2.id)input_cnt
    ,count(distinct t2.rocord_id)input_click_cnt
from temp_zjj_tg_uid_0505 t1
left join data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t2 on t1.uid=t2.uid
    and t2.dt>='20220429' and t2.dt<='20220506'
group by t1.mobile,t1.show_no,concat('A',t1.uid);


select t1.mobile,concat('A',t1.uid)uid,t2.status,count(distinct t2.id)respond_cnt
from temp_zjj_tg_uid_0505 t1
left join data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t2 on t1.uid=t2.receiver_uid and t2.status=2
    and t2.dt>='20220429' and t2.dt<='20220506'
group by t1.mobile,concat('A',t1.uid),t2.status;


select t11.mobile,t11.show_no
    ,concat('A',t11.uid)as uid  
    ,count(distinct t1.content_id) as send_dongtai_cnt
    ,count(distinct t2.id)comment_cnt 
    ,count(distinct t3.id)praise_cnt
from temp_zjj_tg_uid_0505 t11
left join data_platform.ods_app_content_main_day t1 on t11.uid=t1.author_id 
    and t1.content_type = '139' and t1.dt>='20220429' and t1.dt<='20220506' -- 糖果的动态
left join data_platform.ods_content_interaction_t_comment_day t2 on t1.content_id=t2.biz_id 
    and t2.biz_type in (139,220) and t2.dt>='20220429' and t2.dt<='20220506'-- 评论
left join data_platform.ods_content_interaction_t_praise_day t3 on t1.content_id=t3.biz_id 
    and t3.biz_type in (333,334) and t3.dt>='20220429'and t3.dt<='20220506'-- 点赞
group by t11.mobile,t11.show_no,concat('A',t11.uid);


select t1.mobile
    ,concat('A',t1.uid)as uid  
    ,count(distinct t2.id) as zd_comment_cnt
from temp_zjj_tg_uid_0505 t1
left join ods_content_interaction_t_comment_day t2 on t1.uid=t2.uid
    and t2.biz_type in('139','220') and t2.dt >= '20220429' and t2.dt <= '20220506'-- 糖果的动态
group by t1.mobile,concat('A',t1.uid);





select concat('A',t1.author_id) as uid  
, count(distinct t1.content_id) as send_dongtai_cnt
,count(distinct t2.id)comment_cnt 
,count(distinct t3.id)praise_cnt
from ods_app_content_main_day t1
left join ods_content_interaction_t_comment_day t2 on t1.content_id=t2.biz_id -- 评论
-- left join ods_content_interaction_t_praise_day t3 on t1.content_id=t2.biz_id -- 点赞
where content_type = '139' -- 糖果的动态
and dt>='20220511'
group by concat('A',t1.author_id);

-- drop table if exists temp_zjj_tg_comment_showno;
CREATE TABLE IF NOT EXISTS temp_zjj_tg_comment_showno
(
    show_no bigint
    ,nick_name string
);




-- 舞团活动数据
select dt,arg1
    ,count(0)pv
    ,count(distinct uid)uv
    ,count(distinct getmapvalue(args,'memberId'))member
from data_platform.ods_all_server_burying_point
where app_id=80
and dt>='20220504'
and dt<='20220516'
and arg1 in(
    'dance_team_detail'
    ,'dance_team_join'
    ,'dance_team_join_click'
    ,'create_dance_team_outside_click'
    ,'download_app_outside_click'
    )
group by dt,arg1;



-- select concat('A',t1.author_id) as uid  
-- , count(distinct t1.content_id) as send_dongtai_cnt
-- ,count(distinct t2.id)comment_cnt 
-- ,count(distinct t3.id)praise_cnt
-- from ods_app_content_main_day t1
-- left join ods_content_interaction_t_comment_day t2 on t1.content_id=t2.biz_id -- 评论
-- left join ods_content_interaction_t_praise_day t3 on t1.content_id=t2.biz_id -- 点赞
-- where content_type = '139' -- 糖果的动态
-- and dt>='20220429'
-- and dt<='20220506'
-- group by concat('A',t1.author_id);


-- -- part two 互动 begin --
-- -- 点赞人数
-- drop table if exists temp_lzj_tg_dongtai_praise_last_90days;
-- create table temp_lzj_tg_dongtai_praise_last_90days as -- 区别于普通的
-- select *
-- from data_platform.ods_content_interaction_t_praise_day
-- where biz_type in('220','139') -- 糖果的动态
-- and status = 1
-- and dt>='20220429'
-- and dt<='20220506'

-- -- 糖果动态的评论数据（评论）
-- drop table if exists temp_lzj_tg_dongtai_comment_last_90days;
-- create table temp_lzj_tg_dongtai_comment_last_90days as -- 区别于普通的
-- select dt as ds
-- , uid as action_uid-- 发起评论的用户uid
-- , count(1) as action_cnt
-- , 'comment' as action_name
-- from ods_content_interaction_t_comment_day
-- where biz_type in('220','139') -- 糖果的动态
-- and status = 1
-- and dt>='20220429'
-- and dt<='20220506'
-- group by uid ,dt;


select t1.mobile,concat('A',t1.uid)uid,t2.status,count(distinct t2.id)input_cnt
from(
    select * 
    from temp_lzj_tg_uid_0505 t11
    union all 
    select '13818551962',221271192250672663
    )t1
left join data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t2 on t1.uid=t2.uid
    and t2.dt>='20220429' and t2.dt<='20220506'
group by t1.mobile,concat('A',t1.uid),t2.status;


select t1.mobile,concat('A',t1.uid)uid,t2.status,count(distinct t2.id)respond_cnt
from(
    select * 
    from temp_lzj_tg_uid_0505 t
    union all 
    select '13818551962',221271192250672663
    )t1
left join data_platform.ods_mtapp_im_beep_chicken_msg_receiver_record_df t2 on t1.uid=t2.receiver_uid and t2.status=2
    and t2.dt>='20220429' and t2.dt<='20220506'
group by t1.mobile,concat('A',t1.uid),t2.status;


select t11.mobile
    ,concat('A',t11.uid)as uid  
	,count(distinct t1.content_id) as send_dongtai_cnt
	,count(distinct t2.id)comment_cnt 
	,count(distinct t3.id)praise_cnt
from(
    select * 
    from temp_lzj_tg_uid_0505 t
    union all 
    select '13818551962',221271192250672663
    )t11
left join data_platform.ods_app_content_main_day t1 on t11.uid=t1.author_id 
    and t1.content_type = '139' and t1.dt>='20220429' and t2.dt<='20220506' -- 糖果的动态
left join data_platform.ods_content_interaction_t_comment_day t2 on t1.content_id=t2.biz_id 
    and t2.biz_type in (139,220) and t2.dt>='20220429' and t2.dt<='20220506'-- 评论
left join data_platform.ods_content_interaction_t_praise_day t3 on t1.content_id=t3.biz_id 
    and t3.biz_type in (333,334) and t3.dt>='20220429'and t3.dt<='20220506'-- 点赞
group by t11.mobile,concat('A',t11.uid);


select t1.mobile
    ,concat('A',t1.uid)as uid  
    ,count(distinct t2.id) as zd_comment_cnt
from(
    select * 
    from temp_lzj_tg_uid_0505 t11
    union all 
    select '13818551962',221271192250672663
    )t1
left join ods_content_interaction_t_comment_day t2 on t11.uid=t2.uid
and t2.biz_type in('139','220') and t2.dt >= '20220429' and t2.dt <= '20220506'-- 糖果的动态
group by t1.mobile,concat('A',t1.uid);

