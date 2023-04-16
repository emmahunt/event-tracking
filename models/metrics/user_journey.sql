with next_page_host as (
    select     
        device_created_timestamp::date as date_journey_started
        , user_cookie
        , first_value(page_host_name) over (partition by user_cookie, session_id order by device_created_timestamp asc) as first_page_host
        , lead(page_host_name) over (partition by user_cookie, session_id order by device_created_timestamp asc) as next_page_view_host_name
    from stg_web_events
    where event_name = 'page_view'
    and session_increment = 1
)

, count_users_with_journey_to_track as (
    select 
        date_journey_started
        , count(distinct user_cookie) as number_of_users
        , count_if(first_page_host = 'snowplowanalytics.com' and next_page_view_host_name = 'discourse.snowplowanalytics.com') as users_with_valid_journey
    from next_page_host
    group by 1
)

, final as (
    select 
        date_journey_started
        , number_of_users
        , users_with_valid_journey
        , div0(users_with_valid_journey, number_of_users) * 100 as percent_of_users_with_user_journey
    from count_users_with_journey_to_track
)

select * from final
