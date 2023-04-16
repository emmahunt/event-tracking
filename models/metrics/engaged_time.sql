with web_events as (select * from {{ ref('stg_web_events') }})
, bounce_rate as (select * from {{ ref('bounce_rate') }})

-- Engaged time per page view will be calculated by multiplying the number of pings by the ping 'heartbeat' / period
, engaged_time as (
    select 
        page_view_id
        , count_if(event_name = 'page_ping') as num_pings

        -- From analysis of the data, we can infer that the time between page pings is 10 seconds
        , count_if(event_name = 'page_ping') * 10 as engaged_time_seconds
        , min(device_created_timestamp::date) as date_of_page_view
    from web_events
    group by 1
)

, page_views_agg as (
    select
        date_of_page_view
        , sum(engaged_time_seconds) as total_engaged_time_seconds
        , count(distinct page_view_id) as number_of_page_views
    from engaged_time
    group by 1
) 

, calculate_metric as (
    select 
        page_views_agg.date_of_page_view
        , page_views_agg.total_engaged_time_seconds
        , page_views_agg.number_of_page_views

        -- By definition, number of bounces = number of page views that bounced, as a bounce is a session with only 1 page view in it
        , bounce_rate.number_of_bounces
        , div0(page_views_agg.total_engaged_time_seconds, (number_of_page_views - number_of_bounces)) as average_page_engaged_time_seconds
    from page_views_agg
    left join bounce_rate
        on page_views_agg.date_of_page_view = bounce_rate.session_start_date
    group by 1, 2, 3, 4
)

, final as (
    select 
        date_of_page_view
        , round(average_page_engaged_time_seconds, 0) as average_page_engaged_time_seconds
        , round(average_page_engaged_time_seconds / 60 , 1) as average_page_engaged_time_minutes
        , total_engaged_time_seconds
        , number_of_page_views
        , number_of_bounces
    from calculate_metric
)

select * from final
