with session_agg as (
    select 
        session_id
        , min(device_created_timestamp::date) as session_start_date
        , count_if(event_name = 'page_view') as number_of_page_views_in_session
        , count_if(event_name = 'page_ping') as number_of_page_pings_in_session
        , count_if(event_name = 'link_click') as number_of_link_clicks_in_session
    from {{ ref('stg_web_events') }}
    group by session_id
)

, bounces as (
    select
        session_id
        , session_start_date
    from session_agg
    where number_of_page_views_in_session = 1
        and number_of_page_pings_in_session = 0
        and number_of_link_clicks_in_session = 0
)

, final as (
    select
        session_agg.session_start_date
        , count(distinct session_agg.session_id) as number_of_sessions
        , count(distinct bounces.session_id) as number_of_bounces
        , round(div0(count(distinct bounces.session_id), count(distinct session_agg.session_id)) * 100,0) as bounce_rate_percent
    from session_agg
    left join bounces
        on session_agg.session_start_date = bounces.session_start_date
    group by 1
    order by 1 desc
)

select * from final
