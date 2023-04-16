
with dataset as (select * from {{ ref('sa_skillset_dataset') }})

, cast_cols as (
    select 
        event_id
        , page_view_id
        , session_id
        , session_increment::int as session_increment
        , user_cookie
        , event_name
        , page_title
        , page_urlhostname as page_host_name
        , page_url
        , CASE
            WHEN referral_url = 'XXX' THEN 'unknown'
            ELSE referral_url
        END as referral_url
        , to_timestamp(concat(substr(dvce_created_tstamp, 7, 4), '-', substr(dvce_created_tstamp, 4, 2), '-', left(dvce_created_tstamp, 2), ' ', right(dvce_created_tstamp, 5))) as device_created_timestamp
        , upper(coalesce(geo_country, 'ZZ')) as country_code
        , geo_region as region
        , geo_city as city
        , geo_timezone as timezone
        , pp_xoffset_min::int as x_offset_min
        , pp_xoffset_max::int as x_offset_max
        , pp_yoffset_min::int as y_offset_min
        , pp_yoffset_max::int as y_offset_max
        , page_width::int as page_width
        , page_height::int as page_height
        , useragent_family as user_agent_family
        , os_family
        , device_family
        , case 
            when lower(useragent_family) like '%mobile%' then 'Mobile'
            when lower(useragent_family) like '%ios%' then 'Mobile'
            when lower(useragent_family) like '%ios%' then 'Mobile'
            when lower(useragent_family) like '%opera mini%' then 'Mobile'
            when lower(useragent_family) like '%android%' then 'Mobile'
            when lower(useragent_family) = '%other%' then 'Unknown'
            else 'Personal Computer'
        end as device_type
        , os_timezone
        , link_click_target_url
    from dataset

    -- This will exlcude 500 rows as at 2023-04-16
    -- These device types are understood to be bots and generating "bad" / non-human data
    -- The Googlebot in particular is generating many duplicate page view events on the same page view id
    where useragent_family not in ('Googlebot', 'YandexBot', 'PhantomJS')
)

-- Deduplicate true duplicate rows of data
, dedup as (
    select *
    from cast_cols

    -- Remove true duplicate events: those with the same event_id and all the same measure values
    qualify row_number() over (partition by event_id order by device_created_timestamp asc) = 1
)

-- Identify duplicate "real world" events: page views that occured on the same page_view_id at the same time
, duplicate_page_views as (
    select event_id
    from cast_cols
    where event_name = 'page_view'

    -- Identify duplicate page views: this step follows the assumption that the relationship between page view id and a real world "page view" is 1-1
    -- There are cases of 2 view events on the same page view id, sometimes at the exact same time
    qualify row_number() over (
        partition by page_view_id 
        order by device_created_timestamp asc) > 1
)

, final as (
    select dedup.*
    from dedup
    left join duplicate_page_views 
        on dedup.event_id = duplicate_page_views.event_id

    -- Exclude duplicate page views
    where duplicate_page_views.event_id is null
)

select * from final
