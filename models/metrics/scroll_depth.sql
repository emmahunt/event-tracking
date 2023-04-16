with web_events as (select * from {{ ref('stg_web_events') }})

-- Calculate some of the page parameters, e.g. y offsets and page height
, page_min_max as (
    select 
        page_view_id
        , min(device_created_timestamp::date) as date_of_page_view
        
        -- Use the maximum page height for calculation, as there are a number of cases where the page_height changes on a single page view
        -- e.g. c2ee0680-91f5-4420-a6fe-d268f22ddcf2
        , max(page_height) as max_page_height

        -- There are some cases where the minimum y offset is non-zero, e.g. the user may not have seen the top of the page
        -- For now, we'll ignore these cases as the metric is scroll depth, not necesearily the percentage of content seen
        , min(y_offset_min) as min_y_offset
        , max(y_offset_max) as max_y_offset
    from stg_web_events
    group by 1
)

-- Calculate the scroll depth for each page view
, page_view_average_depth as (
    select    
        page_view_id
        , date_of_page_view
        , max_page_height
        , max_y_offset
        , coalesce(max_y_offset / max_page_height, 0) * 100 as percent_of_page_seen
    from page_min_max
)

-- Calculate the average scroll depth at a daily level
, final as (
    select
        date_of_page_view
        , avg(percent_of_page_seen) as average_scroll_depth
    from page_view_average_depth
    group by 1
)

select * from final
