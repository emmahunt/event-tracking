with web_events as (select * from {{ ref('stg_web_events') }})

, examine_first_session as (
    select     
        device_created_timestamp::date as date_journey_started
        , user_cookie
        , page_host_name

        -- This will be used to identify if the user landed on the main site on the first page in their firstsession
        , row_number() over (
            partition by user_cookie
                , session_id 
            order by device_created_timestamp asc
        ) as page_view_ordinal_in_session

        -- This identifies when the user switched platforms onto the discourse site
        , conditional_change_event(page_host_name) over (
            partition by user_cookie
            , session_id 
            order by device_created_timestamp asc
        ) as changed_platforms
    from web_events
    where event_name = 'page_view'
    and session_increment = 1
)

-- This CTE is one row per user_cookie, and has 2 flag columns:
-- The first indiciates if the user started their journey on the main host platform
-- The second indicates if the user switched to the discourse platform at some point within their first session
, valid_user_journeys as (
    select 
        date_journey_started
        , user_cookie

        -- Use max() as we are aggregating over multiple page views per user
        , max(
            case
                when page_view_ordinal_in_session = 1 and page_host_name = 'snowplowanalytics.com' then 1
                else 0 
            end
        ) as started_journey_on_main_site
        , max(
            case
                when changed_platforms = 1 and page_host_name = 'discourse.snowplowanalytics.com' then 1
                else 0
            end
        ) as switched_to_discource_platform_in_session
    from examine_first_session

    -- Limit to just the 2 important page views in the user's first session
    where page_view_ordinal_in_session = 1
        or changed_platforms = 1
    group by 1, 2
)

-- Perform final counts and aggregations
, final as (
    select 
        date_journey_started
        , count(distinct user_cookie) as number_of_users
        , count_if(
            started_journey_on_main_site = 1
                and switched_to_discource_platform_in_session = 1
        ) as number_of_users_with_valid_user_journey
        , div0(
            count_if(
                started_journey_on_main_site = 1 
                    and switched_to_discource_platform_in_session = 1
            )
            , count(distinct user_cookie)
        ) * 100 as percent_of_users_with_user_journey
    from valid_user_journeys
    group by 1
)

select * from final
