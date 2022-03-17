{{ config(
    materialized = 'incremental'
    , unique_key = 'page_view_id'
) }}

WITH events AS (
    SELECT *
        , event_id AS page_view_id
    FROM {{ source('snowplow','events') }}
    {% if is_incremental() %}
    {# checks 4 conditions, Does this model already exists, is it a table, is it configured incremental, was full-refresh passed? Yes, Yes, Yes, No then incremental run #}
    -- WHERE collector_tstamp >= (SELECT max(max_collector_tstamp) FROM {{ this }})
    WHERE collector_tstamp >= (SELECT dateadd('day', -3, max(max_collector_tstamp)) FROM {{ this }})
    {# this, represents the currently existing database object mapped to this model #}
    {% endif %}
),
page_views AS (
    SELECT *
    FROM events
    WHERE event = 'page_view'
), 
aggregated_page_events AS (
    SELECT
        page_view_id
        , count(*) * 10 AS approx_time_on_page
        , min(derived_tstamp) AS page_view_start
        , max(collector_tstamp) AS max_collector_tstamp
    FROM events
    GROUP BY 1
),
joined AS (
    SELECT *
    FROM page_views
    LEFT JOIN aggregated_page_events USING (page_view_id)
), 
indexed AS (
    SELECT
        *
        , row_number() OVER (
            PARTITION BY domain_sessionid
            ORDER BY page_view_start
        ) AS page_view_in_session_index
        , row_number() OVER (
            PARTITION BY user_id
            ORDER BY page_view_start
        ) AS page_view_for_user_index
    FROM joined
)

SELECT * FROM indexed