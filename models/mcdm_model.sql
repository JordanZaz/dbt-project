WITH
bing_ads AS (
SELECT 
    date,
    NULL AS add_to_cart,
    clicks,
    NULL AS comments,
    NULL AS engagements,
    imps AS impressions,
    NULL AS installs,
    NULL AS likes,
    NULL AS link_clicks,
    NULL AS post_click_conversions,
    NULL AS post_view_conversions,
    NULL AS posts,
    NULL AS purchase,
    NULL AS registrations,
    revenue,
    NULL AS shares,
    spend,
    conv AS total_conversions,
    NULL AS video_views,
    CAST(ad_id AS STRING) AS ad_id,
    CAST(adset_id AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    channel,
    CAST(NULL AS STRING) AS creative_id,
    CAST(NULL AS STRING) AS placement_id
FROM {{ ref('src_ads_bing_all_data') }}
),

facebook_ads AS (
SELECT 
    date,
    add_to_cart,
    clicks,
    comments,
    NULL AS engagements,
    impressions,
    NULL AS installs,
    likes,
    inline_link_clicks AS link_clicks,
    NULL AS post_click_conversions,
    NULL AS post_view_conversions,
    NULL AS posts,
    purchase,
    complete_registration AS registrations,
    purchase_value AS revenue,
    shares,
    spend,
    NULL AS total_conversions,
    views AS video_views,
    CAST(ad_id AS STRING) AS ad_id,
    CAST(adset_id AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    channel,
    CAST(creative_id AS STRING) AS creative_id,
    CAST(NULL AS STRING) AS placement_id
FROM {{ ref('src_ads_creative_facebook_all_data') }}
),

tiktok_ads AS (
SELECT 
    date,
    add_to_cart,
    clicks,
    NULL AS comments,
    NULL AS engagements,
    impressions,
    rt_installs AS installs,
    NULL AS likes,
    NULL AS link_clicks,
    NULL AS post_click_conversions,
    NULL AS post_view_conversions,
    NULL AS posts,
    purchase,
    registrations,
    NULL AS revenue,
    NULL AS shares,
    spend,
    conversions AS total_conversions,
    video_views,
    CAST(ad_id AS STRING) AS ad_id,
    CAST(NULL AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    channel,
    CAST(NULL AS STRING) AS creative_id,
    CAST(NULL AS STRING) AS placement_id
FROM {{ ref('src_ads_tiktok_ads_all_data') }}
),

twitter_ads AS (
SELECT 
    date,
    NULL AS add_to_cart,
    clicks,
    comments,
    engagements,
    impressions,
    NULL AS installs,
    likes,
    url_clicks AS link_clicks,
    NULL AS post_click_conversions,
    NULL AS post_view_conversions,
    NULL AS posts,
    NULL AS purchase,
    NULL AS registrations,
    NULL AS revenue,
    retweets AS shares,
    spend,
    NULL AS total_conversions,
    video_total_views AS video_views,
    CAST(NULL AS STRING) AS ad_id,
    CAST(NULL AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    channel,
    CAST(NULL AS STRING) AS creative_id,
    CAST(NULL AS STRING) AS placement_id
FROM {{ ref('src_promoted_tweets_twitter_all_data') }}
)   



SELECT * FROM bing_ads
UNION ALL
SELECT * FROM facebook_ads
UNION ALL
SELECT * FROM tiktok_ads
UNION ALL
SELECT * FROM twitter_ads

