{{ config(
    materialized='table',
    cluster_by=['experiment_handle', 'variant'],
) }}

-- Unified Funnel Experiment
-- T-test experiment: comparing total LTV between test variant and control

-- Gather experiment assignments already filtered for pageviews
with brochure_user_assignments AS (
  SELECT DISTINCT
    experiment_handle,
    subject_id,
    variant,
    first_assigned_at,
  FROM {{ source('mart_growth', 'brochure_user_shop_assignments') }}
),

-- geoblitz US cities to exclude
geoblitz_us as (
  select 'atlanta' as geoblitz_city, 'georgia' as geoblitz_state
  union all
  select 'chicago' as geoblitz_city, 'illinois' as geoblitz_state
  union all 
  select 'denver' as geoblitz_city, 'colorado' as geoblitz_state
  union all 
  select 'phoenix' as geoblitz_city, 'arizona' as geoblitz_state
  union all 
  select 'san diego' as geoblitz_city, 'california' as geoblitz_state
  union all 
  select 'san francisco' as geoblitz_city, 'california' as geoblitz_state
  union all 
  select 'las vegas' as geoblitz_city, 'nevada' as geoblitz_state
  union all 
  select 'new york' as geoblitz_city, 'new york' as geoblitz_state
),

-- Start with /POS page views 
pos_pageviews as (
  SELECT DISTINCT 
  multitrack_token, 
  session_token, 
  page_view_token, 
  viewed_at, 
  pageview_url, 
  regexp_extract(pageview_url, r'(/pos[^?#]*)') as pos_page

  from {{ source('marketing', 'unified_website_brochure_page_views') }} a 
  left join geoblitz_us b on a.geo.city = b.geoblitz_city and a.geo.subdivision = b.geoblitz_state
  WHERE regexp_extract(pageview_url, r'www.shopify.com(/pos[^?#]*)') in 
  ('/pos/free-trial/sell-retail',
    '/pos',
    '/pos/retail-pos',
    '/pos/pos-system-small-business',
    '/pos/multi-store-pos',
    '/pos/hardware',
    '/pos/pos-software',
    '/pos/pos-app',
    '/pos/ipad-pos',
    '/pos/android-pos',
    '/pos/omnichannel',
    '/pos/payments',
    '/pos/staff-management',
    '/pos/pos-inventory-system',
    '/pos/customization',
    '/pos/features',
    '/pos/pricing'
    )
    AND pageview_url not like '%accounts%'
    AND pageview_url not like '%admin%'
    AND b.geoblitz_city is null -- exclude geoblitz cities
), 

-- Did /pos pageview result in a click? 
button_clicks AS (
  SELECT DISTINCT
    page_view_token, 
    component_tree_click, 
    inner_text_click, 
    lower(regexp_extract(recirculation_click, r'([^?]*)')) as destination,

    FROM {{ ref('base__enriched_dux_unified_merchant_events_v1') }}
    WHERE event_type = 'dux_click'
    and lower(regexp_extract(recirculation_click, r'([^?]*)')) in ('#contact-sales', '/pos/request-info', 'https://accounts.shopify.com/store-create')
    and is_production = 'true'
    and is_bot = 'false'
    and page_view_token in (select distinct page_view_token from pos_pageviews)
), 

-- aggregate to 1 row per session; 
pageview_sessions as (
  SELECT 
    p.multitrack_token, 
    p.session_token, 
    MIN(p.viewed_at) as session_start,
    MAX(c.page_view_token is not null) as page_click,
    MAX(c.page_view_token is not null and c.destination like '%store-create%') as free_trial_click,
    MAX(c.page_view_token is not null and c.destination in ('#contact-sales', '/pos/request-info')) as talk_to_sales_click,
    
  FROM pos_pageviews p 
  LEFT JOIN button_clicks c on p.page_view_token = c.page_view_token
  GROUP BY 1,2
),

-- Check for retail lead following /pos page click
retail_lead_subs_and_mqls AS (
  SELECT 
    lse.tokenized_user_token,
    lse.session_token, 

    lse.lead_submission_id,
    lse.lead_id,
    l.converted_opportunity_id, 

    lse.lead_submission_at,
    l.created_at as lead_created_at, 
    l.new_sales_ready_at,
    l.opportunity_at,

    FROM {{ ref('lead_submission_events') }} AS lse
    LEFT OUTER JOIN {{ source('sales', 'salesforce_leads') }} AS l
      ON lse.lead_id = l.lead_id
      AND lse.lead_submission_at < l.created_at
    WHERE lower(lse.primary_product_interest) in ('pos pro', 'retail', 'retail payments')
      AND lse.session_token in (select distinct session_token from pageview_sessions where page_click)
      AND NOT lse.is_rejected
), 

-- Check for retail self service free trial lead following /pos page click
shop_signups as (
  SELECT DISTINCT
    shop_id, 
    multitrack_token, 
    created_at as shop_signup_at, 
    signup_page
    FROM {{ ref('base__shop_conversions') }}
    WHERE multitrack_token in (select distinct multitrack_token from pageview_sessions where page_click)
    AND signup_page like '%/pos%'
), 

unified_funnel_conversions as (
  SELECT 
    p.multitrack_token, 
    MAX(new_sales_ready_at is not null) as retail_mql,
    MAX(shop_signup_at is not null) as free_trial_lead,
    MAX(new_sales_ready_at is not null OR shop_signup_at is not null) as conversion,

    FROM pageview_sessions p 
    LEFT JOIN retail_lead_subs_and_mqls r 
      on p.session_token = r.session_token
      AND p.page_click
      AND r.new_sales_ready_at between p.session_start and TIMESTAMP_ADD(p.session_start, INTERVAL 7 DAY)
    LEFT JOIN shop_signups s 
      on p.multitrack_token = s.multitrack_token
      AND p.page_click
      AND s.shop_signup_at between p.session_start and TIMESTAMP_ADD(p.session_start, INTERVAL 7 DAY)
    GROUP BY 1
)


SELECT DISTINCT
  bua.experiment_handle,
  bua.variant,
  bua.first_assigned_at,
  bua.subject_id,

 CASE WHEN retail_mql then 9996 -- Est. Avg LTV per Retail MQL
    WHEN free_trial_lead then 108 -- Est. Avg LTV per SS lead
    else 0 end as value,

FROM brochure_user_assignments as bua
LEFT OUTER JOIN unified_funnel_conversions AS ufc
  ON bua.subject_id = ufc.tokenized_user_token


