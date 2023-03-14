{% macro create_stg_stitch_sfmc_uusa_email_campaign_dates(
    reference_name='stg_src_stitch_email_job',
    bbcrm = 'stg_src_stitch_sfmc_bbcrm_transactions',
    fru = 'stg_src_stitch_sfmc_recent_transactions'
    ) %}

    WITH current_fy as (
    SELECT
        __BBCRMLOOKUPID_
        ,CONSTITUENTSYSTEMRECORDID
        ,SAFE_CAST(STATUSCODE AS INT64) STATUSCODE
        ,SAFE_CAST(RECORDID AS STRING) AS RECORDID
        ,REVENUE_ID
        ,SAFE_CAST(TRANSACTION_DATE AS STRING) AS TRANSACTION_DATE
        ,PAYMENT_METHOD
        ,SAFE_CAST(RECOGNITION_AMOUNT AS STRING) AS RECOGNITION_AMOUNT
        ,INBOUND_CHANNEL
        ,APPEAL
        ,APPEAL_BUSINESS_UNIT
        ,INITIAL_MARKET_SOURCE
        ,WEB_DONATION_FORM_VALUE
        ,WEB_DONATION_FORM_COMMENT
        ,DESIGNATION
        ,TRANSACTION_TYPE
        ,APPLICATION
        ,VENDOR_ORDER_NUMBER
        ,REVENUE_PLATFORM
        ,SFMC_DateAdded
        ,SFMC_UpdateDate
    FROM (SELECT *,
                row_number() over (
                    partition by REVENUE_ID order by SFMC_UpdateDate desc) as row_num
                    from bsd-arc-uusa.src_stitch_bbcrm_authorized.revenue
                ) as revenue_rows
        WHERE row_num = 1
)

, fru as (
            SELECT lookup_id                        as BBCRMLOOKUPID
               , CAST(null as STRING)             as CONSTITUENTSYSTEMRECORDID
               , CAST(null AS INT64)              as STATUSCODE
               , CAST(null as STRING)             as RECORDID
               , revenue_id                       as REVENUE_ID
               , CAST(transaction_date as STRING) AS TRANSACTION_DATE
               , CAST(null as STRING)             as PAYMENT_METHOD
               , CAST(amount AS STRING)           as RECOGNITION_AMOUNT
               , CAST(null as STRING)             as INBOUND_CHANNEL
               , appeal                           as APPEAL
               , CAST(null as STRING)             as APPEAL_BUSINESS_UNIT
               , initial_market_source            as INITIAL_MARKET_SOURCE
               , CAST(null as STRING)             as WEB_DONATION_FORM_VALUE
               , CAST(null as STRING)             as WEB_DONATION_FORM_COMMENT
               , designation                      as DESIGNATION
               , CAST(null as STRING)             as TRANSACTION_TYPE
               , (case
                      when gift_type = 'single' then 'Donation'
                      when gift_type = 'monthly' then 'Recurring gift'
                      else null
        end)                                      as APPLICATION
               , CAST(null as STRING)             as VENDOR_ORDER_NUMBER
               , 'Fundraise Up - Direct'          as REVENUE_PLATFORM
               , CAST(sfmc_insert_dt as STRING)   as SFMC_DateAdded
               , CAST(sfmc_updated_dt as STRING)  as SFMC_UpdateDate
          FROM bsd-arc-uusa.src_stitch_fundraiseup_authorized.recent_transactions
), incremental_fru as (
    select *
    from fru
    where CAST(TRANSACTION_DATE AS DATETIME) > (SELECT MAX(CAST(TRANSACTION_DATE AS DATETIME)) FROM current_fy)
), revenue as (

    SELECT * FROM current_fy

    UNION ALL

    SELECT * FROM incremental_fru
)

SELECT
BBCRMLOOKUPID as lookup_id
,CONSTITUENTSYSTEMRECORDID as cons_system_record_id
,CAST(STATUSCODE AS INT64) as status_code
,RECORDID as record_id
,REVENUE_ID as revenue_id
,CAST(TRANSACTION_DATE AS DATETIME) as transaction_datetime
,CAST(SUBSTR(LTRIM(TRANSACTION_DATE),1,10) AS DATE) as transaction_date
,PAYMENT_METHOD as payment_method
,CAST(RECOGNITION_AMOUNT AS FLOAT64) as amount
,INBOUND_CHANNEL as inbound_channel
,APPEAL as appeal
,APPEAL_BUSINESS_UNIT as appeal_business_unit
,INITIAL_MARKET_SOURCE as initial_market_source
,WEB_DONATION_FORM_VALUE as web_donation_form_value
,WEB_DONATION_FORM_COMMENT as web_donation_form_comment
,DESIGNATION as designation
,TRANSACTION_TYPE as transaction_type
,APPLICATION as application
,VENDOR_ORDER_NUMBER as vendor_order_number
,REVENUE_PLATFORM as revenue_platform
,CAST(SUBSTR(LTRIM(SFMC_DateAdded),1,10) AS DATE) sfmc_added_dt
,CAST(SUBSTR(LTRIM(SFMC_UpdateDate),1,10) AS DATE) sfmc_updated_dt
FROM (SELECT *,
        row_number() over (
            partition by revenue_id order by CAST(TRANSACTION_DATE AS DATETIME) desc) as row_num
            from revenue
        ) as revenue_rows
WHERE row_num = 1

{% endmacro %}