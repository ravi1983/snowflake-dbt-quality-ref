{{
    config(
        unique_key = ['product_id', 'valid_from'],
        incremental_strategy = 'merge',
        tags='products'
    )
}}
-- Get the data from raw table that has changed since last run.
-- We'll be using ALL versions of the changed data
WITH NEW_DATA AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME,
        UPPER(CATEGORY) AS PRODUCT_CATEGORY,
        BRAND AS PRODUCT_BRAND,
        PRICE,
        FLOOR(RATING) AS RATING,
        LOADED_AT
    FROM {{ source('ecom', 'products') }}
    {% if is_incremental() %}
    WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
    {% endif %}
),
{% if is_incremental() %}
-- For incremental load, merge the data with existing data with new data
EXISTING_CURRENT AS (
    SELECT
        PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY,
        PRODUCT_BRAND, PRICE, RATING, LOADED_AT
    FROM {{ this }}
    WHERE IS_CURRENT = TRUE
    AND PRODUCT_ID IN (SELECT PRODUCT_ID FROM NEW_DATA)
),
UNION_DATA AS (
    SELECT * FROM EXISTING_CURRENT
    UNION ALL
    SELECT * FROM NEW_DATA
),
{% else %}
UNION_DATA AS (SELECT * FROM NEW_DATA),
{% endif %}
-- Adding valid from, to and is_current columns
DATA_WITH_VALID_FROM AS (
    SELECT
        *,
        LOADED_AT AS VALID_FROM,
        LEAD(LOADED_AT) OVER (PARTITION BY PRODUCT_ID ORDER BY LOADED_AT) AS VALID_TO
    FROM UNION_DATA
),
SCD2_FINAL AS (
    SELECT
        *,
        CASE WHEN VALID_TO IS NULL
            THEN TRUE
            ELSE FALSE
        END AS IS_CURRENT
    FROM DATA_WITH_VALID_FROM
)
SELECT
    *
FROM SCD2_FINAL
