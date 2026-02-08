{{
    config(
        unique_key=['user_id', 'valid_from'],
        incremental_strategy = 'merge',
        tags='users'
    )
}}
-- Get ONLY the latest version of modified users since last run
WITH NEW_DATA AS (
    SELECT
        USER_ID,
        SUBSTRING(NAME, 1, CHARINDEX(' ', NAME) - 1) AS FIRST_NAME,
        UPPER(SUBSTRING(NAME, CHARINDEX(' ', NAME) + 1, LEN(NAME))) AS LAST_NAME,
        EMAIL,
        CASE
            WHEN GENDER = 'Male' THEN 'M'
            WHEN GENDER = 'Female' THEN 'F'
            ELSE 'U'
        END AS GENDER,
        CITY,
        SIGNUP_DATE,
        LOADED_AT
    FROM {{ source('ecom', 'users') }}
    {% if is_incremental() %}
    WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY LOADED_AT DESC) = 1
),
{% if is_incremental() %}
-- For incremental load, merge the data with existing data with new data
EXISTING_CURRENT AS (
    SELECT
        USER_ID, FIRST_NAME, LAST_NAME,
        EMAIL, GENDER, CITY, SIGNUP_DATE, LOADED_AT
    FROM {{ this }}
    WHERE IS_CURRENT = TRUE
    AND USER_ID IN (SELECT USER_ID FROM NEW_DATA)
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
        LEAD(LOADED_AT) OVER (PARTITION BY USER_ID ORDER BY LOADED_AT) AS VALID_TO
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