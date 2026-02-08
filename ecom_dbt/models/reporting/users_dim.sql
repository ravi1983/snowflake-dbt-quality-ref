{{
    config(
        materialized = 'view'
    )
}}

WITH USER_ORDER_TOTALS AS (
    SELECT
        USER_ID,
        SUM(TOTAL_AMOUNT) AS TOTAL_AMOUNT
    FROM {{ ref('orders_staged') }}
    GROUP BY USER_ID
),
TIERED_USERS AS (
    SELECT
        users.*,
        CASE
            WHEN orders.TOTAL_AMOUNT >= 500 THEN 'GOLD'
            WHEN orders.TOTAL_AMOUNT >= 200 THEN 'SILVER'
            WHEN orders.TOTAL_AMOUNT > 0 THEN 'BRONZE'
            ELSE 'NEW MEMBER'
        END AS TIER,
        orders.TOTAL_AMOUNT AS TOTAL_AMOUNT
    FROM {{ ref('users_staged') }} AS users
    LEFT OUTER JOIN USER_ORDER_TOTALS AS orders
        ON users.USER_ID = orders.USER_ID
    WHERE users.VALID_TO IS NULL
)
SELECT * FROM TIERED_USERS