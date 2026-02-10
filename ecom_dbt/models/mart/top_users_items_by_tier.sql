{{
    config(
        tags = 'orders'
    )
}}

WITH ORDERS_GROUPED AS (
    SELECT
        USER_ID,
        ORDER_ID
    FROM {{ ref('order_items_fact') }}
    GROUP BY USER_ID, ORDER_ID
),
RANKED_USERS AS (
    SELECT
        orders.*,
        users.TIER,
        users.TOTAL_AMOUNT,
        DENSE_RANK() OVER(PARTITION BY TIER ORDER BY TOTAL_AMOUNT DESC) AS RANKED
    FROM ORDERS_GROUPED orders
    LEFT OUTER JOIN {{ ref('users_dim') }} users ON users.USER_ID = orders.USER_ID
),
RANKED_USERS_WITH_ITEMS AS (
    SELECT
        ranked.*,
        facts.PRODUCT_ID
    FROM RANKED_USERS ranked
    INNER JOIN {{ ref('order_items_fact') }} facts ON facts.ORDER_ID = ranked.ORDER_ID
    WHERE RANKED <= 5
)
SELECT *
FROM
    RANKED_USERS_WITH_ITEMS
