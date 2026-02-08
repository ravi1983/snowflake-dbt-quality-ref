{{
    config(
        unique_key=['order_id'],
        incremental_strategy = 'merge',
        tags='orders'
    )
}}
WITH ORDERS AS (
    SELECT
        ORDER_ID,
        USER_ID,
        ORDER_DATE,
        UPPER(ORDER_STATUS) AS ORDER_STATUS,
        TOTAL_AMOUNT,
        LOADED_AT
    FROM {{ source('ecom', 'orders') }}
    {% if is_incremental() %}
    WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
    {% endif %}
)
SELECT * FROM ORDERS