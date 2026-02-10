{{
    config(
        unique_key=['order_item_id', 'order_id'],
        incremental_strategy = 'merge',
        tags='orders'
    )
}}
WITH ORDER_ITEMS AS (
    SELECT
        ORDER_ITEM_ID,
        ORDER_ID,
        PRODUCT_ID,
        USER_ID,
        QUANTITY,
        ITEM_PRICE,
        LOADED_AT
    FROM {{ source('ecom', 'order_items') }}
    {% if is_incremental() %}
    WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
    {% endif %}
)
SELECT * FROM ORDER_ITEMS