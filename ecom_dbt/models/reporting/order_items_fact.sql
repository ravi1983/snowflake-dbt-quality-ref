{{
    config(
        unique_key = ['ORDER_ID', 'ORDER_ITEM_ID'],
        incremental_strategy = 'merge',
        tags='orders'
    )
}}

WITH ORDER_ITEMS_DATE AS (
    SELECT
        orders.ORDER_DATE,
        orders.ORDER_STATUS,
        items.*,
        (items.ITEM_PRICE * items.QUANTITY) AS ITEM_TOTAL
    FROM {{ ref('order_items_staged') }} items
    LEFT OUTER JOIN {{ ref('orders_staged') }} orders
        ON items.ORDER_ID = orders.ORDER_ID

    {% if is_incremental() %}
    WHERE items.LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
    {% endif %}
)
SELECT * FROM ORDER_ITEMS_DATE