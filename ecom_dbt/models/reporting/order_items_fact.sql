WITH ORDER_ITEMS_DATE AS (
    SELECT
        orders.ORDER_DATE,
        orders.ORDER_STATUS,
        items.*
    FROM {{ ref('order_items_staged') }} items
    LEFT OUTER JOIN {{ ref('orders_staged') }} orders
        ON items.ORDER_ID = orders.ORDER_ID
)
SELECT * FROM ORDER_ITEMS_DATE