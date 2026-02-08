{{
    config(
        unique_key=['order_item_id', 'order_id', 'loaded_at'],
        incremental_strategy = 'merge'
    )
}}
WITH ORDER_ITEMS AS (
    SELECT
        *
    FROM {{ source('ecom', 'order_items') }}
    {% if is_incremental() %}
    WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
    {% endif %}
)
SELECT * FROM ORDER_ITEMS