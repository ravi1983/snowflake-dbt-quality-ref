{% test expect_column_value_by_tier(model, column_name, tier_column, tier_value, min_val=none, max_val=none) %}

WITH VALIDATION_DATA AS (
    SELECT
        {{ tier_column }} AS TIER_COL,
        {{ column_name }} AS VALUE_COL
    FROM {{ model }}
    WHERE {{ tier_column }} = '{{ tier_value }}'
),

ERRORS AS (
    SELECT *
    FROM VALIDATION_DATA
    WHERE
        {% if min_val is not none and max_val is not none %}
            NOT (VALUE_COL >= {{ min_val }} AND VALUE_COL <= {{ max_val }})
        {% elif min_val is not none %}
            NOT (VALUE_COL >= {{ min_val }})
        {% elif max_val is not none %}
            NOT (VALUE_COL <= {{ max_val }})
        {% endif %}
)

SELECT * FROM ERRORS

{% endtest %}