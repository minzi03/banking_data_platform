-- =============================================================================
-- Generic Test: Accepted Range
-- =============================================================================
-- Usage in schema.yml:
--   columns:
--     - name: score
--       tests:
--         - accepted_range:
--             min_value: 0
--             max_value: 100
-- =============================================================================

{% test accepted_range(model, column_name, min_value, max_value) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < {{ min_value }}
   OR {{ column_name }} > {{ max_value }}

{% endtest %}
