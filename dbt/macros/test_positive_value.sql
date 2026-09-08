-- =============================================================================
-- Generic Test: Positive Value
-- =============================================================================
-- Usage in schema.yml:
--   columns:
--     - name: amount
--       tests:
--         - positive_value
-- =============================================================================

{% test positive_value(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
