{% snapshot mock_orders %}

{% set new_schema = target.schema + '_snapshot' %}

{{
    config(
        target_database = 'dbt_learn_arice'
        , target_schema = new_schema
        , unique_key = 'order_id'
    
        , strategy = 'timestamp'
        , updated_at = 'updated_at'
    )
}}

SELECT * FROM dbt_learn_arice.dbt_learn_arice.mock_orders

{% endsnapshot %}