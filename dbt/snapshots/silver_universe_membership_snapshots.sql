{% snapshot silver_universe_membership %}

{{
    config(
      target_database='warehouse',
      target_schema='main',
      unique_key='Ticker',
      strategy='check',
      check_cols='all'
    )
}}


select * 
from {{ ref('bronze_index_membership') }}

{% endsnapshot %}
