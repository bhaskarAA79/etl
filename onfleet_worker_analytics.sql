{{
    config(materialized = 'incremental',unique_key = 'WORKER_ANALYTICS_ID')
}}
{% set now = modules.datetime.datetime.now() %}
select
    {{dbt_utils.surrogate_key(['DRIVER_ID', 'DATE'])}} as WORKER_ANALYTICS_ID,
--worker analytics
    DRIVER_ID,
    DRIVER_NAME,
    DATE,
    DISTANCE_ENROUTE,
    IDLE,  
    ONDUTY,   
    OFFDUTY,  
    TASKS_FAILED,
    TASKS_SUCCEDDED,
    TIME_ENROUTE,
    TIME_IDLE,
--etl details    
    {% if is_incremental() %}
      (SELECT MAX(ETL_EXECUTION_ID) FROM onfleet_worker_analytics)+1 AS ETL_EXECUTION_ID
    {% else %}
		  1 AS ETL_EXECUTION_ID
    {% endif %} ,
    '{{now}}' as META_LOAD_DATE
from 
    {{ref('L1_stg_worker_analytics_column_flatten')}}

{% if is_incremental() %}

  where DATE > (select max(DATE) from onfleet_worker_analytics)

{% endif %}



    
