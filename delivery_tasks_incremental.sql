{{
    config(
        materialized = 'incremental',
        unique_key= 'ONFLEET_TASK_ID'
    )
}}

{% set now = modules.datetime.datetime.now() %}

select 
--delivery task details
	DTD.ONFLEET_TASKID as ONFLEET_TASK_ID,
    DTD.SOURCE_TASK_ID,
	DTD.TASK_STATUS,
    DTD.TASK_SUCCESSFUL,
	DTD.TOTAL_DISTANCE_TRAVELLED,
	DTD.TASK_DURATION_HOURS,
	DTD.TASK_DURATION_MINUTES,
	DTD.TIME_DELAYED,
	DTD.SERVICE_TIME,
    DTD.LOCATION,
	DTD.FIRST_LOCATION,
	DTD.LAST_LOCATION,
--customer_details
	CD.CUSTOMER_ID,
	CD.STREET_NUMBER,
	CD.STREET,
	CD.APARTMENT,
	CD.CITY,
	CD.STATE,
	CD.POSTAL_CODE,
--worker and hub details
	WHD.ONFLEET_WORKER_ID,
	WHD.DRIVER_NAME,
    WHD.ONFLEET_HUB_ID,
    WHD.DEPOT_CODE,
--transaction details
    TD.master_transaction_id,
    TD.TREEZ_ORDER_NUMBER,
	TD.TREEZ_TICKET_ID,
	TD.SALES_CHANNEL,
--delivery task and related notes
	DND.DESTINATION_NOTES,
	DND.DELIVERY_NOTES,
    DND.FAILURE_REASON,
	DND.FAILURE_NOTES,
	DND.COMPLETION_NOTES,
    DND.RECIPIENT_NOTES,
--task timestamps 
	TT.TASK_TIME_CREATED,
	TT.TASK_TIME_LAST_MODIFIED,
	TT.TASK_COMPLETE_BEFORE,
	TT.TASK_COMPLETE_AFTER,
    TT.DATE_TASK_COMPLETED,
	TT.DATETIME_TASK_STARTED,
	TT.DATETIME_TASK_ARRIVED,
	TT.DATETIME_TASK_DEPARTED
	,MONTH(TT.DATE_TASK_COMPLETED)  as MONTH_OF_COMPLETION    
	,YEAR(TT.DATE_TASK_COMPLETED)  as YEAR_OF_COMPLETION    
	,DAYOFWEEK(TT.DATE_TASK_COMPLETED)  as WEEK_DAY_OF_COMPLETION
	,QUARTER(TT.DATE_TASK_COMPLETED)  as QUARTER_OF_COMPLETION 
	,HOUR(TT.DATE_TASK_COMPLETED)  as HOUR_OF_COMPLETION     ,
--task requirement details
	TRD.REQUIREMENTS_MINIMUM_AGE,
	TRD.REQUIREMENTS_NOTES,
	TRD.REQUIREMENTS_PHOTO,
	TRD.REQUIREMENTS_SIGNATURE,
--etl details
	'{{now}}' as META_LOAD_DATE,
	{% if is_incremental() %}
		(SELECT MAX(ETL_EXECUTION_ID) FROM {{this}})+1 AS ETL_EXECUTION_ID
    {% else %}
		1 AS ETL_EXECUTION_ID
    {% endif %}
from {{ref('L2_stg_onfleet_task_details')}} DTD 
left join {{ref('L3_stg_onfleet_customer_details')}} CD on DTD.ONFLEET_TASKID=CD.ONFLEET_TASKID
left join {{ref('L5_stg_onfleet_worker_hub_details')}} WHD on DTD.ONFLEET_TASKID=WHD.ONFLEET_TASKID
left join {{ref('L4_stg_onfleet_transaction_details')}} TD on DTD.ONFLEET_TASKID=TD.ONFLEET_TASKID
left join {{ref('L3_stg_onfleet_notes')}} DND on DTD.ONFLEET_TASKID=DND.ONFLEET_TASKID
left join {{ref('L2_stg_onfleet_timestamps')}} TT on DTD.ONFLEET_TASKID=TT.ONFLEET_TASKID
left join {{ref('L2_stg_onfleet_requirement_details')}} TRD on DTD.ONFLEET_TASKID=TRD.ONFLEET_TASKID

{% if is_incremental() %}
where 
TT.TASK_TIME_LAST_MODIFIED>(select max(TASK_TIME_LAST_MODIFIED) from {{this}})
{% endif %}

