{{
    config(materialized='incremental',unique_key = 'MASTER_ITEM_ID')
}}

with final as  
(
    select 
    --tickets_columns
        T.DEPOT_CODE,
        T.TICKET_ID,
        T.DATE_CREATED AS TRANSACTION_CREATED_DATE,
        T.DATE_CLOSED AS TRANSACTION_DATE_CLOSED,
        MONTH(T.DATE_CREATED) AS MONTH_OF_TRANSACTION,
        YEAR(T.DATE_CREATED) AS YEAR_OF_TRANSACTION,
        DAYOFWEEK(T.DATE_CREATED) AS WEEK_DAY_OF_TRANSACTION,
        QUARTER(T.DATE_CREATED) AS QUARTER_OF_TRANSACTION,
        HOUR(T.DATE_CREATED) AS HOUR_OF_TRANSACTION,
        T.INVENTORY_LOCATION_NAME,
        T.PRODUCT_BRAND,
        T.PRODUCT_ID,
        T.PRODUCT_SIZE_NAME,
        T.PRODUCT_TYPE,
        T.PRODUCT_UNIT,
        T.SELLING_PRICE,
        T.EXCISE_TAX_RATE,
        T.LOCAL_TAX_RATE,
        T.SALES_TAX_RATE,
        T.EXCISE_TAX_AMOUNT,
        T.LOCAL_TAX_AMOUNT,
        T.SALES_TAX_AMOUNT,
        T.PRICE_TOTAL,
        T.QUANTITY,
        DAYOFMONTH(TRANSACTION_CREATED_DATE) as DAY_OF_MONTH,
    --ALL_PRODUCTS     
        P.SUBTYPE as PRODUCT_SUBTYPE,
        P.classification as PRODUCT_CLASSIFICATION,
        P.product_type as PRODUCT_CATEGORY_TYPE,
        P.NAME as PRODUCT_NAME,
    --inventory    
        I.STATE_TRACKING_ID,
        I.PRODUCT_SKU,
    --invoices    
        IV.WHOLESALE_COST,
        IV.MARGIN,
    --TRANSACTIONS_MASTER    
        TM.TRANSACTION_ID as TRANSACTION_ID,
    --etl details
        (select 
            ifnull((select max(DATE_CREATED)::timestamp_ltz 
        from 
            {{ref('L2_stg_items_tickets_columns')}} ),current_timestamp)) as META_LOAD_DATE,
        {% if is_incremental() %}
            (SELECT MAX(ETL_EXECUTION_ID) FROM transaction_items_master)+1 AS ETL_EXECUTION_ID
        {% else %}
		    1 AS ETL_EXECUTION_ID
        {% endif %}    
    from 
        {{ref('L4_stg_items_tickets_tax_columns')}} T
    left join 
        {{ref('L1_STG_ITEMS_ALL_PRODUCTS')}} P
        on T.PRODUCT_ID=P.PRODUCT_ID and T.DEPOT_CODE=p.DEPOT_CODE
    left join 
        {{ref('L1_stg_items_inventory')}} as I
        on T.PRODUCT_ID=I.PRODUCT_ID and T.DEPOT_CODE=I.DEPOT_CODE
    left join 
        {{ref('L1_stg_items_invoices')}} as IV
        on IV.PRODUCT_ID=T.PRODUCT_ID
        --and T.DEPOT_CODE=IV.DEPOT_CODE 
     left join 
        "AMUSE_REPORTING"."REPORTING"."TRANSACTIONS_MASTER" TM 
    on T.TICKET_ID=TM.TREEZ_TICKET_ID
)

select 
    --concat(PRODUCT_ID,TICKET_ID,DEPOT_CODE) as NEW_KEY, * 
    {{dbt_utils.surrogate_key(['PRODUCT_ID','TICKET_ID','DEPOT_CODE'])}} as MASTER_ITEM_ID,*
from 
    final

{% if is_incremental() %}

  where TRANSACTION_DATE_CLOSED >= (select max(TRANSACTION_DATE_CLOSED) from TRANSACTION_ITEMS_MASTER)

{% endif %}
    
