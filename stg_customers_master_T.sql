{{ config(materialized='incremental',unique_key = 'email')}}

with 
all_info as(
  select 
    * from {{ref('L3_stg_customers_jointicketscustomers')}}  
),

depot_date_data as(
   select 
    * from {{ref('L6_stg_customers_alldata')}} 
),

final as(
    select 
    
    --ids--
    D.master_customer_id,
    --date related columns--
    D.last_used_zipcode ,
    D.DATE_OF_FIRST_ORDER,
    D.DATE_OF_LAST_ORDER ,
    YEAR(D.DATE_OF_FIRST_ORDER) AS YEAR_OF_FIRST_PURCHASE,
    DAYOFWEEK(D.DATE_OF_FIRST_ORDER) AS WEEK_DAY_OF_FIRST_PURCHASE,
    QUARTER(D.DATE_OF_FIRST_ORDER) AS QUARTER_OF_FIRST_PURCHASE,
    HOUR(D.DATE_OF_FIRST_ORDER) AS HOUR_OF_FIRST_PURCHASE,
    MONTH(D.DATE_OF_FIRST_ORDER) AS FIRST_PURCHASE_COHORT_MONTHLY,
    WEEK(D.DATE_OF_FIRST_ORDER) AS FIRST_PURCHASE_COHORT_WEEKLY ,
    D.acquisition_source , 
    D.last_sales_channel,
    D.depot_of_first_order ,
    --financial related columns--
    D.total_gross_sales,
    D.total_net_sales,
    D.total_gross_receipt,
    D.aov_with_taxes,
    D.average_order_value,
    D.TotalPromosApp,
    D.orders_without_discounts_applied,
    D.Total_units_purchased,
    D.AVG_UNITS_PER_TRANSACTION,
    D.TotalDiscountAmount,
    --delivery related columns--
    D.number_of_failed_deliveries,
    D.number_of_successful_orders,
    D.number_of_cancelled_orders,
    --is_converted--
    D.is_converted,

    --new/repeat customer
    D.NEW_OR_REPEAT_CUSTOMER,

    --#L3_stg_customers_jointicketscustomers
    
    --#--MAGENTO--#
    A.email,
    A.magento_id , 
    A.magento_first_name,
    A.magento_last_name,
    A.magento_date_of_account_creation,
    A.magento_phone,
    A.magento_date_of_birth,
    
    --#--LAX1--#
    A.lax1_id, A.lax1_first_name , 
    A.lax1_last_name , A.lax1_date_of_birth , 
    A.lax1_phone_number, A.lax1_date_of_account_creation, 
    A.lax1_is_banned, A.lax1_type,

    --#--SFO1--#
    A.sfo1_id , A.sfo1_first_name, 
    A.sfo1_last_name, A.sfo1_date_of_birth, A.sfo1_type,
    A.sfo1_phone_number, A.sfo1_date_of_account_creation, A.sfo1_is_banned,


    --#--reference_date--#
    case 
    when A.sfo1_date_of_account_creation >= A.lax1_date_of_account_creation 
    and A.sfo1_date_of_account_creation>= A.magento_date_of_account_creation
    then A.sfo1_date_of_account_creation
    when A.lax1_date_of_account_creation >= A.sfo1_date_of_account_creation 
    and A.lax1_date_of_account_creation>= A.magento_date_of_account_creation
    then A.lax1_date_of_account_creation
    else A.magento_date_of_account_creation
    end
    as reference_date ,
    CURRENT_TIMESTAMP as meta_load_date

    --ETL execution ID
    -- {% if is_incremental() %} 
    -- (select coalesce(max(ETL_EXECUTION_ID),0)+1 from stg_customers_master_T)  as ETL_EXECUTION_ID
    -- {% else %}
    -- 1 as ETL_EXECUTION_ID
    -- {% endif %}

    from depot_date_data D left join all_info A using (master_customer_id)
)

select distinct * from
final

{% if is_incremental() %}

  where reference_date >= (select coalesce(max(reference_date),'2015-04-03 05:02:30.000 -0700') from {{this}})

{% endif %}

