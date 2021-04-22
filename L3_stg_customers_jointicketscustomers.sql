{{ config(materialized='table') }}

with customer_transaction as (
  select 
  * from 
  {{source('STG_TRANSACTION_CUSTOMERS','STG_TRANSACTION_CUSTOMERS')}}
),

ticket_info as(
  select 
  * from {{ref('L2_stg_tickets_data')}}  
),

customer_info as(
  select 
  * from {{ref('L2_stg_customersdata')}}  
),



-- joining L2_stg_customers_ticketdata with stg_transaction_customers
   
final_ticket_info as(
  select 
  T.*, 
  C.master_customer_id
  from 
  customer_transaction C 
  left join 
  ticket_info T
  using (master_transaction_id)
 ) ,


--joining L2_stg_customersdata with final_ticket_info

 final_customer_info as(
  select A.* , 
  T.master_transaction_id, 
  T.ticket_id, 
  T.magento_order_id ,
  T.zip_code ,
  T.date_created,
  T.date_closed,
  T.depot_code ,
  T.sales_channel,
  T.task_successful,
  T.order_status,
  T.discount_total,
  T.order_number,
  T.customer_id ,
  T.Gross_sales ,
  T.Net_sales ,
  T.Gross_receipt , 
  T.NEW_OR_REPEAT_CUSTOMER,
  T.quantity, 
  T.PRODUCT_NAME,
  T.amount,
  T.discount_title

  from customer_info A
  left join final_ticket_info T
  using (master_customer_id)
 ) 

select * from  final_customer_info