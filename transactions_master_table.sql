{{
    config(
        materialized = 'table'
    )
}}


select
	a.master_transaction_id,
	a.depot_code,
	a.magento_order_id,
	a.ticket_id as treez_ticket_id,
	a.order_number as treez_order_number,
	order_status as treez_order_status,
	ticket_note,
	--customer information
	b.master_customer_id,
	is_first_order,
	is_first_order_amuse,
	is_first_order_houseplant,
	is_first_order_weedmaps,
	is_first_order_d2c,
	--financial information
	gross_sale,
	net_sales,
	gross_receipt,
	gross_revenue,
	gross_revenue_ex_fees, 
	gross_customer_fee,
	gross_product_revenue,
	customer_fees_discount,
	total_net_revenue,
	product_discounts,
	net_product_revenue,
	net_fee_revenue,
	total_discounts,
	c.tax_total,
	--order information
	inventory_location,
	d.sales_channel,
	order_placed_date_utc,
	order_completed_date_utc,
	order_placed_date_pst,
	order_completed_date_pst,
	payment_method
from
{{ref('stg_all_tickets')}} as a
left join
{{ref('stg_transaction_customers')}} as b
on a.master_transaction_id = b.master_transaction_id
left join
{{ref('stg_treez_financial_information')}} as c
on a.master_transaction_id = c.master_transaction_id
left join
{{ref('stg_treez_order_information')}} as d
on a.master_transaction_id = d.master_transaction_id
left join 
{{ref('stg_is_first_order')}} as e
on a.master_transaction_id = e.master_transaction_id



