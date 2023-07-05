SELECT *
FROM {{ ref('tab4') }} t4
WHERE (select count(distinct product_id)
	   from {{ ref('tab4') }} t44
	   where t44.brand = t4.brand) >= 5