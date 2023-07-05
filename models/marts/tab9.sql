SELECT product_id,
	   product_name,
	   brand,
	   min(price) min_price
FROM {{ ref('tab7') }}
GROUP BY product_id, 
		 product_name,
	     brand