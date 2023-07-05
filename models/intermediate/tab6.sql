SELECT tab3.id_row,
    tab3.location,
    tab3.area,
    tab3.number_of_cows,
    tab3.farm_size,
    tab3.date,
    tab4.product_id,
    tab4.product_name,
    tab3.quantity,
    tab4.brand,
    tab4.price,
    round(tab3.quantity * tab4.price, 2) as total
FROM {{ ref('tab3') }}
JOIN {{ ref('tab4') }} USING (id_row)