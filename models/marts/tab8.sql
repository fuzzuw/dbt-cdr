SELECT tab6.*
FROM {{ ref('tab6') }} as tab6
JOIN {{ ref('tab5') }} USING (id_row)