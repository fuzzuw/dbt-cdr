SELECT *
FROM {{ ref('tab3') }}
WHERE area > 500