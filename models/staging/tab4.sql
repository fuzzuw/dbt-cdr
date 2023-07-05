SELECT *
FROM {{ source('dbt_cdr', 'src2') }}