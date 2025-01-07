WITH raw_hours as (
    SELECT 
         period_start,
         period_end
    FROM {{ source('Raw', 'Raw_Hours') }}
)
SELECT * FROM raw_hours
