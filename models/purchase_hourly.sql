WITH purchase_hourly AS(
    SELECT 
        * 
    FROM {{ ref('src_purchases') }} AS p
    LEFT JOIN {{ ref('src_hours') }} AS h
        ON  p.time_received >= h.period_start AND p.time_received < h.period_end
)
SELECT * FROM purchase_hourly
    