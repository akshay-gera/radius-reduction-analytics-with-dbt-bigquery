WITH hourly_purchases_and_radius_reductions AS(
    SELECT 
        period_start, 
        delivery_area_id, 
        MAX(default_radius) AS default_delivery_radius, 
        ROUND(SUM(end_amount_with_vat_eur), 2) AS revenue, 
        COUNT(DISTINCT(purchase_id)) AS purchase_count, 
        SUM(CASE WHEN change='reduced' THEN duration ELSE INTERVAL 0 SECOND END) AS reduction_duration, 
        SUM(CASE WHEN change='reduced' THEN 1 ELSE 0 END) AS reduction_count  
    FROM {{ ref('purchase_radius_joined') }}
    GROUP BY period_start, delivery_area_id
)
SELECT * FROM hourly_purchases_and_radius_reductions
ORDER BY delivery_area_id, period_start