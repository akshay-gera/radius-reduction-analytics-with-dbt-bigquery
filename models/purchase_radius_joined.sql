{{ config(
    materialized='view'
) }}

WITH purchase_radius_joined AS(
    SELECT 
        hh.period_start,
        hh.period_end,
        p.delivery_area_id,
        p.purchase_id,
        p.time_received,
        p.time_delivered,
        p.end_amount_with_vat_eur,
        p.dropoff_distance_straight_line_metres,
        h.time_from,
        h.time_to,
        h.current_radius,
        h.last_known_radius,
        h.duration,
        h.radius_change,
        h.is_temporary_change,
        h.default_radius_value
        
    FROM {{ ref('src_purchases') }} AS p
    LEFT JOIN 
        {{ ref('radius_change_history') }} AS h
        ON p.delivery_area_id = h.delivery_area_id AND p.time_received >= h.time_from AND p.time_received < h.time_to

    LEFT JOIN 
        {{ ref('src_hours') }} AS hh
        ON p.time_received >= hh.period_start AND p.time_received < hh.period_end

    ORDER BY p.delivery_area_id, time_received
)
SELECT * FROM purchase_radius_joined
