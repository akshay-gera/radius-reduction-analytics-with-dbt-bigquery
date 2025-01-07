WITH radius_change_history AS(
    SELECT 
        delivery_area_id,
        event_started_timestamp AS time_from,
        COALESCE(LEAD(event_started_timestamp) OVER(PARTITION BY delivery_area_id ORDER BY event_started_timestamp), event_started_timestamp) AS time_to,
        delivery_radius_meters AS current_radius,
        COALESCE(LAG(delivery_radius_meters) OVER(PARTITION BY delivery_area_id ORDER BY event_started_timestamp), delivery_radius_meters) AS last_known_radius

    FROM {{ ref('src_delivery_radius_log') }}
)

SELECT * FROM radius_change_history