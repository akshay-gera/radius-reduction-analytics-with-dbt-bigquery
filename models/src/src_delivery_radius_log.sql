WITH raw_delivery_radius_log AS(
    SELECT
        delivery_area_id,
        delivery_radius_meters,
        event_started_timestamp
    FROM {{ source('Raw', 'Raw_Delivery_Radius_Log') }}
)
SELECT * FROM raw_delivery_radius_log