WITH default_radii AS(
    SELECT
        delivery_area_id,
        time_from AS event_started_timestamp,
        time_to AS event_ended_timestamp,
        current_radius AS default_radius
    FROM {{ ref('radius_change_history') }}

)
SELECT * FROM default_radii
