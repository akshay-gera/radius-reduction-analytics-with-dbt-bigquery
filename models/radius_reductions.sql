WITH radius_reductions AS(
    SELECT
    r.delivery_area_id,
    r.time_from,
    r.time_to,
    d.default_radius,
    r.current_radius,
    r.radius_change,
    r.duration
    
    FROM {{ ref('radius_change_history') }} AS r
    LEFT JOIN {{ ref('default_radii') }} AS d
    ON r.delivery_area_id = d.delivery_area_id AND time_from >= event_started_timestamp AND time_to <= event_ended_timstamp 
    WHERE current_radius <> default_radius
)
SELECT * FROM radius_reductions