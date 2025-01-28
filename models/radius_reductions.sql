WITH radius_comparison AS(
  SELECT 
      r.delivery_area_id,
      time_from,
      time_to,
      current_radius,
      duration,
      dr.default_radius, 
      CASE 
        WHEN current_radius - dr.default_radius < 0 THEN 'reduced'
        WHEN current_radius - dr.default_radius = 0 THEN 'no change'
        ELSE 'increased' 
      END  AS change
  FROM {{ref('radius_change_history')}} AS r
  LEFT JOIN {{ ref('default_radii') }} dr
    ON r.delivery_area_id = dr.delivery_area_id AND time_from >= event_started_timestamp AND time_from< event_ended_timstamp
  ORDER BY r.delivery_area_id, time_from

)
SELECT * FROM radius_comparison
WHERE change = 'reduced'  