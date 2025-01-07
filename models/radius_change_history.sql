WITH default_radius AS(
  SELECT
    delivery_area_id,
    time_from,
    time_to,
    current_radius,
    last_known_radius,
    time_to-time_from AS duration,
    current_radius-last_known_radius AS radius_change,
    CASE
      WHEN EXTRACT( HOUR FROM (time_to-time_from)) < 24
      THEN 'Yes'
      ELSE 'No'
    END AS is_temporary_change,
    CASE 
      WHEN EXTRACT( HOUR FROM (time_to-time_from)) > 24
      THEN current_radius
      ELSE last_known_radius
    END AS default_radius_value
    
  FROM {{ ref('transformed_radius_log') }}
  ORDER BY delivery_area_id, time_from
)
SELECT * FROM default_radius