WITH default_radius AS(
  SELECT
    delivery_area_id,
    time_from,
    time_to,
    radius_from,
    radius_to,
    time_to-time_from AS duration,
    radius_to-radius_from AS radius_change,
    CASE
      WHEN EXTRACT( HOUR FROM (time_to-time_from)) >= 24
      THEN 'Yes'
      ELSE 'No'
    END AS default_radius
  FROM {{ ref('transformed_radius_log') }}
  ORDER BY delivery_area_id, time_from
)
SELECT * FROM default_radius