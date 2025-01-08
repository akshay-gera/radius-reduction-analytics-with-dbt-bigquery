WITH default_radii AS(
  SELECT 
    delivery_area_id,
    time_from AS event_started_timestamp,
    LEAD(time_from) OVER(PARTITION BY delivery_area_id ORDER BY time_from) AS event_ended_timstamp,
    default_radius_value AS default_radius
  FROM {{ ref('radius_change_history') }}
  WHERE is_temporary_change='No'
  ORDER BY delivery_area_id,time_from
)
SELECT * FROM default_radii 