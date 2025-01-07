WITH raw_purchases AS (
    SELECT 
        purchase_id,
        time_received,
        time_delivered,
        end_amount_with_vat_eur,
        dropoff_distance_straight_line_metres,
        delivery_area_id

    FROM {{ source('Raw', 'Raw_Purchases') }}
)
SELECT * FROM raw_purchases
