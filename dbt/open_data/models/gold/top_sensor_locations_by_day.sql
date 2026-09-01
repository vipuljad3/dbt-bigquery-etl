with all_months as (select c.location_id
                  , l.sensor_description
                  , EXTRACT(MONTH FROM cast(c.sensing_date as date)) AS month -- strftime('%m', sensing_date) AS month
                  , EXTRACT(DAY FROM cast(c.sensing_date as date))AS day
                  , sum(c.direction_1 + c.direction_2) as total_of_count from {{ ref('monthly_counts_per_hour') }} c
          left join {{ ref('sensor_locations') }} l
          on c.location_id = l.location_id
          group by 1,2,3,4
          order by 3,4,5 desc)
          select month,day, location_id, sensor_description, max(total_of_count) as max_count from all_months 
          group by 1,2,3,4
          order by max_count desc