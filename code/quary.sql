-- Drop fact table if it exists to avoid conflicts when recreating
DROP TABLE IF EXISTS fact_table;

-- Create the fact table with foreign key relationships to dimension tables
CREATE TABLE fact_table (
    fact_id INT PRIMARY KEY,
    time_id INT,
    season_id INT,
    location_id INT,
    crash_type_id INT,
    road_condition_id INT,
    vehicle_id INT,
    driver_id INT,
    population_id INT,
    lga_id INT,
    fatalities INT,

    FOREIGN KEY (time_id) REFERENCES time_dimension(time_id),
    FOREIGN KEY (season_id) REFERENCES season_dimension(season_id),
    FOREIGN KEY (location_id) REFERENCES location_dimension(location_id),
    FOREIGN KEY (crash_type_id) REFERENCES crash_type_dimension(crash_type_id),
    FOREIGN KEY (road_condition_id) REFERENCES road_condition_dimension(road_condition_id),
    FOREIGN KEY (vehicle_id) REFERENCES vehicle_dimension(vehicle_id),
    FOREIGN KEY (driver_id) REFERENCES driver_dimension(driver_id),
    FOREIGN KEY (population_id) REFERENCES population_dimension(population_id),
    FOREIGN KEY (lga_id) REFERENCES lga_dimension(lga_id)
);

-- Analysis of crashes and fatality rates by vehicle type (Bus, Heavy Rigid Truck, Articulated Truck)
-- Calculates total crashes, total fatalities, and fatality rate per 100 crashes
SELECT
  vehicle_type,
  COUNT(*) AS total_crashes,
  SUM(fatalities) AS total_fatalities,
  ROUND(SUM(fatalities) * 100.0 / COUNT(*), 2) AS fatality_rate_per_100
FROM (
  SELECT f.fatalities, 'Bus' AS vehicle_type
  FROM fact_table f
  JOIN vehicle_dimension v ON f.vehicle_id = v.vehicle_id
  WHERE v.bus_involvement = 'Yes'

  UNION ALL

  SELECT f.fatalities, 'Heavy Rigid Truck' AS vehicle_type
  FROM fact_table f
  JOIN vehicle_dimension v ON f.vehicle_id = v.vehicle_id
  WHERE v.heavy_rigid_truck_involvement = 'Yes'

  UNION ALL

  SELECT f.fatalities, 'Articulated Truck' AS vehicle_type
  FROM fact_table f
  JOIN vehicle_dimension v ON f.vehicle_id = v.vehicle_id
  WHERE v.articulated_truck_involvement = 'Yes'
) AS combined
GROUP BY vehicle_type
ORDER BY fatality_rate_per_100 DESC;

-- Analysis of crashes and fatality rates by driver demographics (age group and gender)
-- Shows which demographic groups have higher fatality rates
SELECT 
    d.age_group,
    d.gender,
    COUNT(*) AS total_crashes,
    SUM(f.fatalities) AS total_fatalities,
    ROUND(SUM(f.fatalities) * 100.0 / COUNT(*), 2) AS fatality_rate_per_100
FROM fact_table f
JOIN driver_dimension d ON f.driver_count = d.driver_id  
GROUP BY d.age_group, d.gender
ORDER BY fatality_rate_per_100 DESC;

-- Analysis of crashes and fatality rates by state
-- Identifies which states have the highest fatality rates
SELECT 
    l.state,
    COUNT(*) AS total_crashes,
    SUM(f.fatalities) AS total_fatalities,
    ROUND(SUM(f.fatalities) * 100.0 / COUNT(*), 2) AS fatality_rate_per_100
FROM fact_table f
JOIN location_dimension l 
    ON f.location_id = l.location_id
GROUP BY l.state
ORDER BY fatality_rate_per_100 DESC;

-- Analysis of crashes and fatality rates by time of day and day of week
-- Helps identify the most dangerous times for road travel
SELECT 
    t.time_of_day,
    t.day_of_week,
    COUNT(*) AS total_crashes,
    SUM(f.fatalities) AS total_fatalities,
    ROUND(SUM(f.fatalities) * 100.0 / COUNT(*), 2) AS fatality_rate_per_100
FROM fact_table f
JOIN time_dimension t
    ON f.time_id = t.time_id / 100  
GROUP BY t.time_of_day, t.day_of_week
ORDER BY total_fatalities DESC;

-- Yearly trend analysis of crashes and fatalities
-- Shows how crash numbers and fatalities change over time
SELECT 
    f.year,
    COUNT(DISTINCT f.crash_id) AS total_crashes,
    SUM(f.fatalities) AS total_fatalities
FROM fact_table f
GROUP BY f.year
ORDER BY f.year;

-- Comparison of fatality rates between holiday periods and non-holiday periods
-- Determines if holidays have higher crash and fatality rates
SELECT 
  CASE 
    WHEN t.christmas_period = 'Yes' OR t.easter_period = 'Yes' 
    THEN 'Holiday'
    ELSE 'Non-Holiday'
  END AS holiday_status,
  COUNT(*) AS total_crashes,
  SUM(f.fatalities) AS total_fatalities,
  ROUND(SUM(f.fatalities) * 100.0 / COUNT(*), 2) AS fatality_rate_per_100
FROM fact_table f
JOIN time_dimension t 
  ON CAST(f.time_id AS TEXT) = LEFT(CAST(t.time_id AS TEXT), 6)
GROUP BY holiday_status
ORDER BY total_fatalities DESC;

-- Multi-dimensional analysis combining season, state, age group, and gender
-- Provides a comprehensive view of fatality patterns across multiple factors
SELECT
    s.season AS season,
    l.state AS state,
    d.age_group AS age_group,
    d.gender AS gender,
    COUNT(*) AS total_crashes,
    SUM(f.fatalities) AS total_fatalities,
    ROUND(SUM(f.fatalities) * 100.0 / COUNT(*), 2) AS fatality_rate_per_100
FROM fact_table f
JOIN season_dimension s ON f.season_id = s.season_id
JOIN location_dimension l ON f.location_id = l.location_id
JOIN driver_dimension d ON f.driver_count = d.driver_id  
GROUP BY s.season, l.state, d.age_group, d.gender
ORDER BY fatality_rate_per_100 DESC;

-- Alternative multi-dimensional analysis using clean season dimension
-- Similar to the previous query but uses a cleaned version of the season dimension
SELECT
  s.season AS season,
  l.state AS state,
  d.age_group,
  d.gender,
  COUNT(*) AS total_crashes,
  SUM(f.fatalities) AS total_fatalities,
  ROUND(SUM(f.fatalities) * 100.0 / COUNT(*), 2) AS fatality_rate_per_100
FROM fact_table f
JOIN driver_dimension d ON f.driver_count = d.driver_id
JOIN location_dimension l ON f.location_id = l.location_id
JOIN season_dimension sd ON f.season_id = sd.season_id
JOIN season_dimension_clean s ON LOWER(sd.season) = LOWER(s.season)
GROUP BY s.season, l.state, d.age_group, d.gender
ORDER BY fatality_rate_per_100 DESC;
