# Define sources for PSQL tables
source:
  table: vehicle_data
  schema: vehicle_data  

source:
  table: trajectory_info
  schema: trajectory_info

# Create a materialized view for enriched data
model: enriched_data
materialized: view

# SQL query to enrich data
{{ config(
  materialized='view'
) }}

SELECT
  vehicle.vehicle_id,
  trajectory.timestamp,
  COUNT(*) AS num_trajectories  -- Count the number of trajectories per vehicle/timestamp
FROM
  {{ ref('vehicle_data') }} AS vehicle  -- Use Jinja to reference sources
INNER JOIN
  {{ ref('trajectory_info') }} AS trajectory ON vehicle.vehicle_id = trajectory.vehicle_id  -- Join on common key
GROUP BY
  vehicle.vehicle_id,
  trajectory.timestamp;

#Data quality checks using dbt tests
test:
  # Ensure no missing vehicle IDs in the enriched view
  missing_values:
    columns: [vehicle_id]

 # Check if num_trajectories is ever 0 (adjust comparison logic as needed)
# Add a comment explaining the rationale behind this check
relationship:
    field: num_trajectories
    comparison: equal_to
    value: 0
