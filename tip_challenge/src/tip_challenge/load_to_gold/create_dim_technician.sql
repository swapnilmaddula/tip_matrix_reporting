CREATE OR REPLACE TABLE tip_challenge_dbw.gold.dim_technician AS
SELECT DISTINCT
  TechnicianID,
  TechnicianName
FROM tip_challenge_dbw.silver.workshop_management;
