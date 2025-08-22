CREATE OR REPLACE TABLE tip_challenge_dbw.gold.fact_repair_job AS
WITH foreign_key_created AS (
  SELECT
    *,
    concat('WSM_', Work_Pack_Nr, '_', Work_Order_Nr, '_', Work_Order_Task_Nr) AS foreign_key
  FROM tip_challenge_dbw.silver.wot_log
),
joined_table AS (
  SELECT
    
    a.CompanyAddress,
    a.CompanyName,
    a.CustomerNr,
    a.MachineMake,
    a.MachineModel,
    a.TechnicianID,
    a.UnitNr,
    a.VehicleClassificationID,
    a.WSM_Key,
    a.WorkOrderTaskCreateDateTime,
    a.WorkPackCreateDateTime,
    a.WorkTypeGroup,
    a.WorkTypeID,
    a.WorkedTimeInHours,
    a.primary_key,
    c.TechnicianName,
    CASE
      WHEN b.StatusFromDescription = 'Done'
       AND b.StatusToDescription   = 'Start'
      THEN (unix_timestamp(a.WorkOrderTaskCreateDateTime) - unix_timestamp(b.Changed_Date)) / 60.0
      ELSE 0
    END AS TotalWorkInProgressTimeMinutes
  FROM tip_challenge_dbw.silver.workshop_management a
  LEFT JOIN foreign_key_created b
    ON a.WSM_Key = b.foreign_key
  LEFT JOIN tip_challenge_dbw.gold.dim_technician c
    ON a.TechnicianID = c.TechnicianID
)
SELECT *
FROM joined_table;
