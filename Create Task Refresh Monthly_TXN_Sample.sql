/*Creates the procedure to run the monthly txn sample code*/
CREATE OR REPLACE PROCEDURE refresh_monthly_txn_sample()
RETURNS VARCHAR
LANGUAGE SQL
AS

$$
DECLARE
  v_start DATE := DATEADD(month, -1, DATE_TRUNC(month, CURRENT_DATE));  -- first day of last month
  v_end   DATE := DATE_TRUNC(month, CURRENT_DATE);                      -- first day of this month
  v_rows  INTEGER;
BEGIN

  INSERT INTO DEV_BRONZE.PARKING.MONTHLY_TXN_SAMPLE
  /* CTE #1 */
  WITH LOT_LOCATION AS (
    SELECT f.*,
           d.CARPARKLOCATION,
           d.DATASOURCENAME,
           CURRENT_DATE AS IMPORT_DATE
    FROM PROD_GOLD.PARKING.DIM_PARKINGATTRIBUTES AS d
    JOIN PROD_GOLD.PARKING.FACT_PARKING AS f
      ON d.PK_PARKINGATTRIBUTES = f.FK_PARKINGATTRIBUTES
  ),

  /* CTE #2 */
  T1VALETPARKINGLOT AS (
    SELECT DISTINCT *
    FROM LOT_LOCATION
    WHERE CARPARKLOCATION = 'Terminal 1 Valet'
      AND DATASOURCENAME = 'chauntry'
      AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') >= v_start
      AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') <  v_end
      AND REVENUE > 0
      AND PLATENO != 'None'
    LIMIT 273
  ),

  /* CTE #3 */
  T2VALETPARKINGLOT AS (
    SELECT DISTINCT *
    FROM LOT_LOCATION
    WHERE CARPARKLOCATION = 'Terminal 2 Valet'
      AND DATASOURCENAME = 'chauntry'
      AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') >= v_start
      AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') <  v_end
      AND REVENUE > 0
      AND PLATENO != 'None'
    LIMIT 265
  ),

  /* CTE #4 */
  TERMINAL1PARKINGLOT AS (
    SELECT DISTINCT *
    FROM LOT_LOCATION
    WHERE CARPARKLOCATION = 'Terminal 1 Parking Plaza'
      AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') >= v_start
      AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') <  v_end
      AND REVENUE > 0
      AND PLATENO != 'None'
    LIMIT 378
  ),

  /* CTE #5 */
  TERMINAL2PARKINGLOT AS (
    SELECT DISTINCT *
    FROM LOT_LOCATION
    WHERE CARPARKLOCATION = 'Terminal 2 Parking Plaza'
      AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') >= v_start
      AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') <  v_end
      AND REVENUE > 0
      AND PLATENO != 'None'
    LIMIT 381
  ),

  /* CTE #6 */
  SAMPLEDATA AS (
    SELECT * FROM T1VALETPARKINGLOT
    UNION
    SELECT * FROM T2VALETPARKINGLOT
    UNION
    SELECT * FROM TERMINAL2PARKINGLOT
    UNION
    SELECT * FROM TERMINAL1PARKINGLOT
  )
  SELECT * FROM SAMPLEDATA;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN 'Inserted ' || v_rows || ' rows for [' || TO_VARCHAR(v_start) || ',' || TO_VARCHAR(v_end) || ')';
END;
$$;

/*Sets up the task to run the stored procedure */
CREATE OR REPLACE TASK monthly_txn_sample_task
  WAREHOUSE = SAN_DEV_SERVICE_ELT_WH
  SCHEDULE  = 'USING CRON 30 6 1 * * America/Los_Angeles'
AS
  CALL DEV_BRONZE.PARKING.refresh_monthly_txn_sample();   -- Calls stored procedure

  -- Turns task on
ALTER TASK monthly_txn_sample_task RESUME;
