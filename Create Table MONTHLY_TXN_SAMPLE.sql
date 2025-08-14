/*The purpose of this table is to create an empty table where a new sample of 1000 cars will be appended each month to eventually create an average vehicle value by parking lot by month snapshot. */

--create or replace table dev_bronze.parking.MONTHLY_TXN_SAMPLE AS(

--Insert into dev_bronze.parking.monthly_txn_sample(
/*CTE #1 Joins the FACT_PARKING transaction table to get parking lot location column and an import date column*/
WITH LOT_LOCATION AS(
SELECT  f.*,d.CARPARKLOCATION,d.DATASOURCENAME, DATE '2025-06-15'   AS IMPORT_DATE -- Testing to make sure the date feature works 
--SELECT f.*, d.CARPARKLOCATION, d.datasourcename, CURRENT_DATE AS IMPORT_DATE
FROM PROD_GOLD.PARKING.DIM_PARKINGATTRIBUTES AS d 
JOIN PROD_GOLD.PARKING.FACT_PARKING as f
on d.pk_parkingattributes = f.fk_parkingattributes
),

/*CTE #2 that grabs license plates from Valet Lots*/
VALETPARKINGLOT AS(
SELECT DISTINCT*
FROM LOT_LOCATION
WHERE CARPARKLOCATION IN ('Terminal 1 Valet','Terminal 2 Valet')
AND DATASOURCENAME = 'chauntry'
AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') >= DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))  
AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') <  DATE_TRUNC('month', CURRENT_DATE)        
--AND TO_DATE(fk_entrydate::VARCHAR,'YYYYMMDD') >= DATE_TRUNC('month', CURRENT_DATE) -- Grabs current month dates               
--AND TO_DATE(fk_entrydate::VARCHAR,'YYYYMMDD') < DATEADD('month', 1, DATE_TRUNC('month', CURRENT_DATE)) -- Grabs Dates for Current Month 
LIMIT 333
),

/*CTE #3 that grabs license plates from Terminal 1 Parking Plaza*/
TERMINAL1PARKINGLOT AS(
SELECT DISTINCT* 
FROM LOT_LOCATION
WHERE CARPARKLOCATION = 'Terminal 1 Parking Plaza'
AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') >= DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)) -- Grabs previous month dates
AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') <  DATE_TRUNC('month', CURRENT_DATE)             
--AND TO_DATE(fk_entrydate::VARCHAR,'YYYYMMDD') >= DATE_TRUNC('month', CURRENT_DATE) -- Grabs current month dates                  
-- AND TO_DATE(fk_entrydate::VARCHAR,'YYYYMMDD') < DATEADD('month', 1, DATE_TRUNC('month', CURRENT_DATE)) -- Grabs current month dates   
LIMIT 333
),

/*CTE #4 that grabs license plates from Terminal 2 Parking Plaza*/
TERMINAL2PARKINGLOT AS(
SELECT DISTINCT*
FROM LOT_LOCATION
WHERE CARPARKLOCATION = 'Terminal 2 Parking Plaza'
AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') >= DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)) -- Grabs previous month dates
AND TO_DATE(fk_entrydate::VARCHAR, 'YYYYMMDD') <  DATE_TRUNC('month', CURRENT_DATE)
--AND TO_DATE(fk_entrydate::VARCHAR,'YYYYMMDD') >= DATE_TRUNC('month', CURRENT_DATE) -- Grabs current month dates                   
--AND TO_DATE(fk_entrydate::VARCHAR,'YYYYMMDD') < DATEADD('month', 1, DATE_TRUNC('month', CURRENT_DATE)) -- Grabs current month dates   
LIMIT 334
),

/*CTE #5 that union's all 3 parking lot CTE's to create an even sample data of license plates from all 3 lots*/
SAMPLEDATA AS(
(
SELECT *
FROM VALETPARKINGLOT
)
UNION 
(
SELECT *
FROM TERMINAL2PARKINGLOT
)
UNION
(
SELECT *
FROM TERMINAL1PARKINGLOT
)
)

SELECT *
FROM SAMPLEDATA
LIMIT 0
)