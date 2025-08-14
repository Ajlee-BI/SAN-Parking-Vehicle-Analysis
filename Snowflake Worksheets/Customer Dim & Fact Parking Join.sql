/* ------------------------- Table join adding license plate numbers from FACT_PARKING to DIM_PARKINGCUSTOMER ------------------------------------- */
WITH CUSTOMER_DIM_JOIN AS(
SELECT
    d.*,
    f.PLATENO
FROM PROD_GOLD.PARKING.DIM_PARKINGCUSTOMER AS d
JOIN PROD_GOLD.PARKING.FACT_PARKING AS f
    ON f.FK_PARKINGCUSTOMER = d.pk_parkingcustomer
)

SELECT *
FROM CUSTOMER_DIM_JOIN
WHERE PLATENO = '7VFP220'

/*------------------------------------------------- Limiting the join to only return 1000 rows-----------------------------------------------------*/    
WITH LIMITED_JOIN AS(
SELECT d.*
FROM PROD_GOLD.PARKING.DIM_PARKINGCUSTOMER AS d
LIMIT 1000
)

SELECT 
    d.*,
    f.PLATENO as LICENSE_PLATE_NUMBER
FROM LIMITED_JOIN AS d
JOIN PROD_GOLD.PARKING.FACT_PARKING AS f
    ON f.FK_PARKINGCUSTOMER = d.pk_parkingcustomer;

/*---------------------- Integrating the limited join to LICENSEPLATEAPI NOTEBOOK ------------------------------------- */

WITH LIMITED_JOIN AS(
SELECT d.*
FROM PROD_GOLD.PARKING.DIM_PARKINGCUSTOMER AS d
--LIMIT 1000
),

raw_ingest AS(               
    SELECT 
    d.*,
    f.PLATENO AS LICENSE_PLATE_NUMBER
FROM LIMITED_JOIN AS d
JOIN PROD_GOLD.PARKING.FACT_PARKING AS f
    ON f.FK_PARKINGCUSTOMER = d.pk_parkingcustomer
),

already_processed AS ( -- Checks to see if any plates that were gathered from the data set have already been ran before and if so removes them
    SELECT LICENSE_PLATE_NUMBER
    FROM  DEV_SILVER.PARKING.PARKING_VEHICLE_VALUE
),

TEST AS(
SELECT  r.*
FROM    raw_ingest AS r
LEFT JOIN already_processed AS p
ON  r.LICENSE_PLATE_NUMBER = p.LICENSE_PLATE_NUMBER
    WHERE p.LICENSE_PLATE_NUMBER IS NULL
)

SELECT DISTINCT LICENSE_PLATE_NUMBER
FROM TEST 

------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* Creating a sample data set that grabs an even amount of license plates from the different parking lots in a month, ensures plates that have already been ran through the API are filtered out. */

/*CTE #1 that seperates license plates by parking lot, and entry date*/
WITH LOT_LOCATION AS(
SELECT d.CARPARKLOCATION, f.plateno, TO_DATE(f.fk_entrydate::VARCHAR,'YYYYMMDD') AS entry_date, d.datasourcename
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
AND entry_date >= DATE_TRUNC('month', CURRENT_DATE)                
AND entry_date < DATEADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))
LIMIT 333
),

/*CTE #3 that grabs license plates from Terminal 1 Parking Plaza*/
TERMINAL1PARKINGLOT AS(
SELECT DISTINCT* 
FROM LOT_LOCATION
WHERE CARPARKLOCATION = 'Terminal 1 Parking Plaza'
AND entry_date >= DATE_TRUNC('month', CURRENT_DATE)                
AND entry_date < DATEADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))
LIMIT 333
),

/*CTE #4 that grabs license plates from Terminal 2 Parking Plaza*/
TERMINAL2PARKINGLOT AS(
SELECT DISTINCT*
FROM LOT_LOCATION
WHERE CARPARKLOCATION = 'Terminal 2 Parking Plaza'
AND entry_date >= DATE_TRUNC('month', CURRENT_DATE)                
AND entry_date < DATEADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))
LIMIT 333
),

/*CTE #5 that union's all 3 parking lot CTE's to create an even sample data of license plates from all 3 lots*/
SAMPLEDATA AS(
(
SELECT PLATENO AS LICENSE_PLATE_NUMBER, CARPARKLOCATION
FROM VALETPARKINGLOT
)
UNION 
(
SELECT PLATENO AS LICENSE_PLATE_NUMBER, CARPARKLOCATION
FROM TERMINAL2PARKINGLOT
)
UNION
(
SELECT PLATENO AS LICENSE_PLATE_NUMBER, CARPARKLOCATION
FROM TERMINAL1PARKINGLOT
)
),

/*CTE #6 that creates a table to check if any plates have already been processed through the API */
already_processed AS (
    SELECT LICENSE_PLATE_NUMBER
    FROM  DEV_SILVER.PARKING.PARKING_VEHICLE_VALUE
),

/*Left join that removes any license plates that have already been processed*/
API_DATA AS(
SELECT  r.*
FROM SAMPLEDATA AS r
LEFT JOIN already_processed AS p
ON  r.LICENSE_PLATE_NUMBER = p.LICENSE_PLATE_NUMBER
    WHERE p.LICENSE_PLATE_NUMBER IS NULL
)

SELECT DISTINCT LICENSE_PLATE_NUMBER
FROM API_DATA;




