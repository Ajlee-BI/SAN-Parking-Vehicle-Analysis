/* Setting up a dynamic transaction table that will ingest all parking transactions from Chauntry, SKIDATA, and Flash Valet to help develop a Parking Transaction Fact Table. */

CREATE OR REPLACE DYNAMIC TABLE DEV_SILVER.PARKING.PARKING_TRANSACTION
    TARGET_LAG      = '1 day'
    REFRESH_MODE    = AUTO
    INITIALIZE      = ON_CREATE
    WAREHOUSE       = SAN_DEV_SERVICE_ELT_WH    
AS
-- Importing Chauntry Parking Transaction Data from Dev_Bronze 
(
SELECT
"bookingdate" AS TRANSACTION_DATE,
"paymenttotal" AS REVENUE,
"startdate" AS ENTRY_DATE,
"enddate" AS EXIT_DATE,
"carreg" AS LICENSE_PLATE_NUMBER,
"carpark_desc" AS PARKING_LOT_LOCATION,
'CHAUNTRY' AS SOURCE_SYSTEM_NAME
FROM DEV_BRONZE.PARKING.CHAUNTRY_BOOKING
)

UNION ALL

-- Importing Flash Valet Data from Prod_Bronze 
/* 

Looks like theres an issue with some rows in revenue = 0 which are because they made a reservation beforehand. we may need to add an extra column for cars that made a prior reservation or potential remove rows that are revenue = 0.  

*/

(
SELECT
"Departure" AS TRANSACTION_DATE,
"Amount" AS REVENUE,
"Arrival" AS ENTRY_DATE,
"Departure" AS EXIT_DATE,
"License" AS LICENSE_PLATE_NUMBER,
"Arrival Kiosk" AS PARKING_LOT_LOCATION,
'FLASH_VALET' AS SOURCE_SYSTEM_NAME
FROM PROD_BRONZE.PARKING.FLASH_VALET_LOCATION_TRANS_DETAIL
)

UNION ALL

-- Importing SKIDATA Transaction Data 

(
/*
We are using entry rows and transaction rows pairs to represent a complete transaction. The transaction row & time will be used for the exit time since we can assume people will leave the parking structure shortly after paying. The goal of this code is to create a sequence of Entry and Transaction pairs for each License Plate, and Each Transaction. We do this by first paritioning on license plates and ordering by time to get the correct sequence of Entry and Transaction. To transform the data to represent each row as a one complete parking transaction we use the Lead function to pull Transaction Time and Revenue in into the new columns but on the same row as each Entry. To remove any errors in the data what we return from the view is only rows that contain both an Entry and Transaction together. If SKIDATA had multiple entries or transactions for a single entry it will not record this data.
*/

WITH EntryTxnPairs AS (
    SELECT
        "CarparkDesig" AS PARKING_LOT_LOCATION,
        'SKIDATA' AS SOURCE_SYSTEM_NAME,
        "PlateNo" AS LICENSE_PLATE_NUMBER,
        "MovementTypeDesig",
        "Time" AS ENTRY_DATE,

-- Lead function to get the Exit Date & Time
        LEAD("Time") OVER (
            PARTITION BY "PlateNo"
            ORDER BY     "Time"
        ) AS EXIT_DATE,

-- Lead function that designates the next MovementType 
        LEAD("MovementTypeDesig") OVER (
            PARTITION BY "PlateNo"
            ORDER BY     "Time"
        ) AS NextMovementType,
        
-- Lead function that pulls out the Revenue for each 'Transaction Row'        
        LEAD("Amount") OVER (
            PARTITION BY "PlateNo"
            ORDER BY     "Time"
        ) AS REVENUE

        FROM DEV_BRONZE.PARKING.SKIDATA_PARKING_MOVEMENTS
        WHERE "MovementTypeDesig" IN ('Entry','Transaction')
)

/* ────────────────────────── final projection ────────────────────────── */
SELECT 
    EXIT_DATE AS TRANSACTION_DATE,
    REVENUE,
    ENTRY_DATE,
    EXIT_DATE,
    LICENSE_PLATE_NUMBER,
    PARKING_LOT_LOCATION,
    SOURCE_SYSTEM_NAME
FROM   EntryTxnPairs
)

SELECT *
FROM DEV_SILVER.PARKING.PARKING_TRANSACTION
WHERE SOURCE_SYSTEM_NAME = 'SKIDATA'