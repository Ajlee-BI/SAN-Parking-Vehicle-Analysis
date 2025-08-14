-- Setting up a dynamic transaction table that will ingest all transactions from Chauntry, SKIDATA, and Flash Valet to help develop a Parking Transaction Fact Table.

CREATE OR REPLACE VIEW DEV_SILVER.PARKING.PARKING_TRANSACTION
   -- TARGET_LAG      = '1 hour'
    --REFRESH_MODE    = AUTO
   -- INITIALIZE      = ON_CREATE
   -- WAREHOUSE       = SAN_DEV_SERVICE_ELT_WH
    
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

/* Looks like theres an issue with some rows in revenue = 0 which are because they made a reservation beforehand. we may need to add an extra column for cars that made a prior reservation or potential remove rows that are revenue = 0.  */

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

-- Importing SKIDATA Transaction Data (Transforms multiple rows representing different stages of a transaction into one row representing a complete transaction)

(
SELECT
    EXIT_TIME AS TRANSACTION_DATE,
    REVENUE,
    ENTRY_TIME AS ENTRY_DATE,
    EXIT_TIME AS EXIT_DATE,
    PARKING_LOT_LOCATION,
    'SKIDATA' AS SOURCE_SYSTEM_NAME

FROM DEV_BRONZE.PARKING.SKIDATA_PARKING_MOVEMENTS

MATCH_RECOGNIZE (

PARTITION BY "PlateNo"
ORDER BY "Time"
    
    MEASURES
        FIRST("PlateNo") AS LICENSE_PLATE_NUMBER,
        FIRST("CarparkDesig") AS PARKING_LOT_LOCATION,
        FIRST(Entry."Time") AS ENTRY_TIME,
        LAST(Trans."Time") AS EXIT_TIME,
        SUM(Trans."Amount") AS REVENUE
        
ONE ROW PER MATCH
AFTER MATCH SKIP PAST LAST ROW
    
PATTERN (Entry Trans)

DEFINE
    "Entry"  AS "MovementTypeDesig" = 'Entry',
    "Trans"  AS "MovementTypeDesig" = 'Transaction'
)
);

SELECT *
FROM DEV_SILVER.PARKING.VIEW_PARKING_TRANSACTION