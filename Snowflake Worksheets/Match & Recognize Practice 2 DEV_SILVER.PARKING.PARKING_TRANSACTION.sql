/* ───────────────── 1. (optional) basic cleanup ──────────────── */
WITH cleaned AS (
    SELECT DISTINCT
        "CarparkDesig",
        "PlateNo",
        "Amount",
        "MovementTypeDesig",
        "Time"
    FROM DEV_BRONZE.PARKING.SKIDATA_PARKING_MOVEMENTS
    WHERE  "MovementTypeDesig" IN ('Entry','Transaction')
    AND "PlateNo" = '121M31'
   
      --AND  "PlateNo" <> ''
      --AND  "PlateNo" <> '#'
      --AND  "PlateNo" NOT LIKE '0%'
      --AND  "PlateNo" IN (                     -- ← sub-query begins
             --SELECT DISTINCT "PlateNo"
             --FROM   DEV_BRONZE.PARKING.SKIDATA_PARKING_MOVEMENTS
             --LIMIT  100
        -- )
)


/* ───────────────── 2. find one row per completed stay ───────── */
SELECT
    LICENSE_PLATE_NUMBER,
    PARKING_LOT_LOCATION,
    'SKIDATA' AS SOURCE_SYSTEM_NAME,
    ENTRY_TIME,
    EXIT_TIME,
    REVENUE,
    DATEDIFF('minutes', ENTRY_TIME, EXIT_TIME) AS DURATION_IN_MINS
    
FROM cleaned
MATCH_RECOGNIZE (

PARTITION BY "PlateNo"
ORDER BY     "Time"
    
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
ORDER BY ENTRY_TIME
;
