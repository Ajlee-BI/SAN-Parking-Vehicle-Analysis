WITH cleaned AS (
    SELECT
        "CarparkDesig",
        "PlateNo",
        "Amount",
        "MovementTypeDesig",
        "Time"
    FROM DEV_BRONZE.PARKING.SKIDATA_PARKING_MOVEMENTS
    WHERE "PlateNo" IS NOT NULL
      AND "PlateNo" <> ''
      AND "PlateNo" <> '#'
      AND "PlateNo" NOT LIKE '0%'
)

SELECT *
FROM cleaned;

/* Collapse all gate events belonging to one stay */
SELECT
    "PlateNo"              AS LICENSE_PLATE_NUMBER,
    "CarparkDesig"         AS PARKING_LOT_LOCATION,
    'SKIDATA'            AS SOURCE_SYSTEM_NAME,
    "Time",
    "MovementTypeDesig",

-- Case for Entry Time
    
CASE
    WHEN "MovementTypeDesig" = 'Entry' THEN "Time"
END AS ENTRY_TIME,

-- Case for Exit Time

CASE
    WHEN "MovementTypeDesig" = 'Transaction' or "MovementTypeDesig" = 'Exit' THEN "Time"
END AS EXIT_TIME,

-- Case for Revenue

CASE 
    WHEN "MovementTypeDesig" = 'Transaction' THEN "Amount"
END AS REVENUE

FROM DEV_BRONZE.PARKING.SKIDATA_PARKING_MOVEMENTS


    DATEDIFF('minute', Entry_Time, Exit_Time) AS LENGTH_OF_STAY_MINUTES
FROM cleaned
MATCH_RECOGNIZE (
    PARTITION BY "PlateNo"
    ORDER BY     "Time"

    /* ---- columns to emit per match ---- */
    MEASURES
        FIRST(Entry."Time")                             AS "Entry_Time",
        COALESCE( LAST(Exit."Time"),
                  LAST(Trans."Time") )                  AS "Exit_Time",
        SUM(Trans."Amount")                             AS "Revenue"

    /* ---- legal sequence for one stay ---- */
    PATTERN ( Entry ( Reject* Trans? )? Exit? )

    /* ---- how to label each raw row ---- */
    DEFINE
        "Entry"  AS "MovementTypeDesig" = 'Entry',
        "Reject" AS "MovementTypeDesig" = 'Rejection',
        "Trans"  AS "MovementTypeDesig" = 'Transaction',
        "Exit"   AS "MovementTypeDesig" = 'Exit'
)
WHERE Exit_Time IS NOT NULL;        -- keep only completed stays

