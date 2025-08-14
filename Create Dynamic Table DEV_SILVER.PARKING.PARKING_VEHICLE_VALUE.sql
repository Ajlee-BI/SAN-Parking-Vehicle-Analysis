create or replace dynamic table DEV_SILVER.PARKING.PARKING_VEHICLE_VALUE(
	LICENSE_PLATE_NUMBER,
	STATE,
	TRIM_LEVEL,
	VIN,
	YEAR,
	MAKE,
	MODEL,
	CLEAN_MARKET_VALUE,
    IMPORT_DATE
) target_lag = '1 hour' refresh_mode = AUTO initialize = ON_CREATE warehouse = SAN_DEV_SERVICE_ELT_WH
 as
SELECT DISTINCT
    UPPER(TRIM(PLATE))                     AS LICENSE_PLATE_NUMBER,
    UPPER(TRIM(STATE))                     AS STATE,
    UPPER(TRIM("TRIM"))                    AS TRIM_LEVEL,
    UPPER(TRIM(VIN))                       AS VIN,
    YEAR,
    UPPER(TRIM(MAKE))                      AS MAKE,
    UPPER(TRIM(MODEL))                     AS MODEL,
    CLEAN_MARKET_VALUE,
    IMPORT_DATE
FROM DEV_BRONZE.PARKING.VEHICLE_DATABASES_API_RESULTS_DATA
WHERE CLEAN_MARKET_VALUE IS NOT NULL