/*This join takes all the license plates that have already been ran through the API and stored in the VEHICLE_DATABASES_API_RESULTS_DATA and filters them out in the STAGING layer so that when the API Notebook runs it only runs new plates and doesn't waste any credits. */
WITH already_processed as(
select PLATE as LICENSE_PLATE_NUMBER
from DEV_BRONZE.PARKING.VEHICLE_DATABASES_API_RESULTS_DATA
)

SELECT r.PLATENO AS LICENSE_PLATE_NUMBER
FROM DEV_BRONZE.PARKING.STAGE_VEHICLE_DATABASES_API_SAMPLE as r
LEFT JOIN already_processed as p
on r.plateno = p.license_plate_number
where p.license_plate_number is null
