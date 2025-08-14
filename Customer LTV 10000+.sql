/*Selects all license plate numbers with Customer Life Time Value greater than or equal to 10000 and filters them out with license plates already ran through the API Notebook*/

/*Goes through dim customers and selects customers who have a CLT over 10000*/
With CLT10000 as(
select *
from PROD_GOLD.parking.dim_parkingcustomer
where customerltv >= 10000
),

/*Joins the dim_parkingcustomers with bridge_parkingcustomerplatedata to be able to associate customer data with their license plate using the keys */
CLTPLATEDATA AS (
select r.*, p.*
from PROD_GOLD.parking.bridge_parkingcustomerplatedata as r
INNER JOIN CLT10000 as p
on r.fk_parkingcustomer = p.pk_parkingcustomer 
),

/*Actually joins the license plate data with the customer dimension*/
FULLTABLE AS(
select a.*,b.*
from CLTPLATEDATA as a
INNER JOIN prod_gold.parking.dim_parkingplatedata as b
on a.fk_parkingplatedata = b.pk_parkingplatedata
),

/*License plates that have already been ran through the API Notebook to filter out duplicate plates*/
already_processed as(
select PLATE as LICENSE_PLATE_NUMBER
from DEV_BRONZE.PARKING.VEHICLE_DATABASES_API_RESULTS_DATA
),

LICENSE_FILTERED as (
SELECT r.*
FROM fulltable as r
LEFT JOIN already_processed as p
on r."plateno" = p.license_plate_number
where p.license_plate_number is null
),

/*Joins the LTV license plates over 10000 to all their parking transactions*/
FACTPARKING_PLATETABLE AS (
SELECT R.*,P.*
FROM LICENSE_FILTERED AS R
LEFT JOIN PROD_GOLD.PARKING.FACT_PARKING AS P
ON R."plateno" = P.plateno
),

/*Gives the count of license plates for each transaction to figure out which license plates were most used. Removed all rows where the license plate didn't appear at least 20 times*/
FINALPLATENO AS(
SELECT 
  plateno,
  COUNT(*) AS txn_count
FROM FACTPARKING_PLATETABLE
GROUP BY plateno
HAVING COUNT(*) >= 20         -- <-- HAVING goes before ORDER BY; don't use alias here
ORDER BY txn_count DESC
)

/*Prints a list of all the license plates to be ran by the API Notebook*/
SELECT plateno as LICENSE_PLATE_NUMBER
from FINALPLATENO;
