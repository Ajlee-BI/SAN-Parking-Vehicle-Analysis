CREATE OR REPLACE PIPE DEV_BRONZE.PARKING.OCR_DATA_PIPE auto_ingest=true as /*Creates or replaces a pipe auto_ingest means when a new file is loaded into S3 bucket it's automatically imported into Snowflake*/
COPY INTO DEV_BRONZE.PARKING.OCR_DATA /*Location of the table that will receive the new data*/
FROM @dev_common.utilities.s3_stage/ocr/ /*Tells Snowflake where to grab the files from the S3 bucket*/
FILE_FORMAT = (
TYPE = 'CSV' /*Tells them to look for .csv files*/
  FIELD_DELIMITER = ',' /*Data is seprated by a column*/
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' /*Each entry may be enclosed by double or single brackets*/
  PARSE_HEADER = TRUE /*Keep the first row which contains the column headers*/
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE /*If new data is added or missing some rows do not cause an error instead just put Null into these rows*/
)

MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE; /*the data is inputed by the same column names and it's not case sensitive*/
