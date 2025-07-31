# SAN Airport Authority Business Intelligence Summer Internship 2025

## Goal of the project:
The goal of this project was to be able to enrich the Airport's parking transaction data with vehicle value, and vehicle specific information that could help segement customers based upon where they parked and what kind of car they drive. The project was broken down into three seperate parts: Computer-Vision / OCR model to take license plate images and transform them into a usable string of characters, ETL pipeline from snowflake taking transaction data from out current parking lot vendors and using license plate data along with Vehicle Databases API to add vehicle specific data and market value, The last part of the project was taking both the transactions and vehicle data and combining them to create a monthly snapshot table in Power BI. 

## Computer-Vision / OCR Model Explained:
The first stage of the project was building an automated script that used a custom trained computer-vision model along with an OCR reader to detect and read license plate characaters from a photo. I trained a YOLOv8 model using open source datasets of license plates and running 100 epochs to fine-tune the model. The model first detects where a license plate is within the photo and draws a boundary box around it's approximated area, afterwards the image is then cropped to the boundary box to only contain the license plate. From here the image goes through multiple processes to make it easier for the OCR model to identify the characters from the license plate. These processes include a black and white filter, edge detection / blurring to help highlight the edges of each Character to help improve the OCR's accuracy. The hardest part of this project has been getting an open-source OCR model to give high accurate readings of license plates without picking up extra noise or mistaking common letters such as S & Z or B & 8. Most of the times it's able to get a partial / full license plate but with extra characters / words included at the beginning or end due to the OCR model reading everything within the license plate which may include the state, license plate frame, month or year of registration expiration, so it's been difficult to pull a completely perfect string that's ready to be used for the ETL / API enrichment pipeline. Some potential solutions could be training a custom OCR model to read U.S. specific license plates or training an object classification model that can classify numbers and letters from license plates.   


### YOLOv8 License Plate Object Detection Training Script
Script that trained the YOLOv8 Model to detect license plates within a photo, ran it for 100 epochs to improve accuracy. The model itself is pretty accurate but does make mistakes on a rare occasion

### Automated OCR Script
Script that automatically goes through photos of license plates, detects them within the image an uses an OCR reader to turn it into a string and then saves them as a .csv file 

### Additonal Resources
[License plate specific OCR model that can be custom trained](https://github.com/ankandrew/fast-plate-ocr)
- Include link to license plate datasets
- Include link to test photos

## ETL Pipeline Explained:
Second Stage: Parking Transaction Data Integration
The second stage of the project focused on consolidating transaction data from three different parking lot vendor sources — Chauntry, FlashValet, and SKIDATA — into a unified, dynamic parking transaction table. This table serves as the foundation for a parking star schema, which provides a structured view of each transaction and connects to various dimensions for analysis.
The schema captures key transaction details, including:
- Date and time of entry and exit
- Payment timestamps
- Parking lot location
- Source system (Chauntry, FlashValet, or SKIDATA)

### Key Challenge: SKIDATA Transaction Structure
One of the biggest challenges during this phase was handling SKIDATA’s unique table design. Unlike the other vendors, SKIDATA records up to four rows for a single parking transaction because it logs each step in the parking process separately and each row stores a revenue amount but the only one that has an actual number besides zero.

Each row is labeled with a MovementTypeDesig, which indicates the stage of the transaction. The sequence can vary depending on how the customer interacts with the system. For example:

If a customer pays at the pay station before driving to the gate, the sequence looks like:
(Entry → Transaction → Exit)

If a customer drives to the gate and is rejected before paying, the sequence may look like:
(Entry → Rejection → Exit → Transaction)

To standardize the data, we transformed SKIDATA’s multi-row structure into a single-row format by using SQL lead and lag functions, combined with a WHERE clause to filter only rows that represented complete transactions. This approach effectively pivoted the data, turning sequential movement events into columns. Careful mapping of MovementTypeDesig values was essential to ensure every transaction was accurately reconstructed and aligned within the unified schema.



- Include Image of 1 complete transaction highlighted to show the 4 rows
- Include an image afterwards to show the pivoted / elongated table.

## API Notebook Explained:


## Monthly_TXN_Sample

## MONTHLY_SNAPSHOT_
