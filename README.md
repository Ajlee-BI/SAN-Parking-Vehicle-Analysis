# SAN Airport Authority Business Intelligence Summer Internship 2025

## Goal of the Project  

The primary goal of this project was to **enrich the airport’s parking transaction data** with detailed vehicle information — including vehicle type, specifications, and market value — to enable **customer segmentation** based on where customers parked their car and what kind of car they drive.  

The project was divided into three key components:  

1. **Computer Vision / OCR Model** – Developed an object detection model and wrote an automated script to process license plate images and convert them into usable text strings for API integration.  
2. **ETL Pipeline from Snowflake** – Built an ETL pipeline to integrate transaction data from the airport’s parking vendors (Chauntry, FlashValet, SKIDATA). The pipeline enriched these transcaction data with an external Vehicle Database API to append **vehicle-specific attributes** and **market value**.  
3. **Power BI Reporting** – Combined all transaction data along with vehicle value to produce a **monthly snapshot table** in Power BI, providing an overall view of parking behaviors and customer segments for analysis and decision-making.  





## 1) Computer-Vision / OCR Model Explained

The first stage of the project involved building an automated script that used a **custom-trained computer vision model** alongside an OCR reader to detect and read license plate characters from photos.  

I trained a **YOLOv8 model** using open-source license plate datasets, running 100 epochs to fine-tune the model. The model first identifies where a license plate is located within the image and draws a bounding box around that area. The image is then cropped to this bounding box so that only the license plate remains.  

From there, the image undergoes multiple preprocessing steps to improve OCR accuracy, including:  
- Applying a **black-and-white filter**  
- Using **edge detection** and **blurring** to highlight character edges  

These steps help the OCR model more accurately recognize the characters on the plate.  

The biggest challenge has been achieving **high OCR accuracy** using open-source models. The OCR frequently misreads common characters (e.g., confusing **S** with **Z** or **B** with **8**) or picks up extra noise. Often, the OCR produces a **partial or full license plate** but adds unwanted characters or words from the plate’s surrounding features — such as the state name, license plate frame text, or registration stickers.  

This makes it difficult to extract a perfectly clean string ready for use in the **ETL / API enrichment pipeline**.  

**Potential solutions** being considered include:  
- Training a **custom OCR model** specifically for U.S. license plates  
- Building an **object classification model** that can better distinguish numbers and letters on plates  

**YOLOv8 License Plate Object Detection Training Script**
Script that trained the YOLOv8 Model to detect license plates within a photo, ran it for 100 epochs to improve accuracy. The model itself is pretty accurate but does make mistakes on a rare occasion

**Automated OCR Script**
Script that automatically goes through photos of license plates, detects them within the image an uses an OCR reader to turn it into a string and then saves them as a .csv file 

**Additonal Resources**
[License plate specific OCR model that can be custom trained](https://github.com/ankandrew/fast-plate-ocr)
- Include link to license plate datasets
- Include link to test photos




## 2) ETL Pipeline Explained:
Second Stage: Parking Transaction Data Integration
The second stage of the project focused on consolidating transaction data from three different parking lot vendor sources — Chauntry, FlashValet, and SKIDATA — into a unified, dynamic parking transaction table. This table serves as the foundation for a parking star schema, which provides a structured view of each transaction and connects to various dimensions for analysis.
The schema captures key transaction details, including:
- Date and time of entry and exit
- Payment timestamps
- Parking lot location
- Source system (Chauntry, FlashValet, or SKIDATA)

**Key Challenge: SKIDATA Transaction Structure**
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
The API Notebook is the main function of the pipeline that takes license plates and retreives vehicle value through an API service called Vehicle Databases. The notebook is orientated to pull data from a monthly sample of transactions which will be the basis of the sampled average vehicle value by parking lot. The transaction sample contains 1300 transactions calculated to be statistically signifigant for the population which I considered to be the average number of transactions by parking lot. The sampled license plates are then filtered out through DEV_BRONZE.PARKING.VEHICLE_DATABASES_RESULTS this filters out any license plates that have already been ran through the notebook ensuring that no credits are wasted and any plates that have been ran before won't be ran though again. Once the license plates have been filtered to only contain new plates the database from SQL is then imported into a Pandas dataframe where an automated loop goes through all the license plates and either returns a value or an error if the plate was invalid or not within the 5 states in the region. 






## Monthly_TXN_Sample

## MONTHLY_SNAPSHOT_
