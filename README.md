# SAN Airport Authority Business Intelligence Summer Internship 2025

# Project Goal

Enrich airport parking transactions with **vehicle details** (type, specs, market value) to enable **customer segmentation** by parking location and vehicle characteristics.

# Scope

- **CV/OCR → Plate Text**
  - Detect license plates and OCR them into text strings for downstream APIs.
- **ETL (Snowflake) → Enriched Facts**
  - Ingest vendor data (Chauntry, FlashValet, SKIDATA).
  - Join with a Vehicle Database API to append **vehicle attributes** and **market value**.
- **Power BI → Monthly Snapshot**
  - Publish a monthly snapshot table combining transactions and vehicle value for analysis and segmentation.

# Data Flow (high level)

`Plate image → OCR → Plate/state string → Vehicle API lookup → Join to vendor transactions → Enriched table in Snowflake → Power BI monthly snapshot`

# Outcomes

- Segment customers by **where they parked** and **what they drive**.
- Single, consistent source for reporting and decisions.

_________

# 1) Computer Vision / OCR Model

**Purpose:** Convert license-plate photos into clean text strings for downstream ETL/API enrichment.

### Pipeline (high level)
`Image → YOLOv8 plate detect → Crop to bbox → Preprocess (B/W, edges, blur) → OCR → CSV`

### Model & Training
- **Detector:** YOLOv8, custom-trained on open-source license plate datasets.
- **Training:** 100 epochs; model reliably finds plates and draws a bounding box.
- **Output of detection:** Cropped plate image for OCR.

### Preprocessing (to boost OCR)
- Convert to **black & white**.
- Apply **edge detection** and **blurring** to emphasize character edges.

### OCR & Output
- Run an OCR reader on the cropped, preprocessed plate.
- Save results to **CSV** (one row per image; includes the raw OCR string).

### Current Challenges
- **Character confusions:** S↔Z, B↔8, etc.
- **Noise capture:** Extra tokens from **state names**, **frame text**, **stickers**, or surrounding features.
- **Partial/over-complete strings:** OCR may return only part of the plate, or a plate plus unwanted text.
- **Impact:** Hard to produce a perfectly clean string for the ETL/API pipeline.

### Mitigations Under Consideration
- **Custom OCR** specifically trained for **U.S. license plates**.
- **Character classifier** to better distinguish **letters vs. digits** on plates.

### Scripts
- **YOLOv8 License Plate Detection (training)**
  - Trains a model to detect license plates in photos (100 epochs used in this project).
  - Notes: Detector is accurate overall but still makes occasional mistakes.
- **Automated OCR (batch)**
  - Scans a folder of plate photos → detects plates → runs OCR → writes results to a `.csv`.

> Add your actual script paths/CLI usage here when publishing (e.g., `python scripts/train_yolov8.py ...` / `python scripts/run_ocr_batch.py ...`).

### Additional Resources (to link)
- [License plate specific OCR model that can be custom trained](https://github.com/ankandrew/fast-plate-ocr)
- Test photos: **[add link]**
- License-plate–specific OCR that supports custom training: **[add link]**

# 2) ETL Pipeline Explained:
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

# API Notebook

**Purpose:** Query the **Vehicle Databases** API for license plates and append **VIN** and **market value** to support monthly *average vehicle value by parking lot*.

### What it does
1. **Sample selection (monthly):**
   - Pulls a **monthly sample (~1,300 transactions)** intended to estimate average vehicle value at the lot level.
2. **Credit protection / de-duplication:**
   - Excludes plates already processed by checking  
     `DEV_BRONZE.PARKING.VEHICLE_DATABASES_RESULTS`  
     (prevents re-calling the API and wasting credits).
3. **API run (loop over new plates):**
   - Loads the filtered set from SQL into a Pandas dataframe.
   - Calls Vehicle Databases for each plate/state.
   - Records **every attempt**—success or failure—to  
     `DEV_BRONZE.PARKING_VEHICLE_DATABASES_API_RESULTS`.
4. **Outputs:**
   - A complete log of results (one row per plate attempt) including status, VIN (if found), and market value.
   - Downstream logic aggregates to **monthly, by-lot averages**.

### Result handling
- **Success:** Valid VIN and market value returned.
- **No hit / invalid:** Plate not found or invalid format.
- **Out of scope:** Plate outside the **5 supported states** in the region.
- All outcomes are persisted so the **same plate is never re-run**.

### Scheduling & performance
- **Schedule:** Runs **monthly** on the **1st**.
- **Observed runtime:** ~**4.5 hours** for ~**1,300 plates** (≈ **4.8 plates/min**).  
  Actual time varies with plate count and API latency.

### Suggested columns for `DEV_BRONZE.PARKING_VEHICLE_DATABASES_API_RESULTS`
- `run_id`, `plate`, `state`, `vin`, `market_value`
- `status` (`success` | `no_hit` | `invalid` | `out_of_scope` | `error`)
- `error_code`, `error_message` (nullable)
- `api_response_ts`, `created_ts`

### Notes / assumptions
- The 1,300-plate sample is used as the **basis for estimating by-lot averages**; adjust as needed for precision/CI targets.
- Keeping **all attempts** (including failures) in the results table is intentional—this **guarantees de-dupe** and conserves API credits.







## Monthly_TXN_Sample

## MONTHLY_SNAPSHOT_
