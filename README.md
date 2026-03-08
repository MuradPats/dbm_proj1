## Setup and How to Run

### Step 1: Start Jupyter with Docker Compose

In the folder that contains `docker-compose.yml`, run:

```bash
docker compose up -d
```

Then open:

```bash
http://localhost:8888
```

Token:

```bash
bdm
```

To stop:

```bash
docker compose down
```

### Step 2: Open and Run the Notebook

In JupyterLab:

  1. Open the work/src/ folder
  2. Open Untitled.ipynb
  3. Click Run → Run All Cells

---


## 1. Incremental Ingestion

The pipeline processes taxi trip files incrementally from the `data/inbox/` directory. Each run checks which input files have already been processed and only processes newly arrived files.

### File Discovery

All Parquet files in the inbox directory are scanned:

```python
trip_files = sorted(INBOX.glob("*.parquet"))
````
Previously processed files are tracked in a manifest file located at:  
```
state/manifest.json  
````

The manifest contains metadata about each processed input file.  
```python
processed_files = {entry["filename"] for entry in manifest["processed_files"]}
````
New files are determined by excluding those already present in the manifest:
```python
new_files = [f for f in trip_files if f.name not in processed_files]
````
### Incremental Processing.  

Each new file is processed independently. For every file:  
- The Parquet file is read into a Spark DataFrame.
- 'source_file` and `ingested_at` metadata columns are added

## 2. Data Transformation

The `transform()` function performs schema normalization, data cleaning, and deduplication on raw taxi trip records before writing the processed dataset to the output storage in Parquet format.

The transformation pipeline is designed to ensure **consistent schema types**, **remove invalid records**, and **prevent duplicate trips** when ingesting data incrementally.


## 2a. Schema Normalization

The first step enforces explicit data types for key fields. Although Parquet preserves types, casting ensures consistency across datasets and protects against **schema drift** when new files are ingested.

| Column | Target Type | Description |
|------|------|------|
| `VendorID` | integer | Taxi vendor identifier |
| `PULocationID` | integer | Pickup location zone ID |
| `DOLocationID` | integer | Dropoff location zone ID |
| `passenger_count` | integer | Number of passengers |
| `trip_distance` | double | Distance traveled during trip |
| `fare_amount` | double | Base fare charged |
| `total_amount` | double | Final total paid |
| `tpep_pickup_datetime` | timestamp | Trip pickup time |
| `tpep_dropoff_datetime` | timestamp | Trip dropoff time |

This step ensures downstream analytics can rely on **consistent numeric and timestamp formats**.

## 2b. Data Cleaning Rules

After type normalization, several validation rules are applied to remove invalid or corrupted records.

### Rule 1: Remove rows with missing critical identifiers

Rows missing essential identifiers or timestamps are discarded.

Required fields:

- `VendorID`
- `PULocationID`
- `DOLocationID`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

These fields are necessary to uniquely identify and analyze a trip. Records missing any of these values are considered unusable.

Implementation:

```python
.dropna(subset=[
    "VendorID",
    "PULocationID",
    "DOLocationID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
])
````

---

### Rule 2: Enforce positive trip distance

Trips with non-positive distance are removed.

Condition:

```
trip_distance > 0
```

This is because zero or negative distances typically indicate:

  * system logging errors
  * canceled rides
  * corrupted records

Implementation:

```python
.filter(F.col("trip_distance") > 0.0)
```

### Rule 3: Ensure non-negative total charge

Trips with negative total charges are removed.

Condition:

```
total_amount >= 0
```

Negative totals may occur due to:

* refund adjustments
* billing glitches
* malformed records

These values would distort financial and operational metrics.

Implementation:

```python
.filter(F.col("total_amount") >= 0.0)
```

## 2c. Deduplication Strategy

The TLC dataset does **not provide a unique primary key** for trips. To prevent duplicate records when ingesting data, a **composite deduplication key** is used.

### Deduplication Key

The following columns form the uniqueness constraint:

```
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
PULocationID
DOLocationID
```

Implementation:

```python
dedup_key = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID"
]
```

Duplicates are removed using:

```python
.dropDuplicates(subset=dedup_key)
```

### Rationale

This composite key assumes that the probability of the same taxi vendor performing two separate trips **starting at the exact same second**, **ending at the exact same second**, and **between the same pickup and dropoff zones** is effectively zero. Therefore, records sharing all these attributes are considered duplicates and only one is retained.

**Note on Deduplication Scope**

Deduplication is performed only within the currently processed dataset, not against previously processed output files. The project brief does not specify a requirement in maintaining a global deduplication state across historical data. Implementing cross-file deduplication would require additional mechanisms (e.g., maintaining a persistent index, performing merge operations, or re-reading historical partitions), which would add complexity and computational overhead to the pipeline. For the purposes of this project, the pipeline assumes that duplicate records are most likely to occur **within newly ingested data**, and therefore removes duplicates using the defined composite key during the current transformation step.


## 3. Data Enrichment

After cleaning and deduplication, each trip record is enriched with metadata from Taxi Zone lookup table.

The lookup dataset contains mappings between `LocationID` values and human-readable zone information.

### Pickup and Dropoff Enrichment

The lookup table is joined twice:

1. **Pickup zone enrichment**
2. **Dropoff zone enrichment**

Separate aliases are created to avoid column conflicts.

Implementation:

```python
.join(F.broadcast(pu_zones), trips_final["PULocationID"] == pu_zones["pu_location_id_lookup"], "left")
.join(F.broadcast(do_zones), trips_final["DOLocationID"] == do_zones["do_location_id_lookup"], "left")
````
The taxi zone lookup table is small relative to the trip dataset. A broadcast join is used to avoid shuffling the larger dataset.

## 4. Manifest + Custom Scenario

The pipeline maintains processing state using a manifest file located at:
````
state/manifest.json
````

The manifest tracks which input files have already been processed, ensuring the pipeline operates incrementally and avoids reprocessing the same files.

### Manifest Structure

Each processed file entry contains:

| Field | Description |
|------|------|
| `filename` | Name of the processed input file |
| `file_size` | File size in bytes |
| `processed_at` | Timestamp when the file was processed |
| `raw_row_count` | Number of rows read from the raw input file |
| `rows_after_cleaning` | Rows remaining after data cleaning |
| `rows_after_dedup` | Rows remaining after deduplication |
| `duplicates_dropped` | Number of duplicate records removed |


## 5 Main loop
For each new file we do:
- read a file
- process file
- append to output
- add to manifest


