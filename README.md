
# STEDI Human Balance Analytics Project

## Project Overview

The STEDI Human Balance Analytics project involves processing sensor data from the STEDI Step Trainer and associated mobile application. The goal is to build a data lakehouse solution on AWS that enables the training of a machine learning model. The tasks included sanitizing customer and sensor data, creating curated datasets, and performing data joins and aggregations to prepare data for machine learning.

## Activities and Results

### 1. **Data Ingestion**

#### Description:
- Data was ingested from three different sources: `customer_landing`, `step_trainer_landing`, and `accelerometer_landing`. These datasets were placed in their respective S3 buckets for processing.

#### Results:
- Data successfully ingested into AWS Glue from S3 buckets.
- The following rows were observed:
  - `customer_landing`: 956 rows
  - `step_trainer_landing`: 28,680 rows
  - `accelerometer_landing`: 81,273 rows

### 2. **Sanitization of Customer Data**

#### Description:
- The customer data was sanitized to include only records where customers agreed to share their data for research purposes. The cleaned data was stored in the `customer_trusted` table.

#### Query:
```sql
CREATE TABLE customer_trusted AS
SELECT * 
FROM customer_landing 
WHERE sharewithresearchasofdate IS NOT NULL;
```

#### Results:
- The `customer_trusted` table contains 482 rows, filtered from the original 956 rows.

### 3. **Sanitization of Accelerometer Data**

#### Description:
- The accelerometer data was sanitized by filtering it to include only data from customers who agreed to share their data for research purposes. The sanitized data was stored in the `accelerometer_trusted` table.

#### Query:
```sql
CREATE TABLE accelerometer_trusted AS
SELECT ac.*
FROM accelerometer_landing ac
JOIN customer_trusted ct
ON ac.user = ct.email;
```

#### Results:
- The `accelerometer_trusted` table contains 40,981 rows, reduced from the original 81,273 rows.

### 4. **Sanitization of Step Trainer Data**

#### Description:
- The step trainer data was sanitized by filtering it to include only records from customers who have matching records in the `accelerometer_trusted` table. The cleaned data was stored in the `step_trainer_trusted` table.

#### Query:
```sql
CREATE TABLE step_trainer_trusted AS
SELECT st.*
FROM step_trainer_landing st
JOIN customer_trusted ct
ON st.serialNumber = ct.serialnumber;
```

#### Results:
- The `step_trainer_trusted` table contains 14,460 rows, reduced from the original 28,680 rows.

### 5. **Creation of Curated Customer Data**

#### Description:
- A curated dataset was created by filtering the `customer_trusted` table to include only customers who have matching accelerometer data. This data was stored in the `customers_curated` table.

#### Query:
```sql
CREATE TABLE customers_curated AS
SELECT *
FROM customer_trusted ct
WHERE ct.email IN (SELECT DISTINCT user FROM accelerometer_trusted);
```

#### Results:
- The `customers_curated` table contains 482 rows, matching the number of rows in `customer_trusted` as all customers in the `customer_trusted` table have corresponding accelerometer data.

### 6. **Creation of Machine Learning Curated Dataset**

#### Description:
- An aggregated dataset was created by joining the `step_trainer_trusted` and `accelerometer_trusted` tables on matching timestamps. This dataset includes only records from customers who have agreed to share their data for research purposes. The resulting data was stored in the `machine_learning_curated` table.

#### Query:
```sql
CREATE TABLE machine_learning_curated AS
SELECT
    st.serialNumber AS step_trainer_serialNumber,
    st.sensorReadingTime AS step_trainer_time,
    st.distanceFromObject,
    ac.timeStamp AS accelerometer_time,
    ac.x AS accelerometer_x,
    ac.y AS accelerometer_y,
    ac.z AS accelerometer_z
FROM
    step_trainer_trusted st
JOIN
    accelerometer_trusted ac
ON
    st.sensorReadingTime = ac.timeStamp
WHERE
    ac.user IN (SELECT email FROM customers_curated);
```

#### Results:
- The `machine_learning_curated` table contains 43,681 rows, representing synchronized data between the step trainer and accelerometer for customers who agreed to share their data.

## Summary of Final Metrics

- **Landing Zone:**
  - `customer_landing`: 956 rows
  - `accelerometer_landing`: 81,273 rows
  - `step_trainer_landing`: 28,680 rows
- **Trusted Zone:**
  - `customer_trusted`: 482 rows
  - `accelerometer_trusted`: 40,981 rows
  - `step_trainer_trusted`: 14,460 rows
- **Curated Zone:**
  - `customers_curated`: 482 rows
  - `machine_learning_curated`: 43,681 rows

## Conclusion

The STEDI Human Balance Analytics project successfully created a data pipeline that sanitizes and curates sensor data for use in machine learning models. The final dataset, `machine_learning_curated`, is ready for data scientists to use in training and testing predictive models.
