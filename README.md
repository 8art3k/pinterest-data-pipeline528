# Pinterest Data Pipeline Project

## Table of Contents

1. Project Description
2. Setup Instructions
3. Challenges Faced
4. What I Learned

---

## Project Description

This project simulates a data pipeline designed to handle data similar to what a data engineer at **Pinterest** might encounter. The project involves connecting to a remote **EC2** instance, managing data in **Kafka** topics, and eventually storing data in an **S3** bucket for later processing.

The data comes from three tables that represent **user posts**, **geolocation**, and **user information** related to posts uploaded to Pinterest. The main goal of this project was to connect to the EC2 instance, create the necessary Kafka topics, and send data to the provided API endpoint.

### What the project does:
- **Connect**: Connects to an EC2 instance and retrieves data from RDS.
- **Kafka Integration**: Creates Kafka topics and sends the data to Kafka via an API proxy.
- **Store**: The data is then stored in an S3 bucket for later processing.
- **Read & Process**: Read the data from the S3 bucket into Databricks using SQL
- **Monitor**: Status is monitored using journal logs to ensure the Kafka REST proxy is receiving data.

---

## Setup Instructions

1. **Download the provided file** that contains the necessary code and database credentials.

2. **Set up AWS resources**:
   - Create and save the **Key Pair** for EC2 instance access.
   - Ensure the EC2 instance is running Kafka and the Kafka API REST proxy service.

3. **Create a `db_creds.yaml` file** containing the database credentials and make sure this file is hidden to avoid uploading sensitive information to GitHub.

4. **Create Kafka topics** for:
   - `262542bdae36.pin` – for Pinterest post data
   - `262542bdae36.geo` – for geolocation data
   - `262542bdae36.user` – for user data

5. **Modify the `user_posting_emulation.py`** script to send data to the Kafka topics using the API invoke URL.

6. **Run the Kafka API REST proxy service** and monitor the status.

7. **Read Data into Databricks**:
   - After the data is stored in the S3 bucket, we read it into Databricks using SQL commands to create three DataFrames:
     - `df_pin` for Pinterest post data
     - `df_geo` for geolocation data
     - `df_user` for user data

---

## Challenges Faced

### Kafka Data Format Issues:
- **Problem**: When sending data to Kafka, the `datetime` fields were not being correctly recognized, causing errors in the data transmission process.
- **Solution**: The solution was to convert the `datetime` objects to strings using `convert_datetime_to_string()` before sending them to Kafka. This allowed the data to be processed correctly without any issues related to date formats.

### EC2 Setup Issues:
- **Problem**: While installing Kafka and the necessary Confluent modules on the EC2 instance, I encountered a storage shortage problem. In my attempt to free up space, I mistakenly deleted files that were essential for running the Kafka API REST Proxy service, which was already running on the EC2 instance.
- **Solution**: This required setting up a new EC2 instance from scratch with the help of a supporting engineer. The engineer helped ensure Kafka and the API proxy service were correctly installed and configured, allowing me to proceed with the data transmission tasks.

---

## Data Cleaning and Transformation in Databricks
During the project, several data cleaning and transformation tasks were performed on the three main DataFrames (`df_pin`, `df_geo`, and `df_user`) to ensure consistency and usability for analysis. These tasks included:

- **Handling Missing or Inconsistent Data**: Replaced empty or irrelevant entries with `NULL` values across all tables.
- **Data Type Corrections**: Converted columns to appropriate data types, ensuring numerical columns were of integer or float type, and string columns were correctly formatted as strings or timestamps.
- **Column Transformations**: 
  - Combined relevant columns (e.g., `first_name` and `last_name`) into new columns (e.g., `user_name`).
  - Created new columns to store cleaned or transformed data (e.g., `coordinates` from latitude and longitude).
  - Dropped redundant or unnecessary columns (e.g., `latitude`, `longitude`, `first_name`, `last_name`).
- **Data Standardization**: Standardized column values, such as cleaning the `save_location` column to retain only relevant path data and renaming columns for consistency (e.g., renaming `index` to `ind`).

## Data Analysis
Several SQL queries were executed to extract meaningful insights from the data. The following key pieces of information were extracted:

-  Identified the most popular category for users based on their country.
-  Analyzed the number of posts in each category for the years 2018 through 2022.
-  Found the user with the highest follower count in each country.
-  Determined the most popular Pinterest categories for different user age groups (`18-24`, `25-35`, `36-50`, and `+50`).
-  Calculated the median follower count for users in each of the specified age groups.
-  Counted how many users joined Pinterest between 2015 and 2020.
-  Determined the median follower count for users who joined between 2015 and 2020.
-  Calculated the median follower count for users in the age groups of 2015-2020.

---

## What I Learned

### Data Pipeline Fundamentals:
- I gained hands-on experience setting up a data pipeline that integrates various technologies like **Kafka**, **AWS EC2**, and **S3**. This included managing data flow between different components of the system.

### Understanding of Kafka:
- I learned how to set up and manage Kafka topics, ensuring smooth communication between different data sources and endpoints. I also became familiar with using Kafka's REST proxy to send data between Kafka and external services like S3.

### AWS Integration:
- This project provided valuable experience working with AWS resources such as **EC2**, **RDS**, and **S3**. I learned how to configure EC2 instances, install necessary services, and store processed data in S3 for later use.

### Debugging & Issue Resolution:
- Encountering issues such as Kafka data format errors and EC2 misconfigurations taught me how to troubleshoot and resolve problems effectively. These experiences helped me understand the importance of proper setup, data formats, and error handling in production pipelines.

### Databricks SQL Integration:
- I learned how to read data from an **S3** bucket into **Databricks** using **SQL** commands.

### Databricks Table Modifications:
- I learned that in Databricks, direct table modifications such as adding or removing columns are not possible. Instead, transformations are done by creating **temporary views** or **new tables** that reflect the changes or transformations needed for analysis.

### Timestamp Format in Databricks:
- In Databricks, the timestamp is represented in the ISO 8601 format, which includes the date and time with a `T` separator (e.g., `2021-04-01T00:56:57`). This format ensures consistent handling of timestamp data.