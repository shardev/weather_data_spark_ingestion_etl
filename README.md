# Weather data ingestion ETL in PySpark
run: `python etl.py` <br>
This will perform run on 'dev' environment setup.

### ETL pipeline implementation for device and sensor data into a Delta Lake-based data lakehouse.

What have been done in ETL pipeline implementation:
1. Ingestion device and sensor data into a Delta Lake-based data lakehouse.
2. Cleansing the data by handling late arrivals and removing duplicates.
3. Generating a report that provides the monthly average values for `CO2_level`, `humidity`, and `temperature` for each area.
4. Ensuring the ETLs are configurable to support multiple environments (e.g., dev, prod).
