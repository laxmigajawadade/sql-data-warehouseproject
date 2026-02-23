# sql-data-warehouseproject
Building a Data Warehouse through ETL using Medallion Architecture (Bronze, Silver, Gold). <br>
**Extraction**: extracting data from files(.csv) through a pull mechanism using File Parsing. <br>
**Transformation**: Normalization, Handling null values, Derived Columns, Data Aggregation, Integration & Enrichment. 
    Data Cleaning: Removing duplicates, handling missing values, and invalid data, Date typecasting, outlier Detection.<br>
**Loading**: Full Load (Truncate & Insert) for Batch Processing and implementing SCD type 1 and 2<br>
**Bronze Layer**: Extracting raw data as is in the form of tables. Data Engineers have access to this layer. Used for historization and cross verification<br>
**Silver Layer**: Transforming data through data transformation techniques and moving the tables with clean and structured data to the silver layer(normalization). Data Engineers & Data Analysts have access to this layer.<br> 
**Gold Layer**: Creating views as per the business requirements by combining values from multiple tables(Star Schema). Biz teams have access to the views. <br>
---Using PostgreSQL hosted in Docker containers and querying through CloudSQL. 
