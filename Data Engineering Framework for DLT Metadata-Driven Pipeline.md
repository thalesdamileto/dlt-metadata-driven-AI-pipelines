# Data Engineering Framework for DLT Metadata-Driven Pipeline

## Objective

The primary objective of this Data Engineering Framework is to facilitate the efficient design, development, and management of data pipelines that leverage Distributed Ledger Technology (DLT) metadata. By implementing a standardized approach, organizations can ensure data integrity, improve data accessibility, and streamline workflows that depend on DLT data.

## Bronze Layer

The Bronze layer is the foundational layer of the data pipeline, where raw data is ingested from various DLT sources. This layer serves as the landing zone for unrefined data, preserving its original state. In this stage, data is collected in its raw format, allowing for subsequent processing and transformations.

### Key Features:

*   Ingestion of raw DLT data
*   Support for various data formats (JSON, XML, etc.)
*   Data validation and integrity checks

## Silver Layer

The Silver layer is responsible for cleaning, enriching, and transforming the raw data received from the Bronze layer. Data processing takes place in this layer, where the aim is to create a reliable and structured dataset that is ready for analysis and consumption. A central characteristic of the Silver layer is the use of **data contracts** defined in JSON files, which guide the ingestion and transformation process, ensuring data quality and compliance.

### Key Features:

*   **Data Cleaning and Deduplication:** Removes duplicate records and inconsistencies, ensuring data uniqueness and integrity.
*   **Data Transformation to Structured Formats:** Converts raw data into more organized formats ready for analysis, applying business rules and data typing.
*   **Metadata Integration for Enhanced Context:** Uses metadata to enrich data, providing additional information about its origin, structure, and processing.
*   **Data Contracts (JSON):** Ingestion and transformation are guided by JSON files that specify:
    *   **`contract_id`**: Unique contract identifier.
    *   **`data_producer`**: Responsible for the data source.
    *   **`data_consumers`**: List of data consumers, with their purposes and priorities.
    *   **`parameters`**: Source and destination data configurations (catalog, schema, table).
    *   **`pk_columns`**: Columns that form the primary key for unique record identification.
    *   **`watermark_column`**: Column used for incremental processing control.
    *   **`ordering_column`**: Column for ordering data, important for deduplication and updates.
    *   **`column_mapping`**: Mapping and transformation of columns between source and destination, including renaming and type changes.
    *   **`quality_procedures`**: Data quality procedures to be applied to the data.

## Gold Layer

The Gold layer represents the final stage of the data pipeline, where high-quality, curated datasets are produced for business intelligence and analytics purposes. This layer is designed for optimal performance and accessibility, enabling stakeholders to derive insights and make data-driven decisions.

### Key Features:

*   Aggregation of data for reporting
*   High-performance queries and optimization
*   Secure and governed access to data

### Initial scripts to run on Databricks

```sql
create catalog if not exists bronze  
create catalog if not exists silver  
create catalog if not exists gold

create table workspace.default.pipelines_watermark ( destination_table string, contract_id string, watermark_column string, watermark_value timestamp )
```
