# Spotify Azure Pipeline

A comprehensive data pipeline for processing Spotify streaming data using Azure Data Factory (ADF) for incremental ingestion and Databricks Asset Bundle (DAB) for data transformation and analytics.

## Overview

This project implements a modern data lakehouse architecture for Spotify data analytics:

- **Bronze Layer**: Raw data ingestion from Azure SQL Database to Azure Data Lake Storage (ADLS) in Parquet format
- **Silver Layer**: Data cleansing, deduplication, and transformation using Databricks Autoloader
- **Gold Layer**: Slowly Changing Dimensions (SCD) and analytics-ready tables using Delta Live Tables (DLT)

The pipeline handles incremental data loads with Change Data Capture (CDC) tracking to ensure efficient processing of only new or changed records.

## Architecture

```
Azure SQL Database → Azure Data Factory → Azure Data Lake (Bronze)
                                      ↓
Databricks Autoloader → Delta Tables (Silver)
                                      ↓
Delta Live Tables → SCD Tables (Gold)
```

## Components

### Azure Data Factory (ADF)

#### Linked Services
- **azure_sql**: Connection to Azure SQL Database (`azureserverdivya.database.windows.net/azureprojectdbdivya`)
- **datalake**: Connection to Azure Data Lake Storage (`https://storageaccdivy.dfs.core.windows.net/`)

#### Datasets
- **azure_sql**: References the Azure SQL table
- **json_dynamic**: Dynamic JSON dataset for CDC tracking (container/folder/file parameters)
- **parquet_dynamic**: Dynamic Parquet dataset for data output (container/folder/file parameters)

#### Pipelines
- **incremental_ingestion**: Single-table incremental pipeline with CDC tracking
- **incremental_loop**: Multi-table pipeline that processes multiple tables in a loop

### Databricks Asset Bundle (DAB)

#### Silver Layer (`src/silver/`)
- **silver_Dimensions.py**: Autoloader notebooks for transforming bronze Parquet files to silver Delta tables
  - DimUser: Uppercase user names, drop duplicates
  - DimArtist: Drop duplicates
  - DimTrack: Add duration flags, clean track names
  - DimDate: Basic transformation
  - FactStream: Basic transformation

#### Gold Layer (`src/gold/dlt/transformations/`)
- **DimUser.py**: SCD Type 2 dimension with expectations (user_id not null)
- **DimTrack.py**: SCD Type 2 dimension
- **DimDate.py**: SCD Type 2 dimension
- **FactStream.py**: SCD Type 1 fact table

#### Utilities
- **transformations.py**: Reusable utility class for dropping columns
- **jinja_notebook.py**: Dynamic SQL generation using Jinja2 templates for joins

#### Resources
- **spotify_dab.job.yml**: Job definition with notebook task, pipeline refresh, and main Python task
- **spotify_dab.pipeline.yml**: DLT pipeline configuration

## Prerequisites

- Azure subscription with:
  - Azure Data Factory
  - Azure SQL Database
  - Azure Data Lake Storage Gen2
  - Databricks workspace
- Databricks CLI installed
- Python 3.10-3.13
- UV package manager

## Setup and Deployment

### Azure Data Factory

1. **Import ADF Resources**:
   ```bash
   # Publish the factory configuration
   # Use ADF UI or ARM templates to deploy linked services, datasets, and pipelines
   ```

2. **Configure Linked Services**:
   - Update Azure SQL credentials in `linkedService/azure_sql.json`
   - Update Data Lake connection in `linkedService/datalake.json`

3. **Pipeline Parameters**:
   - `schema`: Database schema (e.g., "dbo")
   - `table`: Table name (e.g., "DimUser")
   - `cdc_col`: CDC column (e.g., "updated_at")
   - `from_date`: Optional start date for initial load

### Databricks Asset Bundle

1. **Install Dependencies**:
   ```bash
   uv sync --dev
   ```

2. **Configure Bundle**:
   - Update `databricks.yml` with your workspace details
   - Modify targets for dev/prod environments

3. **Deploy Bundle**:
   ```bash
   # Deploy to development
   databricks bundle deploy --target dev

   # Deploy to production
   databricks bundle deploy --target prod
   ```

## Usage

### Running ADF Pipelines

#### Single Table Ingestion
Trigger the `incremental_ingestion` pipeline with parameters:
- schema: "dbo"
- table: "DimUser"
- cdc_col: "updated_at"
- from_date: "" (empty for incremental)

#### Multi-Table Ingestion
The `incremental_loop` pipeline processes multiple tables defined in the `loop_input` parameter:
```json
[
  {
    "schema": "dbo",
    "table": "DimUser",
    "cdc_col": "updated_at",
    "from_date": ""
  },
  {
    "schema": "dbo",
    "table": "DimTrack",
    "cdc_col": "updated_at",
    "from_date": ""
  }
]
```

### Running Databricks Jobs

```bash
# Run the bundle job
databricks bundle run

# Run specific job
databricks bundle run spotify_dab_job
```

## Data Flow

1. **Ingestion (ADF)**:
   - Query Azure SQL for records where CDC column > last processed value
   - Write to ADLS bronze layer in Parquet format
   - Update CDC tracking file

2. **Silver Transformation (Databricks)**:
   - Autoloader reads bronze Parquet files
   - Applies cleansing rules (deduplication, data quality)
   - Writes to Delta tables in silver layer

3. **Gold Analytics (DLT)**:
   - Reads from silver Delta tables
   - Applies SCD logic for dimensions
   - Creates analytics-ready tables

## CDC Mechanism

The pipeline uses JSON files in the bronze layer to track the last processed CDC value:

- File: `{table}_cdc/cdc.json`
- Contains: `{"cdc": "last_processed_value"}`

For initial loads, set `from_date` parameter or leave empty to process all data.

## Monitoring and Logging

- ADF: Monitor pipeline runs in ADF UI
- Databricks: Monitor jobs and DLT pipelines in Databricks workspace
- Logs: Check Databricks driver logs for transformation details

## Development

### Local Development
```bash
# Install dev dependencies
uv sync --dev

# Run tests
uv run pytest

# Use databricks-connect for local testing
```

### Adding New Tables
1. Add table configuration to `incremental_loop` pipeline parameters
2. Create silver transformation logic in `silver_Dimensions.py`
3. Create gold DLT transformation in `src/gold/dlt/transformations/`

## Security

- Azure SQL credentials are encrypted in linked service
- Data Lake access uses managed identity or service principal
- Databricks workspace access controlled via permissions

## Contributing

1. Follow the existing code structure
2. Add tests for new transformations
3. Update documentation for changes
4. Deploy to dev environment for testing

