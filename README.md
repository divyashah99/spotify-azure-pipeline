# Spotify Azure Pipeline

A comprehensive, enterprise-grade data pipeline for processing Spotify streaming data using **Azure Data Factory (ADF)** for incremental ingestion and **Databricks Asset Bundle (DAB)** for data transformation, modeling, and analytics.

---

## Overview

This project implements a **modern Azure Data Lakehouse architecture** that enables scalable ingestion, transformation, and analytics of Spotify-like streaming data.  
It integrates **Azure Data Factory**, **Databricks**, and **Delta Lake** to handle incremental data loads, ensure high performance, and maintain strong data governance.

### Key Features
- **Metadata-driven ingestion** with dynamic pipeline parameterization  
- **Incremental loading** using Change Data Capture (CDC) tracking  
- **Schema evolution** handling for semi-structured data  
- **Automated CI/CD** deployment using Databricks Asset Bundles  
- **Unity Catalog** for centralized data governance  
- **SCD Type 1 & 2** implementation for dimension management  
- **Data quality enforcement** using DLT expectations  

---

## Architecture
```
Azure SQL Database → Azure Data Factory → Azure Data Lake (Bronze)
                                                ↓
                              Databricks Autoloader → Delta Tables (Silver)
                                                ↓
                            Delta Live Tables → SCD + Analytics Tables (Gold)
                                                ↓
                                    Power BI / Analytics
```

### Layers

| Layer | Purpose | Technology |
|-------|----------|-------------|
| **Bronze** | Raw data ingestion | ADF, ADLS Gen2 |
| **Silver** | Data cleansing, transformation | Databricks Autoloader, Spark |
| **Gold** | Curated SCD dimensions and facts | Delta Live Tables, Delta Lake |

---

## Components

### Azure Data Factory (ADF)

#### Linked Services
- **azure_sql** → Azure SQL Database  
  Example: `azureserverdivya.database.windows.net/azureprojectdbdivya`
- **datalake** → Azure Data Lake Storage Gen2  
  Example: `https://storageaccdivy.dfs.core.windows.net/`

#### Datasets
- **azure_sql** → Source system tables  
- **json_dynamic** → CDC checkpoint tracking  
- **parquet_dynamic** → Output dataset for ingestion results  

#### Pipeline: incremental_loop
- Executes multi-table ingestion dynamically using `Lookup` + `ForEach`
- Iterates through configuration file stored in ADLS or ADF variable
- Implements CDC-based incremental logic for each table
- Parameters: `schema`, `table`, `cdc_col`, `from_date`
- Reads data from Azure SQL → writes to ADLS (Parquet)
- Updates CDC JSON checkpoint after each table ingestion
- Supports **backfill** and **replay** modes for reprocessing older data

#### CDC Mechanism
- Tracks last processed timestamp in `cdc.json` per table
- Example:
```json
  { "cdc": "2025-10-19T23:45:00Z" }
```
- Ensures idempotent ingestion (no duplicate records)

#### Monitoring & Error Handling
- ADF Alerts integrated via Azure Monitor and Logic Apps
- Custom email notifications for failed activities
- Pipeline run history and CDC logs stored in `monitoring/` folder in ADLS

---

### Databricks Asset Bundle (DAB)

#### Silver Layer — `src/silver/`
**silver_Dimensions.py**
- Uses Databricks Autoloader for efficient file ingestion
- Cleanses data (deduplication, type casting, quality checks)
- Implements schema evolution and checkpointing for reliability
- Writes optimized Delta Tables for downstream processing

**Transformations**
- **DimUser**: Cleans names, deduplicates users
- **DimArtist**: Drops duplicates, normalizes artist names
- **DimTrack**: Derives duration flags, removes invalid records
- **DimDate**: Generates time-based keys for streaming events
- **FactStream**: Calculates total plays, unique users, playtime metrics

#### Gold Layer — `src/gold/dlt/transformations/`
Implements Delta Live Tables (DLT) for curated and governed analytics data.

**Files**
- **DimUser.py**: SCD Type 2 (tracks user attribute changes over time)
- **DimTrack.py**: SCD Type 2 (tracks track metadata changes)
- **DimDate.py**: Static date dimension
- **FactStream.py**: Fact table with SCD Type 1 merge for latest metrics

**DLT Expectations**
- `expect_or_fail(user_id IS NOT NULL)`
- `expect_or_drop(duration > 0)`
- Enforced data quality rules at load time

#### Utilities
- **transformations.py**: Helper class for dynamic column operations
- **jinja_notebook.py**: Generates dynamic SQL templates for DLT joins
- **common_functions.py**: Reusable Spark UDFs and validation helpers

#### Configuration
- **spotify_dab.job.yml**: Defines Databricks Job workflow, includes Autoloader task → Silver transformation → DLT run
- **spotify_dab.pipeline.yml**: Configures DLT cluster, schema, tables, and refresh policies

---

## Metadata-Driven Design

A central configuration file drives the ingestion logic dynamically:
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

This eliminates hardcoding and makes pipelines reusable across multiple tables.

---

## Prerequisites

- **Azure Subscription** with:
  - Data Factory
  - Data Lake Storage Gen2
  - Azure SQL Database
  - Databricks workspace
- **Python 3.10+**
- **Databricks CLI**
- **UV Package Manager**
- **GitHub** (for CI/CD integration)

---

## Setup and Deployment

### Azure Data Factory

#### Deploy Linked Services
```bash
az datafactory linked-service create --resource-group rg --factory-name spotify-adf --name azure_sql --properties azure_sql.json
```

#### Configure Pipeline Parameters
Example:
```json
{
  "schema": "dbo",
  "table": "DimUser",
  "cdc_col": "updated_at"
}
```

#### Publish Factory
Use ADF Studio or ARM templates to publish pipelines.

---

### Databricks Asset Bundle

#### Install Dependencies
```bash
uv sync --dev
```

#### Configure Bundle
Update `databricks.yml`:
```yaml
workspace:
  host: https://adb-xxxxxxxxx.azuredatabricks.net
targets:
  dev:
    default_cluster:
      node_type_id: Standard_DS3_v2
      num_workers: 2
  prod:
    default_cluster:
      node_type_id: Standard_DS3_v2
      num_workers: 4
```

#### Deploy
```bash
databricks bundle deploy --target dev
databricks bundle deploy --target prod
```

---

## CI/CD and Automation

- Uses **GitHub Actions** for automatic deployment to Databricks
- Supports multi-environment workflows (dev, staging, prod)
- Bundles manage all dependencies and job configurations
- Version-controlled via GitHub with branch protection and review gates

**Example CI/CD Workflow** (`.github/workflows/deploy.yml`):
```yaml
on: [push]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Sync dependencies
        run: uv sync --dev
      - name: Deploy to Databricks
        run: databricks bundle deploy --target dev
```

---

## Security and Governance

- **Managed Identity** used for secure ADF-to-ADLS and ADF-to-SQL authentication
- **Unity Catalog** manages permissions and table lineage across layers
- Secrets stored in **Azure Key Vault** and referenced in Databricks via `secrets.get()`
- Data encryption at rest (ADLS) and in transit (HTTPS)

---

## Monitoring and Logging

| Component | Monitoring Tool | Description |
|-----------|----------------|-------------|
| **ADF** | Azure Monitor | Pipeline run status, alerts |
| **Databricks** | Job UI, Driver Logs | Job-level logs, DLT run metrics |
| **DLT** | Data Quality UI | Rule compliance and failed records |
| **ADLS** | Storage Insights | File size, ingestion health |

---

## Data Modeling

### Fact Table: FactStream

| Column | Type | Description |
|--------|------|-------------|
| stream_id | INT | Unique stream identifier |
| user_id | INT | Listener ID |
| track_id | INT | Song reference |
| timestamp | DATETIME | Stream event time |
| duration | FLOAT | Playback duration (sec) |
| device | STRING | Streaming device type |

### Dimension Tables

- **DimUser**: User attributes (country, age group)
- **DimArtist**: Artist metadata
- **DimTrack**: Track-level data (genre, popularity)
- **DimDate**: Date attributes (week, month, year)

---

## Local Development
```bash
uv sync --dev
pytest tests/
databricks bundle run spotify_dab_job
```

---

## Performance Optimization

- **Autoloader** for incremental data discovery
- **Z-Ordering** and `OPTIMIZE` commands on Delta Tables
- File compaction for small file management
- Partition pruning for query performance
- Cluster autoscaling based on job load

---

## Contributing

- Follow the project's modular folder structure
- Add unit tests for new transformations
- Document all schema changes in `/docs/data_dictionary.md`
- Use pull requests for all changes to `main`

---

## License

This project is licensed under the **MIT License** — free for educational and commercial use.