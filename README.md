# Sales Data Modeling Project

A comprehensive data modeling solution for sales data processing and warehousing using Snowflake and Python.

## 🛠 Technologies & Tools

### Core Technologies
- **Snowflake** - Cloud Data Warehouse
- **Python 3.8** - Primary programming language
- **PySpark** - Distributed data processing
- **Docker** - Containerization
- **JupyterLab** - Interactive development environment

### Python Libraries
- Snowflake Connector Python
- Snowpark Python
- PyArrow
- Pandas
- Python-dotenv

### Data Formats Supported
- CSV
- Parquet
- JSON

## 🏗 Project Structure

```
sales_data_modeling/
├── data/
│   └── sales/           # Source data files
├── schema/
│   ├── schema.sql       # Main database schema
│   ├── sales_schema.sql # Sales-specific schema
│   └── internal_stage.sql # Staging configuration
├── src/
│   ├── source_IN.py     # India sales processing
│   ├── source_US.py     # US sales processing
│   ├── source_FR.py     # France sales processing
│   ├── data_modelling.py # Core data modeling logic
│   └── uploader.py      # Data upload utilities
├── Dockerfile
└── requirements.txt
```

## 📊 Data Architecture

### Database Schemas
- **source** - Raw data staging
- **curated** - Cleaned and processed data
- **consumption** - Dimensional model
- **audit** - Tracking and monitoring
- **common** - Shared resources

### Dimensional Model
- Region Dimension
- Product Dimension
- Customer Dimension
- Payment Dimension
- Promo Code Dimension
- Sales Fact Table

## 🔄 Data Flow

1. **Data Ingestion**
   - Multiple source formats (CSV, Parquet, JSON)
   - Internal staging in Snowflake
   - Region-specific data processing (IN, US, FR)

2. **Data Processing**
   - Currency conversion
   - Data validation
   - Deduplication
   - Region-specific transformations

3. **Dimensional Modeling**
   - Incremental dimension loading
   - Fact table generation
   - Data quality checks

## 🚀 Getting Started

1. **Environment Setup**
   ```bash
   docker build -t sales_modeling .
   docker run -p 8888:8888 sales_modeling
   ```

2. **Database Setup**
   ```sql
   -- Run schema creation scripts
   source schema/schema.sql
   source schema/sales_schema.sql
   ```

3. **Data Processing**
   ```bash
   python src/uploader.py
   python src/data_modelling.py
   ```

## 🔐 Security Features

- Environment variable management
- Secure Snowflake connection handling
- Role-based access control

## 📈 Performance Features

- Parallel file processing
- Incremental data loading
- Optimized joins and transformations
- Efficient staging strategies

## 🛡 Data Quality Measures

- Input validation
- Duplicate detection
- Currency conversion validation
- Transaction integrity checks

## 📝 Notes

- The project uses Snowflake's internal staging for secure data transfers
- Supports multiple currency conversions
- Implements incremental processing for all dimension tables
- Includes comprehensive error handling and logging

