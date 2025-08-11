# DDL Migration-Conversion Tool

Real-time database schema migration tool that automatically detects and replicates DDL changes across multiple database platforms.

## 🏗️ Architecture Overview

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SOURCE    │    │     MONITOR      │    │   CONVERTER     │    │  DESTINATION    │
│ DATABASES   │───▶│   & DETECT       │───▶│   & PROCESS     │───▶│   DATABASES     │
│             │    │                  │    │                 │    │                 │
│ • MySQL     │    │ • Change         │    │ • Syntax        │    │ • MySQL         │
│ • PostgreSQL│    │   Detection      │    │   Translation   │    │ • PostgreSQL    │
│ • Snowflake │    │ • DDL Extraction │    │ • Schema Sync   │    │ • Snowflake     │
└─────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🔧 Low-Level System Flow

```
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                                    main.py (CLI)                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│  │ Args: -s [mysql,postgres,snowflake] -d [mysql,postgres,snowflake] -i interval   │  │
│  └─────────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────┬─────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                          DDLTransferManager.run()                                     │
│ ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│ │                        Continuous Loop (every N seconds)                        │   │
│ │                                                                                 │   │
│ │  1. get_current_ddl() ──────▶ Extract DDL from all sources                      │   │
│ │  2. detect_changes() ────────▶ Compare with previous state                      │   │
│ │  3. process_changes() ───────▶ Apply changes to destinations                    │   │
│ │                                                                                 │   │
│ └─────────────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────┬─────────────────────────────────────────────────────┘
                                  │
                 ┌────────────────┼────────────────┐
                 ▼                ▼                ▼
        ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
        │  EXTRACT    │   │   DETECT    │   │  PROCESS    │
        │             │   │             │   │             │
        │ ┌─────────┐ │   │ ┌─────────┐ │   │ ┌─────────┐ │
        │ │ MySQL   │ │   │ │ Compare │ │   │ │ MySQL   │ │
        │ │Extractor│ │   │ │ Current │ │   │ │Importer │ │
        │ └─────────┘ │   │ │   vs    │ │   │ └─────────┘ │
        │ ┌─────────┐ │   │ │Previous │ │   │ ┌─────────┐ │
        │ │Postgres │ │   │ │  DDL    │ │   │ │Postgres │ │
        │ │Extractor│ │   │ │ States  │ │   │ │Importer │ │
        │ └─────────┘ │   │ └─────────┘ │   │ └─────────┘ │
        │ ┌─────────┐ │   │      │      │   │ ┌─────────┐ │
        │ │Snowflake│ │   │      ▼      │   │ │Snowflake│ │
        │ │Extractor│ │   │ ┌─────────┐ │   │ │Importer │ │
        │ └─────────┘ │   │ │ Added   │ │   │ └─────────┘ │
        └─────────────┘   │ │Removed  │ │   └─────────────┘
                          │ │Modified │ │           ▲
                          │ │ Tables  │ │           │
                          │ └─────────┘ │    ┌─────────────┐
                          └─────────────┘    │  CONVERT    │
                                             │             │
                                             │ ┌─────────┐ │
                                             │ │ToMySQL  │ │
                                             │ │Converter│ │
                                             │ └─────────┘ │
                                             │ ┌─────────┐ │
                                             │ │ToPostgres││
                                             │ │Converter│ │
                                             │ └─────────┘ │
                                             │ ┌─────────┐ │
                                             │ │ToSnowflake│ 
                                             │ │Converter│ │
                                             │ └─────────┘ │
                                             └─────────────┘

Key Components:
• Extractors: Query SHOW CREATE TABLE, information_schema, or DDL views
• Change Detector: Set difference algorithm to identify added/removed tables  
• Converters: Regex-based syntax translation (INT→INTEGER, backticks→quotes)
• Importers: Execute DROP/CREATE statements with error handling & logging
```

## ✨ Features

### 🔄 Real-time Schema Monitoring
- **Continuous Polling**: Monitors source databases at configurable intervals (default: 5s)
- **Smart Change Detection**: Identifies table additions, modifications, and deletions
- **Multi-Source Support**: Monitor multiple databases simultaneously

### 🔧 Cross-Platform DDL Conversion
- **MySQL ↔ PostgreSQL ↔ Snowflake**: Seamless syntax translation between platforms
- **Type Mapping**: Intelligent data type conversion (INT→INTEGER, DATETIME→TIMESTAMP)
- **Schema Preservation**: Maintains table structures and constraints

### 🛡️ Features
- **Error Recovery**: Graceful handling with continued monitoring on failures
- **Comprehensive Logging**: Detailed logs in `ddl_migration.log`
- **Database Auto-Creation**: Creates destination databases if they don't exist

## 🚀 Quick Start

### Prerequisites
```bash
pip install -r requirements.txt
```

### Configuration
Edit `src/config/db.ini` with your database credentials:
```ini
[mysql]
host = localhost
port = 3306
user = root
password = password
database = source

[postgres]
host = localhost
port = 5432
user = postgres
password = password
database = destination

[snowflake]
host = your-account.snowflakecomputing.com
account = your-account
user = username
password = password
warehouse = COMPUTE_WH
database = DESTINATION
schema = PUBLIC
```

### Usage Examples

**Single Source to Single Destination:**
```bash
python src/main.py -s mysql -d snowflake
```

**Single Source to Multiple Destinations:**
```bash
python src/main.py -s mysql -d snowflake postgres 
```
**Multiple Sources to Single Destination:**
```bash
python src/main.py -s mysql postgres -d snowflake
```
**Custom Monitoring Interval:**
```bash
python src/main.py -s mysql -d postgres -i 30  # Check every 30 seconds
```

## 📁 Project Structure

```
src/
├── main.py                 # CLI entry point
├── detect_transfer.py      # Core migration logic
├── config/
│   └── db.ini             # Database configurations
├── extractors/            # DDL extraction modules
│   ├── mysql_extractor.py
│   ├── postgres_extractor.py
│   └── snowflake_extractor.py
├── converters/            # Syntax conversion modules
│   ├── to_mysql.py
│   ├── to_postgres.py
│   └── to_snowflake.py
└── importers/             # DDL import modules
    ├── mysql_importer.py
    ├── postgres_importer.py
    └── snowflake_importer.py
```

## 🔧 Technical Details

### Supported Operations
- **Table Creation**: New tables are automatically replicated
- **Table Modification**: Schema changes are detected and applied
- **Table Deletion**: Dropped tables are removed from destinations

### Database Support Matrix
| Database   | Extract | Convert To | Import |
|------------|---------|------------|--------|
| MySQL      | ✅      | ✅         | ✅     |
| PostgreSQL | ✅      | ✅         | ✅     |
| Snowflake  | ✅      | ✅         | ✅     |

## 🚦 Example Workflow

1. **Detection**: Tool detects new `users` table in MySQL source
2. **Extraction**: Extracts `CREATE TABLE users (id INT, name VARCHAR(50))` 
3. **Conversion**: Converts to PostgreSQL: `CREATE TABLE users (id INTEGER, name VARCHAR(50))`
4. **Import**: Executes DDL in PostgreSQL destination
5. **Logging**: Records successful migration with timestamp

## 📋 Requirements

- Python 3.7+
- Database connectors: `psycopg2`, `mysql-connector-python`
- Data processing: `pandas`, `sqlalchemy`
- Configuration: `configparser`

## 🎯 Use Cases

- **Database Migration Projects**: Gradual migration between database platforms
- **Multi-Environment Sync**: Keep dev/staging/prod schemas synchronized  
- **Backup Schema Replication**: Maintain schema replicas across different platforms
- **CI/CD Integration**: Automated schema deployment pipelines
