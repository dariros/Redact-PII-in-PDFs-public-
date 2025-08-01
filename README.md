# PII Redaction Solution - Implementation Guide

This comprehensive guide will walk you through implementing a complete PII redaction solution for PDF files using Snowflake Cortex AI, from initial setup to production deployment.

## 📁 Project Overview

### What This Solution Does
- **Automatically detects** personally identifiable information (PII) in PDF documents
- **Visually redacts** PII by overlaying black rectangles over sensitive content
- **Processes files in batch** using Snowflake's `EXECUTE NOTEBOOK` functionality
- **Provides comprehensive tracking** with detailed audit logs and monitoring
- **Handles complex filenames** including spaces and special characters

### 📄 **PDF Compatibility Requirements**

**✅ Works with**: Machine-readable PDFs (text-based documents, properly formatted documents)

**⚠️ Limitations**: While `snowflake.cortex.parse_document` can extract text from non-machine readable PDFs (image-based scans), **PyMuPDF cannot redact** these types of documents. The redaction process requires machine-readable text that PyMuPDF can locate and overlay with black rectangles.

**Recommended**: Ensure your PDFs are machine-readable or have been processed with OCR before using this solution for optimal redaction results.

### Files Included
- **`Interactive_PII_Redaction_Demo.ipynb`** - Interactive Streamlit version for manual processing and testing
- **`flexible_batch_pii_redaction.ipynb`** - Flexible batch processing notebook for both single files and bulk processing
- **`README.md`** - This implementation guide

## 🏗️ Architecture Overview

### Interactive Notebook (`Interactive_PII_Redaction_Demo.ipynb`)
- **Purpose**: Manual, interactive PII redaction with Streamlit UI
- **Use Case**: Testing, demonstration, single-file processing with visual feedback
- **Features**: Real-time preview, manual review of redactions, immediate feedback

### Flexible Batch Processing Notebook (`flexible_batch_pii_redaction.ipynb`)
- **Purpose**: Flexible automated processing for both single files and bulk operations
- **Use Case**: Production workflows, processing individual files or entire stages
- **Features**: 
  - **2 Parameters**: Process ALL PDF files in a stage automatically
  - **3+ Parameters**: Process a specific file (handles filenames with spaces)
  - Parameter-driven execution, comprehensive logging, error handling, tracking table
  - Built-in bulk processing eliminates need for stored procedures

### Processing Flow
```
Input PDF → Snowflake Stage → Cortex AI Text Extraction → 
Claude 3.5 Sonnet PII Detection → PyMuPDF Visual Redaction → 
Output PDF → Snowflake Stage → Audit Log
```

**⚠️ Important**: This flow requires **machine-readable PDFs**. While Cortex AI can extract text from image-based PDFs, PyMuPDF cannot apply visual redactions to non-machine readable documents.

## ✅ Prerequisites

Before starting, ensure you have:

### Snowflake Requirements
- Snowflake account with **Cortex AI enabled**
- Role with **ACCOUNTADMIN** privileges (for initial setup)
- Access to create databases, schemas, stages, and integrations
- **External Access Integration** capability enabled on your account
- **Container Runtime access** (automatically available - no additional setup required)

### Permissions Needed
- Create and manage databases/schemas
- Create and use external access integrations
- Execute notebooks and create stored procedures
- Read/write access to stages
- Use Cortex AI functions (`PARSE_DOCUMENT`, `AI_COMPLETE`)
- **USAGE privilege on Container Runtime compute pools**

### ⚠️ Critical RBAC Restrictions for Container Runtime

**Important**: Users with `ACCOUNTADMIN`, `ORGADMIN`, or `SECURITYADMIN` roles **cannot directly create or own notebooks** on Container Runtime. Notebooks created or directly owned by these roles will fail to run.

**Workaround**: If a notebook is owned by a role that these administrative roles inherit privileges from (such as the `PUBLIC` role), then you can use those roles to run the notebook.

**Recommendation**: Use a dedicated data science or development role (not administrative roles) for creating and owning Container Runtime notebooks.

---

## 🚀 Step-by-Step Implementation

## Step 1: Snowflake Environment Setup

### 1.1 Create Database and Schema

```sql
-- Create the database
CREATE DATABASE IF NOT EXISTS ADVANCED_ANALYTICS
COMMENT = 'Database for advanced analytics workloads';

-- Create the schema for PII redaction demo
CREATE SCHEMA IF NOT EXISTS ADVANCED_ANALYTICS.REDACT_PDF_DEMO
COMMENT = 'Schema for PII redaction demonstration';

-- Switch to the correct context
USE DATABASE ADVANCED_ANALYTICS;
USE SCHEMA REDACT_PDF_DEMO;
```

### 1.2 Create Required Stages

```sql
-- Create stage for original PDF files
CREATE STAGE IF NOT EXISTS ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS
DIRECTORY = ( ENABLE = true ) 
ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
COMMENT = 'Stage for original PDF files to be processed';

-- Create stage for redacted PDF files
CREATE STAGE IF NOT EXISTS ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS
DIRECTORY = ( ENABLE = true ) 
ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
COMMENT = 'Stage for redacted PDF files after processing';

-- Refresh stages to ensure they're ready
ALTER STAGE ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS REFRESH;
ALTER STAGE ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS REFRESH;
```

### 1.3 Set Up Role Permissions

```sql
-- Replace <your_role> with your actual role name
USE ROLE ACCOUNTADMIN;

-- Grant usage on database and schema
GRANT USAGE ON DATABASE ADVANCED_ANALYTICS TO ROLE <your_role>;
GRANT USAGE ON SCHEMA ADVANCED_ANALYTICS.REDACT_PDF_DEMO TO ROLE <your_role>;

-- Grant stage permissions
GRANT READ, WRITE ON STAGE ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS TO ROLE <your_role>;
GRANT READ, WRITE ON STAGE ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS TO ROLE <your_role>;

-- Grant table creation permissions for tracking table
GRANT CREATE TABLE ON SCHEMA ADVANCED_ANALYTICS.REDACT_PDF_DEMO TO ROLE <your_role>;

-- Grant Cortex AI permissions
GRANT USAGE ON FUNCTION snowflake.cortex.parse_document TO ROLE <your_role>;
GRANT USAGE ON FUNCTION AI_COMPLETE TO ROLE <your_role>;

-- Container Runtime specific permissions
-- Grant USAGE on system compute pools for Container Runtime notebooks
GRANT USAGE ON COMPUTE POOL SYSTEM_COMPUTE_POOL_CPU TO ROLE <your_role>;

-- Switch back to your working role
USE ROLE <your_role>;
```

### 1.4 Verify Environment Setup

```sql
-- Verify database and schema exist
USE DATABASE ADVANCED_ANALYTICS;
USE SCHEMA REDACT_PDF_DEMO;

-- Check that stages exist and are accessible
DESCRIBE STAGE ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS;
DESCRIBE STAGE ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS;

-- Test Cortex AI access
SELECT AI_COMPLETE(
    model => 'claude-3-5-sonnet', 
    prompt => 'Hello, this is a test. Please respond with "Cortex AI is working"'
) as test_result;

-- Verify Container Runtime compute pool access
SHOW GRANTS TO ROLE <your_role>;
-- Look for: USAGE on COMPUTE POOL SYSTEM_COMPUTE_POOL_CPU

-- Verify compute pool exists and is accessible
SHOW COMPUTE POOLS;
```

## Step 2: External Access Integration Setup

### 2.1 Create External Access Integration (ACCOUNTADMIN Required)

```sql
USE ROLE ACCOUNTADMIN;

-- Create network rule for Python package repositories
CREATE OR REPLACE NETWORK RULE pypi_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('pypi.org', 'pypi.python.org', 'pythonhosted.org', 'files.pythonhosted.org');

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION pypi_access_integration
ALLOWED_NETWORK_RULES = (pypi_network_rule)
ENABLED = true;

-- Grant usage to your role
GRANT USAGE ON INTEGRATION pypi_access_integration TO ROLE <your_role>;

-- Switch back to your working role
USE ROLE <your_role>;
```

### 2.2 Verify Integration Setup

```sql
-- Verify External Access Integration
SHOW INTEGRATIONS LIKE '%ACCESS%';

-- Check your permissions
SHOW GRANTS TO ROLE <your_role>;
```

## Step 3: Upload and Deploy Notebooks

### 3.1 Create Notebooks in Snowflake

1. **Access Snowflake Notebooks**:
   - Log into Snowflake Snowsight
   - Navigate to **Projects** → **Notebooks**
   - Ensure you're in the correct database/schema context

2. **Upload Interactive Notebook**:
   - Click **"+ Notebook"** → **"Import .ipynb file"**
   - Upload `Interactive_PII_Redaction_Demo.ipynb`
   - Name it: `Interactive_PII_Redaction_Demo`
   - Set database: `ADVANCED_ANALYTICS`
   - Set schema: `REDACT_PDF_DEMO`
   - **Important**: Configure runtime settings (see section 3.2 below)

3. **Upload Flexible Batch Processing Notebook**:
   - Click **"+ Notebook"** → **"Import .ipynb file"**
   - Upload `flexible_batch_pii_redaction.ipynb`
   - Name it: `flexible_batch_pii_redaction`
   - Set database: `ADVANCED_ANALYTICS`
   - Set schema: `REDACT_PDF_DEMO`
   - **Important**: Configure runtime settings (see section 3.2 below)

### 3.2 Configure Notebook Settings

**⚠️ CRITICAL: Both notebooks require Container Runtime and External Access Integration for proper functionality.**

For **both notebooks** (`Interactive_PII_Redaction_Demo` and `flexible_batch_pii_redaction`), configure the following settings during notebook creation or via notebook settings:

#### Step-by-Step Runtime Configuration:

1. **Select Runtime Type**:
   - Choose **"Run on container"** (instead of "Run on warehouse")
   - This enables [Container Runtime for ML](https://docs.snowflake.com/en/developer-guide/snowflake-ml/notebooks-on-spcs) powered by Snowpark Container Services

2. **Select Runtime Version**:
   - Choose **CPU** runtime version (sufficient for PDF processing and AI inference)
   - GPU runtime is available but not required for this solution

3. **Select Compute Pool**:
   - Choose **`SYSTEM_COMPUTE_POOL_CPU`** (default system-provided CPU compute pool)
   - This is automatically provisioned by Snowflake for running notebooks on Container Runtime
   - Provides flexible container infrastructure for installing external packages

#### Required Integration Settings (MANDATORY):
- **🚨 External Access Integration**: Select `pypi_access_integration` 
  - **CRITICAL**: This is **MANDATORY** for both notebooks to function
  - Required for installing Python packages (`pypdfium2`, `PyMuPDF`) from PyPI
  - **Without this, the notebooks will fail immediately** with `ModuleNotFoundError`
  - Must be configured in notebook settings → External Access Integration → Select `pypi_access_integration`
  - As per [Snowflake documentation](https://docs.snowflake.com/en/developer-guide/snowflake-ml/notebooks-on-spcs), EAI is required for external package installation

#### Database Context:
- **Database**: `ADVANCED_ANALYTICS`
- **Schema**: `REDACT_PDF_DEMO`

#### Why Container Runtime is Required:
- **Package Installation**: Container Runtime allows installing packages from multiple sources using `!pip install`
- **Flexible Environment**: More flexible than virtual warehouses for data science workloads
- **Pre-installed Packages**: Comes with base Python packages verified by Snowflake
- **External Dependencies**: Required for `pypdfium2` and `PyMuPDF` package installation

## Step 4: Upload Test Files and Initial Testing

### 4.1 Upload Sample PDF Files

Upload test PDF files to your input stage. You can do this via:

**Option A: Snowflake Web UI**
1. Navigate to **Data** → **Databases** → **ADVANCED_ANALYTICS** → **REDACT_PDF_DEMO**
2. Click on **ORIG_PDFS** stage
3. Use **"Upload Files"** button to select and upload PDF files

**Option B: SnowSQL Command Line**
```bash
# Upload a single file
snowsql -c your_connection -q "PUT file://path/to/document.pdf @ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS AUTO_COMPRESS=FALSE"

# Upload multiple files
snowsql -c your_connection -q "PUT file://path/to/pdfs/* @ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS AUTO_COMPRESS=FALSE"
```

### 4.2 Verify File Upload

```sql
-- List uploaded files
LIST @ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS;

-- Check directory table
SELECT * FROM DIRECTORY(@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS) 
WHERE RELATIVE_PATH ILIKE '%.pdf';
```

### 4.3 Test Interactive Notebook

1. Open the `Interactive_PII_Redaction_Demo` notebook
2. Run all cells to start the Streamlit application
3. Test with one of your uploaded files
4. Verify that:
   - File can be selected and loaded
   - Text is extracted properly
   - PII is detected and highlighted
   - Redacted PDF is generated

### 4.4 Test Flexible Batch Processing Notebook

Test both processing modes of the flexible notebook:

```sql
-- Test Mode 1: Process a specific file (3 parameters)
EXECUTE NOTEBOOK ADVANCED_ANALYTICS.REDACT_PDF_DEMO.flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS',
    'your_test_file.pdf'
);

-- Test Mode 2: Process ALL PDF files in stage (2 parameters)
EXECUTE NOTEBOOK ADVANCED_ANALYTICS.REDACT_PDF_DEMO.flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS'
);
```

## Step 5: Flexible Batch Processing Implementation

### 5.1 Understanding the Flexible Parameter System

The flexible batch notebook automatically detects processing mode based on the number of parameters:

#### 📋 **Two Processing Modes**

**Mode 1: Bulk Processing (2 Parameters)**
- **Parameter 1**: `input_stage` 
- **Parameter 2**: `output_stage`
- **Behavior**: Automatically processes **ALL PDF files** in the input stage

**Mode 2: Single File Processing (3+ Parameters)**
- **Parameter 1**: `input_stage`
- **Parameter 2**: `output_stage` 
- **Parameter 3+**: `filename` (automatically handles spaces by joining all remaining parameters)
- **Behavior**: Processes **one specific file**

#### 📝 **Parameter Examples**

```sql
-- BULK MODE: Process all PDF files (2 parameters)
EXECUTE NOTEBOOK flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS'
);

-- SINGLE FILE MODE: Simple filename (3 parameters)
EXECUTE NOTEBOOK flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS',
    'document.pdf'
);

-- SINGLE FILE MODE: Filename with spaces (4+ parameters - automatically joined)
EXECUTE NOTEBOOK flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS',
    'my important document.pdf'
);
```

### 5.2 Single File Processing

Process individual files using **3+ parameters** (input_stage, output_stage, filename):

```sql
-- Process specific files without spaces
EXECUTE NOTEBOOK flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS',
    'contract_2024.pdf'
);

-- Process files with spaces in filename (automatically handled)
EXECUTE NOTEBOOK flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS',
    'employee resume march 2024.pdf'
);

-- Process files with complex names
EXECUTE NOTEBOOK flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS',
    'Q4-2024 Financial Report (Final Version).pdf'
);
```

### 5.3 Bulk Processing (All Files)

**🎆 NEW: No stored procedures needed!** The flexible notebook handles bulk processing natively.

Simply use **2 parameters** (input_stage, output_stage) to process ALL PDF files:

```sql
-- Process ALL PDF files in the input stage automatically
EXECUTE NOTEBOOK flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS'
);
```

#### 📊 **What Happens in Bulk Mode:**

1. **Auto-Discovery**: Notebook automatically finds all PDF files in the input stage
2. **Sequential Processing**: Processes each file one by one with progress tracking
3. **Error Resilience**: Continues processing other files if one fails
4. **Comprehensive Reporting**: Shows detailed success/failure summary
5. **Full Logging**: Each file gets tracked in `PII_REDACTION_LOG` table

#### 📝 **Bulk Processing Output Example:**

```
🚀 MODE: Processing ALL PDF files in stage
📂 Input Stage: @ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS
📂 Output Stage: @ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS

📊 Found 5 PDF files to process

============================================================
📄 Processing file 1/5: 'contract_2024.pdf'
============================================================
✅ Successfully processed: contract_2024.pdf

============================================================
📄 Processing file 2/5: 'employee resume march 2024.pdf'
============================================================
✅ Successfully processed: employee resume march 2024.pdf

============================================================
📊 BULK PROCESSING SUMMARY
============================================================
Total files found: 5
Successfully processed: 5
Failed: 0
Success rate: 100.0%
Total processing time: 45.32 seconds
```

#### 🔄 **For Large-Scale Operations:**

For processing hundreds of files, you can still create a simple wrapper if needed:

```sql
-- Optional: Create a simple procedure wrapper for scheduling
CREATE OR REPLACE PROCEDURE PROCESS_ALL_PDFS_WRAPPER()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    EXECUTE NOTEBOOK flexible_batch_pii_redaction(
        '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
        '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS'
    );
    RETURN 'Bulk processing completed. Check PII_REDACTION_LOG for details.';
END;
$$;
```

## Step 6: Monitoring and Tracking System

### 6.1 Understanding the Tracking Table

The batch notebook automatically creates a `PII_REDACTION_LOG` table that tracks:
- **Processing status**: SUBMITTED → COMPLETED/FAILED
- **Detailed metrics**: text length, PII found, redactions made
- **Performance data**: processing time, timestamps
- **Error logging**: detailed error messages and warnings

### 6.2 Monitor Processing Status

```sql
-- View recent processing attempts
SELECT 
    INPUT_FILE, 
    STATUS, 
    TIMESTAMP, 
    TEXT_LENGTH,
    PII_FOUND_COUNT,
    REDACTIONS_MADE,
    PROCESSING_TIME_SECONDS,
    ERROR_MESSAGE 
FROM PII_REDACTION_LOG 
ORDER BY TIMESTAMP DESC 
LIMIT 10;

-- Check success rate
SELECT 
    COUNT(*) as TOTAL_ATTEMPTS,
    SUM(CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 0 END) as SUCCESSFUL,
    SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) as FAILED,
    ROUND(100.0 * SUM(CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2) as SUCCESS_RATE_PCT
FROM PII_REDACTION_LOG;
```

### 6.3 Verify Output Files

```sql
-- List all redacted files
LIST @ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS;

-- Compare input vs output files
WITH input_files AS (
    SELECT RELATIVE_PATH as INPUT_FILE 
    FROM DIRECTORY(@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS)
    WHERE RELATIVE_PATH ILIKE '%.pdf'
),
output_files AS (
    SELECT RELATIVE_PATH as OUTPUT_FILE,
           REGEXP_REPLACE(RELATIVE_PATH, '^redacted_', '') as ORIGINAL_FILE
    FROM DIRECTORY(@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS)
    WHERE RELATIVE_PATH ILIKE 'redacted_%.pdf'
)
SELECT 
    i.INPUT_FILE,
    o.OUTPUT_FILE,
    CASE WHEN o.OUTPUT_FILE IS NOT NULL THEN '✅ Processed' ELSE '❌ Missing' END as STATUS
FROM input_files i
LEFT JOIN output_files o ON i.INPUT_FILE = o.ORIGINAL_FILE
ORDER BY i.INPUT_FILE;
```

### 6.4 Download Processed Files

Files can be downloaded either through the Snowflake UI or using SQL commands:

#### Option A: Download via Snowflake UI
1. Navigate to **Data** → **Databases** → **ADVANCED_ANALYTICS** → **REDACT_PDF_DEMO**
2. Click on **REDACTED_PDFS** stage
3. Select the file(s) you want to download
4. Click the "Download" button

#### Option B: Download via SQL
```sql
-- Download specific redacted file
GET @ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS/redacted_your_file.pdf 
file:///path/to/local/directory/;

-- Download all redacted files
GET @ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS/*.pdf 
file:///path/to/local/directory/;

```

## Step 7: Production Deployment

### 7.1 Automated Processing Task (Optional)

For regular bulk processing, create a scheduled task using the flexible notebook:

```sql
-- Create a task to process new PDFs automatically using the flexible notebook
CREATE OR REPLACE TASK PROCESS_NEW_PDFS
WAREHOUSE = 'COMPUTE_WH'  -- Adjust warehouse name as needed
SCHEDULE = 'USING CRON 0 */4 * * * America/Los_Angeles'  -- Every 4 hours
AS
BEGIN
    -- Use flexible notebook in bulk mode (2 parameters = process all files)
    EXECUTE NOTEBOOK flexible_batch_pii_redaction(
        '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
        '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS'
    );
END;

-- Start the task
ALTER TASK PROCESS_NEW_PDFS RESUME;

-- Monitor task execution
SELECT * FROM INFORMATION_SCHEMA.TASK_HISTORY 
WHERE TASK_NAME = 'PROCESS_NEW_PDFS'
ORDER BY SCHEDULED_TIME DESC 
LIMIT 10;

-- Alternative: Create a wrapper procedure for more complex scheduling
CREATE OR REPLACE PROCEDURE SCHEDULED_BULK_PROCESSING()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Log start of scheduled processing
    INSERT INTO PII_REDACTION_LOG (INPUT_STAGE, INPUT_FILE, OUTPUT_STAGE, STATUS, ERROR_MESSAGE)
    VALUES ('@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS', 'SCHEDULED_BULK_RUN', 
            '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS', 'SUBMITTED', 
            'Scheduled bulk processing started at ' || CURRENT_TIMESTAMP()::STRING);
    
    -- Execute the flexible notebook in bulk mode
    EXECUTE NOTEBOOK flexible_batch_pii_redaction(
        '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
        '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS'
    );
    
    RETURN 'Scheduled bulk processing completed. Check PII_REDACTION_LOG for detailed results.';
END;
$$;
```

### 7.2 Production Monitoring Dashboard

Set up comprehensive monitoring:

```sql
-- Create view for production dashboard
CREATE OR REPLACE VIEW PII_PROCESSING_DASHBOARD AS
SELECT 
    DATE(TIMESTAMP) as PROCESSING_DATE,
    COUNT(*) as TOTAL_FILES,
    SUM(CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 0 END) as COMPLETED_FILES,
    SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) as FAILED_FILES,
    ROUND(AVG(PROCESSING_TIME_SECONDS), 2) as AVG_PROCESSING_TIME,
    SUM(PII_FOUND_COUNT) as TOTAL_PII_DETECTED,
    SUM(REDACTIONS_MADE) as TOTAL_REDACTIONS
FROM PII_REDACTION_LOG
GROUP BY DATE(TIMESTAMP)
ORDER BY PROCESSING_DATE DESC;

-- View dashboard
SELECT * FROM PII_PROCESSING_DASHBOARD;
```

### 7.3 Alerting Setup

```sql
-- Create view for failed processing alerts
CREATE OR REPLACE VIEW FAILED_PROCESSING_ALERTS AS
SELECT 
    INPUT_FILE,
    ERROR_MESSAGE,
    TIMESTAMP,
    DATEDIFF(MINUTE, TIMESTAMP, CURRENT_TIMESTAMP()) as MINUTES_AGO
FROM PII_REDACTION_LOG 
WHERE STATUS = 'FAILED' 
AND TIMESTAMP >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC;

-- Check for recent failures
SELECT * FROM FAILED_PROCESSING_ALERTS;
```

---

## 🔧 Troubleshooting Guide

### Common Issues and Solutions

#### 1. **ModuleNotFoundError for pypdfium2 or PyMuPDF**
**Solution**: This error occurs when notebooks are not properly configured for Container Runtime. Ensure:
- **Runtime Type**: Set to **"Run on container"** (not "Run on warehouse")
- **Runtime Version**: Select **CPU** runtime version
- **Compute Pool**: Select **`SYSTEM_COMPUTE_POOL_CPU`** (default system CPU pool)
- **External Access Integration**: Select `pypi_access_integration` in notebook settings
- **Restart notebook**: After changing settings, restart the notebook to apply changes
- **Reference**: See [Container Runtime setup guide](https://docs.snowflake.com/en/developer-guide/snowflake-ml/notebooks-on-spcs)

#### 2. **Compute Pool Access Denied**
**Solution**: This error occurs when the role lacks USAGE privilege on the compute pool:
```sql
-- Verify compute pool permissions
SHOW GRANTS TO ROLE <your_role>;
-- Grant USAGE on compute pool if missing
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON COMPUTE POOL SYSTEM_COMPUTE_POOL_CPU TO ROLE <your_role>;
USE ROLE <your_role>;
```

#### 3. **Notebook Fails to Run with Administrative Roles**
**Symptom**: Notebooks fail when created or owned by ACCOUNTADMIN, ORGADMIN, or SECURITYADMIN roles
**Solution**: 
- Create notebooks using a non-administrative role (e.g., data scientist role)
- Transfer notebook ownership to a non-administrative role
- Ensure the role has proper Container Runtime permissions

#### 4. **Stage Access Denied**
**Solution**: 
```sql
-- Verify stage permissions
SHOW GRANTS ON STAGE ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS;
-- Re-grant if necessary
GRANT READ, WRITE ON STAGE ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS TO ROLE <your_role>;
```

#### 5. **Cortex AI Access Denied**
**Solution**:
```sql
-- Check AI function permissions
SHOW GRANTS TO ROLE <your_role>;
-- Grant if missing
GRANT USAGE ON FUNCTION snowflake.cortex.parse_document TO ROLE <your_role>;
GRANT USAGE ON FUNCTION AI_COMPLETE TO ROLE <your_role>;
```

#### 6. **Text Extraction Returns Minimal Characters**
**Symptom**: Results show "Text Length: 17 characters"
**Cause**: Incorrect stage reference in `parse_document()`
**Solution**: Ensure stages are referenced without quotes: `@STAGE_NAME` not `'@STAGE_NAME'`

#### 7. **Files with Spaces Not Processing**
**Status**: ✅ **Fixed in current version**
**Verification**: Check tracking table for specific error details:
```sql
SELECT INPUT_FILE, ERROR_MESSAGE, TIMESTAMP 
FROM PII_REDACTION_LOG 
WHERE STATUS = 'FAILED' AND INPUT_FILE LIKE '% %'
ORDER BY TIMESTAMP DESC;
```

#### 8. **Parameter Errors**
**Solution**: Ensure proper parameter format for the flexible notebook:
- Stage names include `@` symbol: `'@DATABASE.SCHEMA.STAGE_NAME'`
- **2 Parameters (Bulk Mode)**: input_stage, output_stage
- **3+ Parameters (Single File Mode)**: input_stage, output_stage, filename
- All parameters are strings in single quotes
- **NEW**: Parameter order is input_stage, output_stage, [optional filename]

#### 9. **PDF Redaction Issues - No Redactions Applied**
**Symptom**: Text is extracted successfully, PII is detected, but no visual redactions appear in the output PDF
**Cause**: PDF is not machine-readable (image-based scan without proper OCR)
**Explanation**: 
- `snowflake.cortex.parse_document` can extract text from image-based PDFs using advanced AI
- However, PyMuPDF requires machine-readable text coordinates to place redaction rectangles
- Image-based PDFs lack the text positioning data needed for visual redaction

**Solutions**:
- **Verify PDF type**: Open the PDF and try to select/copy text. If you cannot select text, it's likely image-based
- **Use OCR preprocessing**: Convert image-based PDFs to machine-readable format using OCR tools
- **Check processing logs**: Look for warnings in `PII_REDACTION_LOG` table indicating low redaction counts

```sql
-- Check for potential image-based PDF issues
SELECT 
    INPUT_FILE,
    TEXT_LENGTH,
    PII_FOUND_COUNT,
    REDACTIONS_MADE,
    WARNINGS
FROM PII_REDACTION_LOG 
WHERE STATUS = 'COMPLETED' 
AND PII_FOUND_COUNT > 0 
AND REDACTIONS_MADE = 0
ORDER BY TIMESTAMP DESC;
```

### Diagnostic Commands

```sql
-- Environment health check
SELECT 'Database' as COMPONENT, DATABASE_NAME as NAME, 'OK' as STATUS 
FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = 'ADVANCED_ANALYTICS'
UNION ALL
SELECT 'Schema' as COMPONENT, SCHEMA_NAME as NAME, 'OK' as STATUS 
FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'REDACT_PDF_DEMO'
UNION ALL
SELECT 'Integration' as COMPONENT, NAME, 'OK' as STATUS 
FROM INFORMATION_SCHEMA.INTEGRATIONS WHERE NAME = 'PYPI_ACCESS_INTEGRATION';

-- Container Runtime RBAC verification
SHOW GRANTS TO ROLE <your_role>;
-- Verify you see: USAGE on COMPUTE POOL SYSTEM_COMPUTE_POOL_CPU

-- Check compute pool availability
SHOW COMPUTE POOLS;

-- Verify role is not administrative (should return 0 rows)
SELECT CURRENT_ROLE() as CURRENT_ROLE
WHERE CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ORGADMIN', 'SECURITYADMIN');

-- Recent notebook executions
SELECT * FROM INFORMATION_SCHEMA.NOTEBOOK_EXECUTION_HISTORY 
WHERE NOTEBOOK_NAME = 'flexible_batch_pii_redaction'
ORDER BY START_TIME DESC LIMIT 5;

-- Tracking table health
SELECT 
    COUNT(*) as TOTAL_LOGS,
    MAX(TIMESTAMP) as LAST_PROCESSED,
    COUNT(DISTINCT STATUS) as UNIQUE_STATUSES,
    COUNT(DISTINCT INPUT_FILE) as UNIQUE_FILES
FROM PII_REDACTION_LOG;
```

---

## 📋 Implementation Checklist

Use this checklist to verify your implementation:

### Environment Setup
- [ ] Database `ADVANCED_ANALYTICS` created
- [ ] Schema `REDACT_PDF_DEMO` created
- [ ] Stages `ORIG_PDFS` and `REDACTED_PDFS` created
- [ ] Role permissions granted for all components
- [ ] **Container Runtime compute pool permissions** (`USAGE` on `SYSTEM_COMPUTE_POOL_CPU`)
- [ ] External Access Integration configured
- [ ] Cortex AI functions accessible
- [ ] **Non-administrative role used** for notebook creation (not ACCOUNTADMIN/ORGADMIN/SECURITYADMIN)

### Notebook Deployment
- [ ] Interactive notebook uploaded and functional
- [ ] **Flexible batch processing notebook uploaded and functional**
- [ ] **Container Runtime configured** ("Run on container" + CPU runtime + SYSTEM_COMPUTE_POOL_CPU)
- [ ] **External Access Integration enabled** in notebook settings (`pypi_access_integration`) - **MANDATORY**
- [ ] Test files uploaded to input stage

### Testing and Validation
- [ ] Interactive notebook processes files correctly
- [ ] **Flexible notebook executes in single file mode (3 parameters)**
- [ ] **Flexible notebook executes in bulk mode (2 parameters)**
- [ ] Flexible notebook handles files with spaces
- [ ] Tracking table created and logging properly
- [ ] Output files generated in redacted stage

### Production Readiness
- [ ] Multi-file processing procedure created
- [ ] Monitoring queries validated
- [ ] Error handling verified
- [ ] Documentation reviewed
- [ ] Automated task scheduled (if desired)

### Handoff Documentation
- [ ] All SQL scripts documented
- [ ] Parameter examples provided
- [ ] Troubleshooting guide reviewed
- [ ] Monitoring procedures documented

---

## 🎯 Success Criteria

Your implementation is successful when:

1. **Files Process Successfully**: Both simple filenames and files with spaces process without errors
2. **Tracking Works**: All processing attempts are logged in `PII_REDACTION_LOG` with appropriate status
3. **PII Detection Works**: Files with PII show detected elements and redactions in the output
4. **Monitoring Functions**: You can view processing status, success rates, and download redacted files
5. **Error Handling Works**: Failed processing attempts are logged with meaningful error messages
6. **Scalability Proven**: Multiple files can be processed via bulk mode (2 parameters)
7. **PDF Compatibility Verified**: Machine-readable PDFs show visual redactions; image-based PDFs may show PII detection but zero redactions (expected behavior)

**Final Validation Tests**:
```sql
-- Test 1: Process a single file with spaces in the name (3 parameters)
EXECUTE NOTEBOOK flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS',
    'test document with spaces.pdf'
);

-- Test 2: Process all files in bulk mode (2 parameters)
EXECUTE NOTEBOOK flexible_batch_pii_redaction(
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.ORIG_PDFS',
    '@ADVANCED_ANALYTICS.REDACT_PDF_DEMO.REDACTED_PDFS'
);

-- Verify success
SELECT * FROM PII_REDACTION_LOG 
WHERE INPUT_FILE = 'test document with spaces.pdf' 
ORDER BY TIMESTAMP DESC LIMIT 1;
```

If this test passes with `STATUS = 'COMPLETED'`, your implementation is ready for production use! 🎉