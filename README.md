# PDF Data Extraction with Snowflake Document AI

Extract structured data from PDF documents using Snowflake's AI_PARSE_DOCUMENT function and Cortex LLM.

## Overview

This solution demonstrates how to extract tabular data from PDF files stored in Snowflake stages using:

- **AI_PARSE_DOCUMENT** with LAYOUT mode for standard table extraction
- **AI_PARSE_DOCUMENT** with OCR mode combined with **Cortex LLM** for complex layouts

## Prerequisites

- Snowflake account with access to:
  - [Document AI (AI_PARSE_DOCUMENT)](https://docs.snowflake.com/en/user-guide/snowflake-cortex/document-ai)
  - [Cortex LLM Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- Warehouse with sufficient compute
- Stage with PDF files uploaded

## Setup

### 1. Create Database Objects

```sql
CREATE DATABASE IF NOT EXISTS my_database;
CREATE SCHEMA IF NOT EXISTS my_database.my_schema;
USE DATABASE my_database;
USE SCHEMA my_schema;
```

### 2. Create Stage and Upload Files

```sql
CREATE STAGE IF NOT EXISTS pdf_stage;

-- Upload files using SnowSQL or Snowsight
PUT file:///path/to/files/*.pdf @pdf_stage;
```

### 3. Verify Files

```sql
LIST @pdf_stage;
```

## Extraction Methods

### Method 1: LAYOUT Mode

Best for PDFs with well-structured tables:

```sql
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@pdf_stage/document.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    TRIM(SPLIT_PART(line.value, '|', 2)) AS column_1,
    TRIM(SPLIT_PART(line.value, '|', 3)) AS column_2,
    -- Add more columns as needed
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%';
```

### Method 2: OCR + LLM Hybrid

Best for PDFs with non-standard formatting:

```sql
WITH ocr_output AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@pdf_stage/document.pdf'),
        {'mode': 'OCR'}
    ):content::STRING AS raw_text
),
llm_response AS (
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'Parse the following text into a JSON array with columns: col1, col2, col3. Return ONLY valid JSON.

' || raw_text
    ) AS response
    FROM ocr_output
),
parsed AS (
    SELECT TRY_PARSE_JSON(
        REGEXP_SUBSTR(response, '\\[.*\\]', 1, 1, 's')
    ) AS json_data
    FROM llm_response
)
SELECT 
    f.value:"col1"::STRING AS column_1,
    f.value:"col2"::STRING AS column_2,
    TRY_TO_NUMBER(f.value:"col3"::STRING) AS column_3
FROM parsed, LATERAL FLATTEN(input => json_data) f;
```

## Common Patterns

### Handling Split Columns

When columns are split across multiple markdown columns:

```sql
TRIM(SPLIT_PART(line.value, '|', 3)) || TRIM(SPLIT_PART(line.value, '|', 4)) AS merged_column
```

### Filtering Header Rows

```sql
WHERE TRIM(SPLIT_PART(line.value, '|', 2)) REGEXP '^[0-9]+$'
```

### Multi-Page PDFs with OCR

```sql
SELECT AI_PARSE_DOCUMENT(
    TO_FILE('@pdf_stage/document.pdf'),
    {'mode': 'OCR', 'page_split': TRUE}
) AS parsed
```

## Best Practices

1. **Inspect raw output first** - Always examine the raw LAYOUT or OCR output before writing extraction logic
2. **Use TRY_TO_NUMBER** - Safely convert numeric strings to numbers
3. **Handle empty values** - Use NULL or empty string handling as appropriate
4. **Test incrementally** - Validate extraction logic on sample data before full runs

## Resources

- [AI_PARSE_DOCUMENT Documentation](https://docs.snowflake.com/en/sql-reference/functions/ai_parse_document)
- [Cortex LLM Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- [FLATTEN Function](https://docs.snowflake.com/en/sql-reference/functions/flatten)

## License

Apache-2.0
