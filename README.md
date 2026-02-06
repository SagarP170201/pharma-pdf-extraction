# PDF Data Extraction with Snowflake Document AI

Extract structured data from PDF documents using Snowflake's AI_PARSE_DOCUMENT function and Cortex LLM.

## Overview

This solution demonstrates how to extract tabular data from PDF files stored in Snowflake stages using:

- **AI_PARSE_DOCUMENT** with LAYOUT mode for standard table extraction
- **AI_PARSE_DOCUMENT** with OCR mode combined with **Cortex LLM** for complex layouts

## File Mapping

| PDF # | File Name | Output Table | Rows | Method | Columns |
|-------|-----------|--------------|------|--------|---------|
| 1 | `10000029 - Shweta Drug.pdf` | RAW_10000029_SHWETA_DRUG | 5 | LAYOUT | 8 cols |
| 2 | `10000053.pdf.pdf` | RAW_10000053 | 23 | LAYOUT | 14 cols (merged) |
| 3 | `10013295.pdf` | RAW_10013295 | 14 | LAYOUT | 13 cols |
| 4 | `10024246.pdf.pdf` | RAW_10024246 | 33 | LAYOUT | 12 cols |
| 5 | `10030920 PDF.pdf` | RAW_10030920 | 13 | LAYOUT | 14 cols |
| 6 | `10032299.pdf` | RAW_10032299 | 10 | LAYOUT | 13 cols |
| 7 | `10013972.PDF` | RAW_10013972 | 24 | OCR+LLM | 13 cols |

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

## Files

| File | Description |
|------|-------------|
| `extraction_template.sql` | Generic reusable patterns for PDF extraction |
| `extraction_implementation.sql` | Working queries for all 7 PDF templates |

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
    TRIM(SPLIT_PART(line.value, '|', 3)) AS column_2
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
        'Parse into JSON array with columns: col1, col2. Return ONLY valid JSON.

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
    TRY_TO_NUMBER(f.value:"col2"::STRING) AS column_2
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

## Resources

- [AI_PARSE_DOCUMENT Documentation](https://docs.snowflake.com/en/sql-reference/functions/ai_parse_document)
- [Cortex LLM Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- [FLATTEN Function](https://docs.snowflake.com/en/sql-reference/functions/flatten)

## License

Apache-2.0
