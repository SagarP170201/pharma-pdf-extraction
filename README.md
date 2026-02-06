# Pharma Stock Statement PDF Extraction

Extracts stock data from PDF files using Snowflake AI_PARSE_DOCUMENT and Cortex LLM.

## Overview

Supports multiple PDF templates with different column structures using two extraction methods:

| Method | Use Case |
|--------|----------|
| **LAYOUT** | PDFs with standard table formatting |
| **OCR + LLM** | PDFs with non-standard formatting (dashed lines, etc.) |

## PDF Summary

| PDF | File | Rows | Method |
|-----|------|------|--------|
| 1 | 10000029 - Shweta Drug.pdf | 5 | LAYOUT |
| 2 | 10000053.pdf.pdf | 23 | LAYOUT |
| 3 | 10013295.pdf | 14 | LAYOUT |
| 4 | 10024246.pdf.pdf | 33 | LAYOUT |
| 5 | 10030920 PDF.pdf | 13 | LAYOUT |
| 6 | 10032299.pdf | 10 | LAYOUT |
| 7 | 10013972.PDF | 24 | OCR+LLM |
| 8 | 10000406--SHANMUGA | - | Pending |

## Prerequisites

- Snowflake account with Cortex AI enabled
- Database: `PHARMA_POC`
- Schema: `STOCK_EXTRACTION`
- Stage: `@PDF_STAGE_V2`

## Usage

1. Upload PDFs to stage:
```sql
PUT file:///path/to/pdfs/* @PDF_STAGE_V2;
```

2. Run extraction script in Snowsight

3. View results:
```sql
SELECT * FROM RAW_10000053;
```

## Key Techniques

### Column Merging
For split columns:
```sql
TRIM(SPLIT_PART(line.value, '|', 4)) || TRIM(SPLIT_PART(line.value, '|', 5)) AS item_description
```

### OCR + LLM Hybrid
For non-standard tables:
```sql
WITH ocr_output AS (
    SELECT AI_PARSE_DOCUMENT(TO_FILE('@stage/file.pdf'), {'mode': 'OCR'}):content::STRING
),
llm_parsed AS (
    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', 'Parse to JSON...' || raw_text)
)
SELECT ... FROM llm_parsed, LATERAL FLATTEN(input => json_data) f;
```

## Files

- `pharma_extraction_complete.sql` - Main extraction script
