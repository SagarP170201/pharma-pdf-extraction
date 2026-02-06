# Pharma PDF Extraction

Extracts stock statement data from pharma PDF files using Snowflake's AI_PARSE_DOCUMENT and Cortex LLM.

## Overview

This project handles 8 different PDF templates for pharma stock statements. Each template has different column structures, requiring customized extraction logic.

## Methods Used

| Method | Use Case |
|--------|----------|
| **LAYOUT** | PDFs with proper table formatting (markdown tables) |
| **OCR + LLM** | PDFs with dashed-line tables that LAYOUT mode doesn't recognize |

## PDF Summary

| PDF | File | Rows | Method | Template Type |
|-----|------|------|--------|---------------|
| 1 | 10000029 - Shweta Drug.pdf | - | Skipped | Custom |
| 2 | 10000053.pdf.pdf | 23 | LAYOUT | Metabolics |
| 3 | 10013295.pdf | 14 | LAYOUT | GI Optima |
| 4 | 10024246.pdf.pdf | 33 | LAYOUT | Standard |
| 5 | 10030920 PDF.pdf | 13 | LAYOUT | Alternate |
| 6 | 10032299.pdf | 10 | LAYOUT | Summary |
| 7 | 10013972.PDF | 24 | OCR+LLM | Dashed-line |
| 8 | 10000406--SHANMUGA | 89 | OCR+LLM | Multi-page |

## Prerequisites

- Snowflake account with Cortex AI enabled
- Database: `PHARMA_POC`
- Schema: `STOCK_EXTRACTION`
- Stage: `@PDF_STAGE_V2` with uploaded PDFs

## Usage

1. Upload PDFs to Snowflake stage:
```sql
PUT file:///path/to/pdfs/* @PDF_STAGE_V2;
```

2. Run the extraction script in Snowsight:
```sql
-- Open pharma_extraction_complete.sql and run
```

3. View results:
```sql
SELECT * FROM RAW_10000053;  -- PDF 2
SELECT * FROM RAW_10013295;  -- PDF 3
-- etc.
```

## Key Techniques

### Column Merging
Some PDFs split item descriptions across multiple columns. Fixed by concatenating:
```sql
TRIM(SPLIT_PART(line.value, '|', 4)) || TRIM(SPLIT_PART(line.value, '|', 5)) AS item_description
```

### OCR + LLM for Dashed Tables
For PDFs where LAYOUT mode fails:
```sql
WITH ocr_output AS (
    SELECT AI_PARSE_DOCUMENT(TO_FILE('@stage/file.pdf'), {'mode': 'OCR'}):content::STRING
),
llm_parsed AS (
    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', 'Parse to JSON...' || raw_text)
)
SELECT ... FROM llm_parsed, LATERAL FLATTEN(input => json_data) f;
```

### Column Rules for Empty Columns
When OCR produces space-delimited text, explicitly tell LLM which columns are empty:
```
Column rules:
- Packing: ALWAYS empty
- Stock-Out: ALWAYS empty  
- Stock-In: ALWAYS empty
```

## Files

- `pharma_extraction_complete.sql` - Main extraction script
