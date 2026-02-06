-- ============================================================================
-- PHARMA PDF EXTRACTION - COMPLETE END-TO-END SCRIPT
-- Extracts stock statement data from 8 different PDF templates
-- Run in Snowsight to extract all PDFs
-- ============================================================================

USE DATABASE PHARMA_POC;
USE SCHEMA STOCK_EXTRACTION;

-- ============================================================================
-- PART 1: LAYOUT EXTRACTION (6 PDFs with markdown tables)
-- Each PDF has different column structures - handled individually
-- ============================================================================

-- PDF 1: RAW_10000029_SHWETA_DRUG (Skipped - different template)
-- Uncomment and customize if needed
/*
CREATE OR REPLACE TABLE RAW_10000029_SHWETA_DRUG AS
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@PDF_STAGE_V2/10000029 - Shweta Drug.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    '10000029 - Shweta Drug.pdf' AS source_file,
    TRIM(SPLIT_PART(line.value, '|', 2)) AS col1,
    TRIM(SPLIT_PART(line.value, '|', 3)) AS col2,
    -- Add more columns as needed
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%';
*/

-- PDF 2: RAW_10000053 (23 rows)
-- Columns: Code, Item Description (merged from 2 cols), Packing, Opening, Purchase, Stock-Out, Aug, Sep, Sales, Stock-In, Closing, Stock-Value, Sales-Value
CREATE OR REPLACE TABLE RAW_10000053 AS
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@PDF_STAGE_V2/10000053.pdf.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    '10000053.pdf.pdf' AS source_file,
    TRIM(SPLIT_PART(line.value, '|', 2)) AS code,
    TRIM(SPLIT_PART(line.value, '|', 4)) || TRIM(SPLIT_PART(line.value, '|', 5)) AS item_description,
    TRIM(SPLIT_PART(line.value, '|', 6)) AS packing,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 7))) AS opening,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 8))) AS purchase,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 9))) AS stock_out,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 10))) AS aug,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 11))) AS sep,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 12))) AS sales,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 13))) AS stock_in,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 14))) AS closing,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 15))) AS stock_value,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 16))) AS sales_value
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%'
  AND TRIM(SPLIT_PART(line.value, '|', 2)) REGEXP '^[0-9]+$';

-- PDF 3: RAW_10013295 (14 rows)
-- Columns: Product Description, Packing, Opeste, Purch, Sale, Sale Val, Blot, Stock, Stk Val, Sep, Aug, Stk120, Exp3m
CREATE OR REPLACE TABLE RAW_10013295 AS
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@PDF_STAGE_V2/10013295.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    '10013295.pdf' AS source_file,
    TRIM(SPLIT_PART(line.value, '|', 2)) AS product_description,
    TRIM(SPLIT_PART(line.value, '|', 3)) AS packing,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 4))) AS opeste,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 5))) AS purch,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 6))) AS sale,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 7))) AS sale_val,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 8))) AS blot,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 9))) AS stock,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 10))) AS stk_val,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 11))) AS sep,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 12))) AS aug,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 13))) AS stk120,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 14))) AS exp3m
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%'
  AND TRIM(SPLIT_PART(line.value, '|', 2)) NOT IN ('PRODUCT DESCRIPTION', '')
  AND TRIM(SPLIT_PART(line.value, '|', 2)) NOT LIKE 'ABBOTT%'
  AND TRIM(SPLIT_PART(line.value, '|', 2)) NOT LIKE '%TOTAL%';

-- PDF 4: RAW_10024246 (33 rows)
-- Columns: Code, Item Description, Packing, Opening, Purchase Stock-Out (merged), Aug, Sep, Sales, Stock-In, Closing Stock (merged), Stock-Value, Sales-Value
CREATE OR REPLACE TABLE RAW_10024246 AS
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@PDF_STAGE_V2/10024246.pdf.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    '10024246.pdf.pdf' AS source_file,
    TRIM(SPLIT_PART(line.value, '|', 2)) AS code,
    TRIM(SPLIT_PART(line.value, '|', 3)) AS item_description,
    TRIM(SPLIT_PART(line.value, '|', 4)) AS packing,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 5))) AS opening,
    TRIM(SPLIT_PART(line.value, '|', 6)) AS purchase_stock_out,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 7))) AS aug,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 8))) AS sep,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 9))) AS sales,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 10))) AS stock_in,
    TRIM(SPLIT_PART(line.value, '|', 11)) AS closing_stock,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 12))) AS stock_value,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 13))) AS sales_value
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%'
  AND TRIM(SPLIT_PART(line.value, '|', 2)) REGEXP '^[0-9]+$';

-- PDF 5: RAW_10030920 (13 rows)
-- Columns: S.No, Product Name, Qty, PQty, SQty, S/he, S/let, Stock, Pur/late, Stock Value, Sq/ur Value, PQ/ur, PFr, P/let
CREATE OR REPLACE TABLE RAW_10030920 AS
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@PDF_STAGE_V2/10030920 PDF.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    '10030920 PDF.pdf' AS source_file,
    TRIM(SPLIT_PART(line.value, '|', 2)) AS sno,
    TRIM(SPLIT_PART(line.value, '|', 3)) AS product_name,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 4))) AS qty,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 5))) AS pqty,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 6))) AS sqty,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 7))) AS s_he,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 8))) AS s_let,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 9))) AS stock,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 10))) AS pur_late,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 11))) AS stock_value,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 12))) AS squr_value,
    TRIM(SPLIT_PART(line.value, '|', 13)) AS pqur,
    TRIM(SPLIT_PART(line.value, '|', 14)) AS pfr,
    TRIM(SPLIT_PART(line.value, '|', 15)) AS p_let
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%'
  AND TRIM(SPLIT_PART(line.value, '|', 2)) REGEXP '^[0-9]+$';

-- PDF 6: RAW_10032299 (10 rows)
-- Columns: S.No, Product Name, Packing, Opsth, For, Aug, Sep, Sale, SFree, CurStk, OrdQty, Rate, TotIssues
CREATE OR REPLACE TABLE RAW_10032299 AS
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@PDF_STAGE_V2/10032299.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    '10032299.pdf' AS source_file,
    TRIM(SPLIT_PART(line.value, '|', 2)) AS sno,
    TRIM(SPLIT_PART(line.value, '|', 3)) AS product_name,
    TRIM(SPLIT_PART(line.value, '|', 4)) AS packing,
    TRIM(SPLIT_PART(line.value, '|', 5)) AS opsth,
    TRIM(SPLIT_PART(line.value, '|', 6)) AS for_col,
    TRIM(SPLIT_PART(line.value, '|', 7)) AS aug,
    TRIM(SPLIT_PART(line.value, '|', 8)) AS sep,
    TRIM(SPLIT_PART(line.value, '|', 9)) AS sale,
    TRIM(SPLIT_PART(line.value, '|', 10)) AS sfree,
    TRIM(SPLIT_PART(line.value, '|', 11)) AS curstk,
    TRIM(SPLIT_PART(line.value, '|', 12)) AS ordqty,
    TRIM(SPLIT_PART(line.value, '|', 13)) AS rate,
    TRIM(SPLIT_PART(line.value, '|', 14)) AS totissues
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%'
  AND TRIM(SPLIT_PART(line.value, '|', 2)) REGEXP '^[0-9]+$';

-- ============================================================================
-- PART 2: OCR + LLM HYBRID EXTRACTION (2 PDFs with dashed-line tables)
-- These PDFs use dashed-line separators that LAYOUT mode doesn't recognize
-- ============================================================================

-- PDF 7: RAW_10013972 (24 rows)
-- Single page, dashed-line table format
-- Columns: Code, Item Description, Packing, Opening, Purchase, Stock-Out, Aug, Sep, Sales, Stock-In, Closing, Stock-Value, Sales-Value
CREATE OR REPLACE TABLE RAW_10013972 AS
WITH ocr_output AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@PDF_STAGE_V2/10013972.PDF'),
        {'mode': 'OCR'}
    ):content::STRING AS raw_text
),
llm_raw AS (
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'Parse ALL 24 product rows to JSON array. Include rows with NO numeric data (empty inventory).

13 columns: Code|Item Description|Packing|Opening|Purchase|Stock-Out|Aug|Sep|Sales|Stock-In|Closing|Stock-Value|Sales-Value

Column rules:
- Packing: ALWAYS empty (ignore any text that looks like packing)
- Stock-Out: ALWAYS empty  
- Stock-In: ALWAYS empty
- Sales: HAS VALUES when present
- Closing: HAS VALUES when present

Include these codes even if they have no data: 27982, 27667, 21069, 27978

Return ONLY valid JSON array with ALL 24 rows.

' || raw_text
    ) AS raw_result
    FROM ocr_output
),
llm_parsed AS (
    SELECT TRY_PARSE_JSON(
        REGEXP_SUBSTR(raw_result, '\\[.*\\]', 1, 1, 's')
    ) AS json_data
    FROM llm_raw
)
SELECT 
    '10013972.PDF' AS source_file,
    f.value:"Code"::STRING AS code,
    f.value:"Item Description"::STRING AS item_description,
    f.value:"Packing"::STRING AS packing,
    TRY_TO_NUMBER(f.value:"Opening"::STRING) AS opening,
    TRY_TO_NUMBER(f.value:"Purchase"::STRING) AS purchase,
    NULL AS stock_out,
    TRY_TO_NUMBER(f.value:"Aug"::STRING) AS aug,
    TRY_TO_NUMBER(f.value:"Sep"::STRING) AS sep,
    TRY_TO_NUMBER(f.value:"Sales"::STRING) AS sales,
    NULL AS stock_in,
    TRY_TO_NUMBER(f.value:"Closing"::STRING) AS closing,
    TRY_TO_NUMBER(f.value:"Stock-Value"::STRING) AS stock_value,
    TRY_TO_NUMBER(f.value:"Sales-Value"::STRING) AS sales_value
FROM llm_parsed, LATERAL FLATTEN(input => json_data) f;

-- PDF 8: RAW_10000406_SHANMUGA (89 rows) - Skipped
-- Multi-page, multi-company, dashed-line tables
-- Uncomment if needed
/*
CREATE OR REPLACE TABLE RAW_10000406_SHANMUGA AS
WITH pages AS (
    SELECT 
        page.index AS page_num,
        page.value:content::STRING AS page_content
    FROM (
        SELECT AI_PARSE_DOCUMENT(
            TO_FILE('@PDF_STAGE_V2/10000406--SHANMUGA MEDICAL AGENCIES.pdf'),
            {'mode': 'OCR', 'page_split': TRUE}
        ) AS parsed
    ),
    LATERAL FLATTEN(input => parsed:pages) page
),
llm_parsed AS (
    SELECT 
        page_num,
        REPLACE(REPLACE(
            SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-70b',
                'Parse stock rows into JSON array. Extract: company, product_name, packing, opening_qty, receipt_qty, sales_last, sales_qty, closing_qty, closing_value, age_days. Skip headers/totals/expiry/claims. Return ONLY JSON array.

' || page_content
            ), '```json', ''), '```', '') AS raw_json
    FROM pages
),
parsed_json AS (
    SELECT page_num, TRY_PARSE_JSON(raw_json) AS json_data
    FROM llm_parsed
    WHERE raw_json IS NOT NULL
)
SELECT 
    '10000406--SHANMUGA MEDICAL AGENCIES.pdf' AS source_file,
    f.value:company::STRING AS company,
    f.value:product_name::STRING AS product_name,
    f.value:packing::STRING AS packing,
    TRY_TO_NUMBER(f.value:opening_qty::STRING) AS opening_qty,
    TRY_TO_NUMBER(f.value:receipt_qty::STRING) AS receipt_qty,
    TRY_TO_NUMBER(f.value:sales_last::STRING) AS sales_last,
    TRY_TO_NUMBER(f.value:sales_qty::STRING) AS sales_qty,
    TRY_TO_NUMBER(f.value:closing_qty::STRING) AS closing_qty,
    TRY_TO_NUMBER(f.value:closing_value::STRING) AS closing_value,
    TRY_TO_NUMBER(f.value:age_days::STRING) AS age_days
FROM parsed_json, LATERAL FLATTEN(input => json_data) f
WHERE json_data IS NOT NULL
  AND f.value:product_name::STRING IS NOT NULL 
  AND f.value:product_name::STRING != '';
*/

-- ============================================================================
-- PART 3: EXTRACTION SUMMARY
-- ============================================================================

SELECT 
    'RAW_10000053' AS table_name, COUNT(*) AS row_count, 'LAYOUT' AS method FROM RAW_10000053
UNION ALL SELECT 'RAW_10013295', COUNT(*), 'LAYOUT' FROM RAW_10013295
UNION ALL SELECT 'RAW_10024246', COUNT(*), 'LAYOUT' FROM RAW_10024246
UNION ALL SELECT 'RAW_10030920', COUNT(*), 'LAYOUT' FROM RAW_10030920
UNION ALL SELECT 'RAW_10032299', COUNT(*), 'LAYOUT' FROM RAW_10032299
UNION ALL SELECT 'RAW_10013972', COUNT(*), 'OCR+LLM' FROM RAW_10013972
ORDER BY table_name;

-- ============================================================================
-- PART 4: VIEW EXTRACTED DATA
-- ============================================================================

-- PDF 2
SELECT * FROM RAW_10000053;

-- PDF 3
SELECT * FROM RAW_10013295;

-- PDF 4
SELECT * FROM RAW_10024246;

-- PDF 5
SELECT * FROM RAW_10030920;

-- PDF 6
SELECT * FROM RAW_10032299;

-- PDF 7
SELECT * FROM RAW_10013972 ORDER BY TRY_CAST(code AS INT);
