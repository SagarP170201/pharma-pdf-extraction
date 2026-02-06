-- ============================================================================
-- PHARMA STOCK STATEMENT PDF EXTRACTION
-- ============================================================================
-- Extracts stock data from PDF files using Snowflake AI_PARSE_DOCUMENT
-- Supports multiple PDF templates with different column structures
-- ============================================================================

USE DATABASE PHARMA_POC;
USE SCHEMA STOCK_EXTRACTION;

-- ============================================================================
-- LAYOUT MODE EXTRACTIONS
-- For PDFs with standard table formatting
-- ============================================================================

-- PDF 1: 10000029 - Shweta Drug (5 rows)
CREATE OR REPLACE TABLE RAW_10000029_SHWETA_DRUG AS
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@PDF_STAGE_V2/10000029 - Shweta Drug.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    '10000029 - Shweta Drug.pdf' AS source_file,
    TRIM(SPLIT_PART(line.value, '|', 2)) AS product_name,
    TRIM(SPLIT_PART(line.value, '|', 3)) AS unit,
    TRIM(SPLIT_PART(line.value, '|', 4)) AS co,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 5))) AS op_qty,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 6))) AS in_qty,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 7))) AS out_qty,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 8))) AS cl_qty,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 9))) AS cl_val
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%'
  AND TRIM(SPLIT_PART(line.value, '|', 2)) NOT IN ('Product name', '')
  AND TRIM(SPLIT_PART(line.value, '|', 2)) NOT LIKE '%Total%';

-- PDF 2: 10000053 (23 rows)
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

-- PDF 3: 10013295 (14 rows)
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

-- PDF 4: 10024246 (33 rows)
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

-- PDF 5: 10030920 (13 rows)
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

-- PDF 6: 10032299 (10 rows)
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
-- OCR + LLM HYBRID EXTRACTION
-- For PDFs with non-standard table formatting (dashed lines, etc.)
-- ============================================================================

-- PDF 7: 10013972 (24 rows)
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
- Packing: ALWAYS empty
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

-- PDF 8: 10000406 (Pending - requires additional configuration)
-- Uncomment and configure when ready
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
                'Parse stock rows into JSON array. Extract: company, product_name, packing, opening_qty, receipt_qty, sales_last, sales_qty, closing_qty, closing_value, age_days. Skip headers/totals. Return ONLY JSON array.

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
-- EXTRACTION SUMMARY
-- ============================================================================

SELECT 'RAW_10000029_SHWETA_DRUG' AS table_name, COUNT(*) AS row_count FROM RAW_10000029_SHWETA_DRUG
UNION ALL SELECT 'RAW_10000053', COUNT(*) FROM RAW_10000053
UNION ALL SELECT 'RAW_10013295', COUNT(*) FROM RAW_10013295
UNION ALL SELECT 'RAW_10024246', COUNT(*) FROM RAW_10024246
UNION ALL SELECT 'RAW_10030920', COUNT(*) FROM RAW_10030920
UNION ALL SELECT 'RAW_10032299', COUNT(*) FROM RAW_10032299
UNION ALL SELECT 'RAW_10013972', COUNT(*) FROM RAW_10013972
ORDER BY table_name;

-- ============================================================================
-- VIEW EXTRACTED DATA
-- ============================================================================

SELECT * FROM RAW_10000029_SHWETA_DRUG;
SELECT * FROM RAW_10000053;
SELECT * FROM RAW_10013295;
SELECT * FROM RAW_10024246;
SELECT * FROM RAW_10030920;
SELECT * FROM RAW_10032299;
SELECT * FROM RAW_10013972 ORDER BY TRY_CAST(code AS INT);
