/***************************************************************************************************
  PDF EXTRACTION - IMPLEMENTATION
  
  Pre-built extraction queries for specific PDF templates.
  Each PDF has unique column structures requiring customized extraction logic.
  
  See extraction_template.sql for reusable patterns.
***************************************************************************************************/

USE DATABASE PHARMA_POC;
USE SCHEMA STOCK_EXTRACTION;

-- =================================================================================================
-- PDF 1: 10000029 - Shweta Drug (5 rows, 8 columns)
-- Columns: Product name, Unit, Co, Op qty, InQty, OutQty, Cl qty, Cl val
-- =================================================================================================

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

-- =================================================================================================
-- PDF 2: 10000053 (23 rows, 14 columns with merged description)
-- Columns: Code, Item Description (merged c4+c5), Packing, Opening, Purchase, Stock-Out, 
--          Aug, Sep, Sales, Stock-In, Closing, Stock-Value, Sales-Value
-- =================================================================================================

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

-- =================================================================================================
-- PDF 3: 10013295 (14 rows, 13 columns)
-- Columns: Product Description, Packing, Opeste, Purch, Sale, Sale Val, Blot, Stock, 
--          Stk Val, Sep, Aug, Stk120, Exp3m
-- =================================================================================================

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

-- =================================================================================================
-- PDF 4: 10024246 (33 rows, 12 columns)
-- Columns: Code, Item Description, Packing, Opening, Purchase Stock-Out (merged), 
--          Aug, Sep, Sales, Stock-In, Closing Stock (merged), Stock-Value, Sales-Value
-- =================================================================================================

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

-- =================================================================================================
-- PDF 5: 10030920 (13 rows, 14 columns)
-- Columns: S.No, Product Name, Qty, PQty, SQty, S/he, S/let, Stock, Pur/late, 
--          Stock Value, Sq/ur Value, PQ/ur, PFr, P/let
-- =================================================================================================

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

-- =================================================================================================
-- PDF 6: 10032299 (10 rows, 13 columns)
-- Columns: S.No, Product Name, Packing, Opsth, For, Aug, Sep, Sale, SFree, 
--          CurStk, OrdQty, Rate, TotIssues
-- =================================================================================================

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

-- =================================================================================================
-- PDF 7: 10013972 (24 rows, OCR + LLM method)
-- Columns: Code, Item Description, Packing, Opening, Purchase, Stock-Out, Aug, Sep, 
--          Sales, Stock-In, Closing, Stock-Value, Sales-Value
-- Note: Uses OCR + LLM due to dashed-line table format
-- =================================================================================================

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

-- =================================================================================================
-- EXTRACTION SUMMARY
-- =================================================================================================

SELECT 'RAW_10000029_SHWETA_DRUG' AS table_name, COUNT(*) AS row_count FROM RAW_10000029_SHWETA_DRUG
UNION ALL SELECT 'RAW_10000053', COUNT(*) FROM RAW_10000053
UNION ALL SELECT 'RAW_10013295', COUNT(*) FROM RAW_10013295
UNION ALL SELECT 'RAW_10024246', COUNT(*) FROM RAW_10024246
UNION ALL SELECT 'RAW_10030920', COUNT(*) FROM RAW_10030920
UNION ALL SELECT 'RAW_10032299', COUNT(*) FROM RAW_10032299
UNION ALL SELECT 'RAW_10013972', COUNT(*) FROM RAW_10013972
ORDER BY table_name;

-- =================================================================================================
-- VIEW EXTRACTED DATA
-- =================================================================================================

SELECT * FROM RAW_10000029_SHWETA_DRUG;
SELECT * FROM RAW_10000053;
SELECT * FROM RAW_10013295;
SELECT * FROM RAW_10024246;
SELECT * FROM RAW_10030920;
SELECT * FROM RAW_10032299;
SELECT * FROM RAW_10013972 ORDER BY TRY_CAST(code AS INT);
