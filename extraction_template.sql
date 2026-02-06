/***************************************************************************************************
  PDF DATA EXTRACTION WITH SNOWFLAKE DOCUMENT AI
  
  This script demonstrates how to extract structured data from PDF documents using:
  - AI_PARSE_DOCUMENT with LAYOUT mode for standard tables
  - AI_PARSE_DOCUMENT with OCR mode + Cortex LLM for complex layouts
  
  Documentation:
  - AI_PARSE_DOCUMENT: https://docs.snowflake.com/en/sql-reference/functions/ai_parse_document
  - Cortex LLM: https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions
***************************************************************************************************/

-- =================================================================================================
-- SETUP
-- =================================================================================================

USE DATABASE <your_database>;
USE SCHEMA <your_schema>;

-- Verify stage contents
-- LIST @<your_stage>;

-- =================================================================================================
-- METHOD 1: LAYOUT MODE EXTRACTION
-- Use for PDFs with well-structured markdown tables
-- =================================================================================================

-- Example: Basic table extraction
CREATE OR REPLACE TABLE extracted_data_layout AS
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@<your_stage>/<your_file>.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    '<your_file>.pdf' AS source_file,
    TRIM(SPLIT_PART(line.value, '|', 2)) AS column_1,
    TRIM(SPLIT_PART(line.value, '|', 3)) AS column_2,
    TRIM(SPLIT_PART(line.value, '|', 4)) AS column_3,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 5))) AS column_4,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 6))) AS column_5
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%'
  AND TRIM(SPLIT_PART(line.value, '|', 2)) NOT IN ('Header1', '')
  AND TRIM(SPLIT_PART(line.value, '|', 2)) NOT LIKE '%Total%';

-- Example: Handling merged/split columns
CREATE OR REPLACE TABLE extracted_data_merged AS
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@<your_stage>/<your_file>.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    '<your_file>.pdf' AS source_file,
    TRIM(SPLIT_PART(line.value, '|', 2)) AS id,
    -- Merge split description columns
    TRIM(SPLIT_PART(line.value, '|', 3)) || TRIM(SPLIT_PART(line.value, '|', 4)) AS description,
    TRIM(SPLIT_PART(line.value, '|', 5)) AS category,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 6))) AS quantity,
    TRY_TO_NUMBER(TRIM(SPLIT_PART(line.value, '|', 7))) AS value
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
  AND line.value NOT LIKE '%---%'
  AND TRIM(SPLIT_PART(line.value, '|', 2)) REGEXP '^[0-9]+$';

-- =================================================================================================
-- METHOD 2: OCR + LLM HYBRID EXTRACTION
-- Use for PDFs with non-standard formatting (dashed lines, complex layouts)
-- =================================================================================================

-- Example: Single page PDF with OCR + LLM
CREATE OR REPLACE TABLE extracted_data_ocr_llm AS
WITH ocr_output AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@<your_stage>/<your_file>.pdf'),
        {'mode': 'OCR'}
    ):content::STRING AS raw_text
),
llm_response AS (
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'Parse the following text into a JSON array. Each row should have these fields:
- id (string)
- description (string)  
- quantity (number)
- value (number)

Return ONLY a valid JSON array, no explanation.

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
    '<your_file>.pdf' AS source_file,
    f.value:"id"::STRING AS id,
    f.value:"description"::STRING AS description,
    TRY_TO_NUMBER(f.value:"quantity"::STRING) AS quantity,
    TRY_TO_NUMBER(f.value:"value"::STRING) AS value
FROM parsed, LATERAL FLATTEN(input => json_data) f
WHERE f.value:"id"::STRING IS NOT NULL;

-- Example: Multi-page PDF with page-by-page processing
CREATE OR REPLACE TABLE extracted_data_multipage AS
WITH pages AS (
    SELECT 
        page.index AS page_num,
        page.value:content::STRING AS page_content
    FROM (
        SELECT AI_PARSE_DOCUMENT(
            TO_FILE('@<your_stage>/<your_file>.pdf'),
            {'mode': 'OCR', 'page_split': TRUE}
        ) AS parsed
    ),
    LATERAL FLATTEN(input => parsed:pages) page
),
llm_responses AS (
    SELECT 
        page_num,
        REPLACE(REPLACE(
            SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-70b',
                'Parse rows into JSON array with fields: id, name, quantity, value. 
Skip headers and totals. Return ONLY JSON array.

' || page_content
            ), '```json', ''), '```', '') AS raw_json
    FROM pages
),
parsed AS (
    SELECT page_num, TRY_PARSE_JSON(raw_json) AS json_data
    FROM llm_responses
    WHERE raw_json IS NOT NULL
)
SELECT 
    '<your_file>.pdf' AS source_file,
    page_num,
    f.value:id::STRING AS id,
    f.value:name::STRING AS name,
    TRY_TO_NUMBER(f.value:quantity::STRING) AS quantity,
    TRY_TO_NUMBER(f.value:value::STRING) AS value
FROM parsed, LATERAL FLATTEN(input => json_data) f
WHERE json_data IS NOT NULL
  AND f.value:id::STRING IS NOT NULL;

-- =================================================================================================
-- UTILITY QUERIES
-- =================================================================================================

-- Inspect raw LAYOUT output
SELECT AI_PARSE_DOCUMENT(
    TO_FILE('@<your_stage>/<your_file>.pdf'),
    {'mode': 'LAYOUT'}
):content::STRING AS markdown_content;

-- Inspect raw OCR output
SELECT AI_PARSE_DOCUMENT(
    TO_FILE('@<your_stage>/<your_file>.pdf'),
    {'mode': 'OCR'}
):content::STRING AS ocr_text;

-- Inspect column positions
WITH parsed AS (
    SELECT AI_PARSE_DOCUMENT(
        TO_FILE('@<your_stage>/<your_file>.pdf'),
        {'mode': 'LAYOUT'}
    ):content::STRING AS markdown_content
)
SELECT 
    TRIM(SPLIT_PART(line.value, '|', 2)) AS c2,
    TRIM(SPLIT_PART(line.value, '|', 3)) AS c3,
    TRIM(SPLIT_PART(line.value, '|', 4)) AS c4,
    TRIM(SPLIT_PART(line.value, '|', 5)) AS c5,
    TRIM(SPLIT_PART(line.value, '|', 6)) AS c6
FROM parsed,
LATERAL FLATTEN(input => SPLIT(markdown_content, '\n')) line
WHERE line.value LIKE '%|%'
LIMIT 5;

-- Extraction summary
SELECT 
    'extracted_data_layout' AS table_name, COUNT(*) AS row_count FROM extracted_data_layout
UNION ALL 
SELECT 'extracted_data_ocr_llm', COUNT(*) FROM extracted_data_ocr_llm
ORDER BY table_name;
