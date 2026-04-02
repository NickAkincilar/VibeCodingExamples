CREATE OR REPLACE STAGE AI_BUILDER.TEST.Doctor_Notes
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- 1. Create stream on source
CREATE STREAM IF NOT EXISTS AI_BUILDER.TEST.Text_Document_STREAM
  ON STAGE AI_BUILDER.TEST.Doctor_Notes;

-- 2. Create target table (source columns + AI output + split page columns)
CREATE TABLE IF NOT EXISTS AI_BUILDER.TEST.Text_Document (
  RELATIVE_PATH VARCHAR,
  SIZE NUMBER,
  LAST_MODIFIED TIMESTAMP_TZ,
  MD5 VARCHAR,
  ETAG VARCHAR,
  FILE_URL VARCHAR,
  document_text VARIANT,
  PAGE_NUMBER INT,
  PAGE_TEXT VARCHAR,
  PROCESSED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Create triggered task (parse + split pages in one step)
CREATE OR REPLACE TASK AI_BUILDER.TEST.Text_Document_TASK
  WAREHOUSE = COMPUTE_WH
  WHEN SYSTEM$STREAM_HAS_DATA('AI_BUILDER.TEST.Text_Document_STREAM')
AS
INSERT INTO AI_BUILDER.TEST.Text_Document (RELATIVE_PATH, SIZE, LAST_MODIFIED, MD5, ETAG, FILE_URL, document_text, PAGE_NUMBER, PAGE_TEXT)
SELECT
  RELATIVE_PATH, SIZE, LAST_MODIFIED, MD5, ETAG, FILE_URL,
  parsed_result,
  f.index,
  f.value:content::STRING
FROM (
  SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED, MD5, ETAG, FILE_URL,
    SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(TO_FILE('@AI_BUILDER.TEST.Doctor_Notes', RELATIVE_PATH), {'mode': 'LAYOUT', 'page_split': true}) AS parsed_result
  FROM AI_BUILDER.TEST.Text_Document_STREAM
),
LATERAL FLATTEN(input => parsed_result:pages) f;

-- Resume task to start processing
ALTER TASK AI_BUILDER.TEST.Text_Document_TASK RESUME;

CREATE OR REPLACE DYNAMIC TABLE AI_BUILDER.TEST.Text_Pages
  TARGET_LAG = '1 hours'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT 
  RELATIVE_PATH,
  f.index AS PAGE_NUMBER,
  f.value:content::STRING AS PAGE_TEXT
FROM AI_BUILDER.TEST.Text_Document,
LATERAL FLATTEN(input => document_text:pages) f;

CREATE OR REPLACE CORTEX SEARCH SERVICE AI_BUILDER.TEST.Search_doctors_notes
  ON PAGE_TEXT
  ATTRIBUTES RELATIVE_PATH
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hours'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
  INITIALIZE = ON_SCHEDULE
  AS (
    SELECT * FROM AI_BUILDER.TEST.Text_Pages
  );


--- Below Agent requires the following semantic view EMR_DB.PATIENT_360.PATIENT_360_EMR

create or replace agent EMR_AGENT
profile='{"display_name":"EMR_AGENT"}'
from specification
$$
models:
  orchestration: "auto"
orchestration: {}
instructions:
  response: "Answer only using data from the the tools. Do not create answers based\
    \ on general info."
  orchestration: "You are Doctors assistant. Your job is to search doctors notes about\
    \ a specific patient and combine that with EMR datamart to answer questions.\n\
    \nTo properly identify a patient:\n-   first try match matching by MRN number.\n\
    - if not found, match by first name, lat name and date of birth instead."
tools:
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "EMR_Datamart"
      description: "This is a patient datamart that contains EMR dataset about patients"
  - tool_spec:
      type: "cortex_search"
      name: "Doctors_Notes"
      description: "This tool allows the agent to search previous doctors notes about\
        \ a patient. You can use patient name, MRN number, NPI number or Doctors name\
        \ to find patients"
  - tool_spec:
      type: "web_search"
      name: "Web Search"
skills: []
tool_resources:
  Doctors_Notes:
    columns_and_descriptions:
      PAGE_TEXT:
        description: "This is the text from the doctors note."
        filterable: false
        searchable: true
        type: "TEXT"
    id_column: "RELATIVE_PATH"
    max_results: 5
    search_service: "AI_BUILDER.TEST.SEARCH_DOCTORS_NOTES"
    title_column: "RELATIVE_PATH"
  EMR_Datamart:
    execution_environment:
      type: "warehouse"
      warehouse: ""
    semantic_view: "EMR_DB.PATIENT_360.PATIENT_360_EMR"
  Web Search:
    max_results: 10
$$;
