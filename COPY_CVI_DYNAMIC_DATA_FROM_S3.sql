/*******************************************************************************
* Procedure: COPY_CVI_DYNAMIC_DATA_FROM_S3
* Description: Copies JSON data from S3 stage into the DEV_CVI_RAW_POC table
* 
* The procedure:
* 1. Reads JSON files from the dev_cvi_reporting/dynamicTablesTest/ stage
* 2. Parses the JSON content into a VARIANT column 'raw_json_data'
* 3. Adds a timestamp for when the data was loaded
* 4. Uses Snowflake's COPY command for efficient bulk loading
*******************************************************************************/

CREATE OR REPLACE PROCEDURE APRICOT_DEV_RAW.COMMON.COPY_CVI_DYNAMIC_DATA_FROM_S3()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Copy data from S3 stage into the target table
    -- The stage path points to JSON files containing dynamic table definitions
    COPY INTO DEV_CVI_RAW_POC FROM (
        SELECT 
            PARSE_JSON($1) AS raw_json_data,    -- Parse the JSON file content into a VARIANT column
            CURRENT_TIMESTAMP() AS _loaded_at    -- Add load timestamp for tracking
        FROM @APRICOT_DEV_RAW.COMMON.dev_cvi_reporting/dynamicTablesTest/(FILE_FORMAT => APRICOT_DEV_RAW.COMMON.RAW_JSON_FF)
    );
END;
$$;