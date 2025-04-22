-- Execute the procedure
CALL DEBUG_CREATE_TABLE();
CREATE OR REPLACE PROCEDURE DEBUG_CREATE_TABLE()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Get all rows from the table
    //  WHERE raw_json_data:metadata.tenant_id = 'apricot_103326' AND raw_json_data:metadata.source_table = 'data_16'
    // raw_json_data:metadata.tenant_id = 'apricot_116558' AND raw_json_data:metadata.source_table IN ('data_4') AND raw_json_data:metadata.extract_timestamp::TIMESTAMP > '2025-04-17 00:00:00'
    // loaded_at > DATEADD(HOURS, -1, CURRENT_TIMESTAMP())
    // 103326 - data_176, data_16
    var getJSONData = snowflake.createStatement({
        sqlText: `SELECT * FROM APRICOT_DEV_RAW.COMMON.dev_cvi_raw_poc WHERE raw_json_data:metadata.tenant_id = 'apricot_116558' AND raw_json_data:metadata.source_table IN ('data_4')`
    });
    var result = getJSONData.execute();
    
    var allResults = [];
    var processedTables = new Set();
    
    // Process each row
    while (result.next()) {
        // Get the first column that contains JSON data
        var JSON_DATA;
        for (var i = 1; i <= result.getColumnCount(); i++) {
            var value = result.getColumnValue(i);
            if (typeof value === 'object' && value !== null) {
                JSON_DATA = value;
                break;
            }
        }
        
        if (!JSON_DATA) {
            continue; // Skip rows without JSON data
        }
        
        // Create the target table name
        var targetTableName = `${JSON_DATA.metadata.tenant_id}_${JSON_DATA.metadata.source_table}`;
        
        // Debug: Print the JSON structure directly from JavaScript
        var debugInfo = `-- Debug Information:
-- Source Table: ${JSON_DATA.metadata.source_table}
-- Tenant ID: ${JSON_DATA.metadata.tenant_id}
-- Target Table: ${targetTableName}
-- Column Count: ${JSON_DATA.columns.length}
-- Data Row Count: ${JSON_DATA.data.length}
-- Primary Keys: ${JSON_DATA.keys.primary_key ? JSON_DATA.keys.primary_key.join(', ') : 'None'}`;

        // Function to convert Snowflake data type to SQL data type
        function getSQLDataType(dataType, maxLength) {
            switch(dataType.toUpperCase()) {
                case 'INT':
                    return 'INTEGER';
                case 'TINYINT':
                    return 'TINYINT';
                case 'VARCHAR':
                    return `VARCHAR(${maxLength})`;
                case 'DATETIME':
                    return 'TIMESTAMP';
                case 'DATE':
                    return 'DATE';
                case 'JSON':
                    return 'VARIANT';
                case 'BLOB':
                    return 'VARIANT';
                default:
                    return dataType.toUpperCase();
            }
        }

        // Function to generate column definition
        function generateColumnDef(column, includeDefaults = true) {
            let def = `${column.name} ${getSQLDataType(column.data_type, column.max_length)}`;
            
            // Handle NOT NULL constraint
            // Always apply NOT NULL if column is not nullable, regardless of default value
            // if (!column.nullable) {
            //     def += ' NOT NULL';
            // }
            
            // Handle default values only if includeDefaults is true
            // if (includeDefaults && column.default_value !== null) {
            //     switch(column.data_type.toUpperCase()) {
            //         case 'TIMESTAMP':
            //             def += ` DEFAULT TIMESTAMP_NTZ_FROM_PARTS(1970, 1, 1, 0, 0, 0)`;
            //             break;
            //         case 'DATE':
            //             def += ` DEFAULT DATE_FROM_PARTS(1970, 1, 1)`;
            //             break;
            //         case 'VARCHAR':
            //             def += ` DEFAULT ${column.default_value === '' ? "''" : `'${column.default_value}'`}`;
            //             break;
            //         case 'INTEGER':
            //         case 'TINYINT':
            //             def += ` DEFAULT ${column.default_value || 0}`;
            //             break;
            //         case 'JSON':
            //         case 'VARIANT':
            //             def += ` DEFAULT PARSE_JSON('{}')`;
            //             break;
            //         default:
            //             def += ` DEFAULT ${column.default_value}`;
            //     }
            // }
            
            return def;
        }

        // Generate CREATE TABLE statement
        let createTableSQL = `CREATE TABLE IF NOT EXISTS ${targetTableName} (\n`;
        
        // Add columns
        const columnDefs = JSON_DATA.columns.map(col => generateColumnDef(col, true));
        createTableSQL += columnDefs.join(',\n');
        createTableSQL += ',\n_loaded_at TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()';
        
        // Add primary key if exists
        if (JSON_DATA.keys.primary_key && JSON_DATA.keys.primary_key.length > 0) {
            createTableSQL += `,\nPRIMARY KEY (${JSON_DATA.keys.primary_key.join(', ')})`;
        }
        
        createTableSQL += '\n);';
        
        // Check if table exists and get current schema
        var checkTableSQL = `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = UPPER('${targetTableName}');`;
        
        // Create a map of existing columns (case-insensitive)
        var existingColumns = new Map();
        var checkTableStmt = snowflake.createStatement({
            sqlText: checkTableSQL
        });
        var tableResult = checkTableStmt.execute();
        while (tableResult.next()) {
            existingColumns.set(tableResult.getColumnValue(1).toUpperCase(), {
                dataType: tableResult.getColumnValue(2),
                maxLength: tableResult.getColumnValue(3)
            });
        }
        
        // Generate ALTER TABLE statements for new columns
        var alterTableSQL = '';
        var needsAlter = false;
        JSON_DATA.columns.forEach(column => {
            if (!existingColumns.has(column.name.toUpperCase())) {
                alterTableSQL += `ALTER TABLE ${targetTableName} ADD COLUMN IF NOT EXISTS ${generateColumnDef(column, false)};\n`;
                needsAlter = true;
            }
        });
        
        // Generate INSERT or COPY statement based on whether we need to alter the table
        var dataLoadSQL;
        if (!JSON_DATA.keys.primary_key || JSON_DATA.keys.primary_key.length === 0) {
            throw "Error: Primary key is required for MERGE operation";
        }

        // Create a temporary table to hold the new data
        var tempTableName = `${targetTableName}_TEMP`;
        dataLoadSQL = `-- Create temporary table with same structure as target\n`;
        dataLoadSQL += `CREATE OR REPLACE TEMPORARY TABLE ${tempTableName} (\n`;
        dataLoadSQL += columnDefs.join(',\n');
        dataLoadSQL += '\n);\n\n';

        // Insert the JSON data into the temporary table
        const escapedJSONData = JSON.stringify(JSON_DATA.data)
                .replace(/\\/g, '\\\\')
                .replace(/'/g, "\\'");

        // Add column list for INSERT
        const columnNames = JSON_DATA.columns.map(col => col.name).join(',\n    ');
        const columnValues = JSON_DATA.columns.map(col => {
            if (col.data_type.toUpperCase() === 'JSON') {
                return `PARSE_JSON(f.value:${col.name}::STRING)`;
            }
            return `f.value:${col.name}::${getSQLDataType(col.data_type, col.max_length)}`;
        }).join(',\n    ');

        dataLoadSQL += `-- Load JSON data into temporary table\n`;
        dataLoadSQL += `INSERT INTO ${tempTableName} (${columnNames})\n`;
        dataLoadSQL += `SELECT\n    ${columnValues}\n`;
        dataLoadSQL += `FROM TABLE(FLATTEN(input => PARSE_JSON('${escapedJSONData}'))) f;\n\n`;

        // Generate MERGE statement
        const primaryKeyJoinCondition = JSON_DATA.keys.primary_key
            .map(key => `TARGET.${key} = SOURCE.${key}`)
            .join(' AND ');

        const updateSetClause = JSON_DATA.columns
            .filter(col => !JSON_DATA.keys.primary_key.includes(col.name))
            .map(col => `${col.name} = SOURCE.${col.name}`)
            .join(',\n    ');

        dataLoadSQL += `-- Perform MERGE operation\n`;
        dataLoadSQL += `MERGE INTO ${targetTableName} AS TARGET\n`;
        dataLoadSQL += `USING ${tempTableName} AS SOURCE\n`;
        dataLoadSQL += `ON ${primaryKeyJoinCondition}\n`;
        dataLoadSQL += `WHEN MATCHED THEN\n`;
        dataLoadSQL += `  UPDATE SET\n    ${updateSetClause}\n`;
        dataLoadSQL += `WHEN NOT MATCHED THEN\n`;
        dataLoadSQL += `  INSERT (${columnNames}, _loaded_at)\n`;
        dataLoadSQL += `  VALUES (${JSON_DATA.columns.map(col => `SOURCE.${col.name}`).join(',\n    ')},\n    CURRENT_TIMESTAMP(2));\n\n`;

        // Add logging statements
        dataLoadSQL += `-- Log MERGE operation results\n`;
        dataLoadSQL += `SELECT\n`;
        dataLoadSQL += `  'MERGE Results for ' || '${targetTableName}' as OPERATION,\n`;
        dataLoadSQL += `  SYSTEM$LAST_QUERY_ID() as QUERY_ID,\n`;
        dataLoadSQL += `  (SELECT COUNT(*) FROM ${tempTableName}) as SOURCE_ROWS,\n`;
        dataLoadSQL += `  SYSTEM$MERGE_ROWS_INSERTED() as ROWS_INSERTED,\n`;
        dataLoadSQL += `  SYSTEM$MERGE_ROWS_UPDATED() as ROWS_UPDATED,\n`;
        dataLoadSQL += `  SYSTEM$MERGE_ROWS_DELETED() as ROWS_DELETED;\n\n`;

        dataLoadSQL += `-- Clean up\n`;
        dataLoadSQL += `DROP TABLE IF EXISTS ${tempTableName};`;

        // Add to results
        allResults.push(`${debugInfo}

-- CREATE TABLE Statement:
${createTableSQL}

-- Check Table Schema Statement:
${checkTableSQL}

-- ALTER TABLE Statements:
${needsAlter ? alterTableSQL : 'No schema changes needed'}

-- Data Load Method: MERGE
-- Data Load Statement:
${dataLoadSQL}`);
    }
    
    if (allResults.length === 0) {
        return "Error: No valid JSON data found in the table";
    }
    
    // Return all results separated by a divider
    return allResults.join('\n\n-- ============================================\n\n');
$$;

-- Execute the procedure
CALL DEBUG_CREATE_TABLE();