CREATE OR REPLACE PROCEDURE APRICOT_DEV_RAW.COMMON.CREATE_TABLE_FROM_JSON()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Get all rows from the table
    //  WHERE raw_json_data:metadata.tenant_id = 'apricot_103326' AND raw_json_data:metadata.source_table = 'data_16'
    var getJSONData = snowflake.createStatement({
        sqlText: `SELECT * FROM APRICOT_DEV_RAW.COMMON.dev_cvi_raw_poc WHERE _loaded_at > DATEADD(HOURS, -1, CURRENT_TIMESTAMP())`
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
            // f (!column.nullable) {
            //    def += ' NOT NULL';
            // 
            
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
        
        try {
            // Check if table exists and get current schema
            var checkTableSQL = `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
                                FROM INFORMATION_SCHEMA.COLUMNS 
                                WHERE TABLE_NAME = UPPER('${targetTableName}');`;
            var checkTableStmt = snowflake.createStatement({
                sqlText: checkTableSQL
            });
            var tableResult = checkTableStmt.execute();
            
            // Create a map of existing columns (case-insensitive)
            var existingColumns = new Map();
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
            
            // Execute CREATE TABLE if it doesn't exist, otherwise execute ALTER TABLE if needed
            if (existingColumns.size === 0) {
                // Table doesn't exist, create it
                var createTableStmt = snowflake.createStatement({
                    sqlText: createTableSQL
                });
                createTableStmt.execute();
            } else if (needsAlter) {
                // Table exists and needs new columns
                var alterTableStmt = snowflake.createStatement({
                    sqlText: alterTableSQL
                });
                alterTableStmt.execute();
            }

            // Check for primary key requirement
            if (!JSON_DATA.keys.primary_key || JSON_DATA.keys.primary_key.length === 0) {
                throw "Error: Primary key is required for MERGE operation";
            }

            // Create a temporary table to hold the new data
            var tempTableName = `${targetTableName}_TEMP`;
            var tempTableSQL = `CREATE OR REPLACE TEMPORARY TABLE ${tempTableName} (\n`;
            tempTableSQL += columnDefs.join(',\n');
            tempTableSQL += '\n);\n\n';

            var createTempTableStmt = snowflake.createStatement({
                sqlText: tempTableSQL
            });
            createTempTableStmt.execute();

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

            var insertTempSQL = `INSERT INTO ${tempTableName} (${columnNames})\n`;
            insertTempSQL += `SELECT\n    ${columnValues}\n`;
            insertTempSQL += `FROM TABLE(FLATTEN(input => PARSE_JSON('${escapedJSONData}'))) f;\n\n`;

            var insertTempStmt = snowflake.createStatement({
                sqlText: insertTempSQL
            });
            insertTempStmt.execute();

            // Generate MERGE statement
            const primaryKeyJoinCondition = JSON_DATA.keys.primary_key
                .map(key => `TARGET.${key} = SOURCE.${key}`)
                .join(' AND ');

            const updateSetClause = JSON_DATA.columns
                .filter(col => !JSON_DATA.keys.primary_key.includes(col.name))
                .map(col => `${col.name} = SOURCE.${col.name}`)
                .join(',\n    ');

            var mergeSQL = `MERGE INTO ${targetTableName} AS TARGET\n`;
            mergeSQL += `USING ${tempTableName} AS SOURCE\n`;
            mergeSQL += `ON ${primaryKeyJoinCondition}\n`;
            mergeSQL += `WHEN MATCHED THEN\n`;
            mergeSQL += `  UPDATE SET\n    ${updateSetClause}\n`;
            mergeSQL += `WHEN NOT MATCHED THEN\n`;
            mergeSQL += `  INSERT (${columnNames}, _loaded_at)\n`;
            mergeSQL += `  VALUES (${JSON_DATA.columns.map(col => `SOURCE.${col.name}`).join(',\n    ')},\n    CURRENT_TIMESTAMP(2));\n\n`;

            var mergeStmt = snowflake.createStatement({
                sqlText: mergeSQL
            });
            mergeStmt.execute();

            // Add logging statements
            //var loggingSQL = `SELECT\n`;
            //loggingSQL += `  'MERGE Results for ' || '${targetTableName}' as OPERATION,\n`;
            //loggingSQL += `  SYSTEM$LAST_QUERY_ID() as QUERY_ID,\n`;
            //loggingSQL += `  (SELECT COUNT(*) FROM ${tempTableName}) as SOURCE_ROWS,\n`;
            //loggingSQL += `  SYSTEM$MERGE_ROWS_INSERTED() as ROWS_INSERTED,\n`;
            //loggingSQL += `  SYSTEM$MERGE_ROWS_UPDATED() as ROWS_UPDATED,\n`;
            //loggingSQL += `  SYSTEM$MERGE_ROWS_DELETED() as ROWS_DELETED;\n\n`;
            //
            //var loggingStmt = snowflake.createStatement({
            //    sqlText: loggingSQL
            //});
            //loggingStmt.execute();

            // Clean up
            var dropTempTableSQL = `DROP TABLE IF EXISTS ${tempTableName};`;
            var dropTempTableStmt = snowflake.createStatement({
                sqlText: dropTempTableSQL
            });
            dropTempTableStmt.execute();
            
            // Add success message to results
            allResults.push(`${debugInfo}

-- Table ${targetTableName} created/altered and data loaded successfully
-- CREATE/ALTER TABLE Statement:
${existingColumns.size === 0 ? createTableSQL : (needsAlter ? alterTableSQL : 'No schema changes needed')}

-- Data Load Method: MERGE
-- Data Load Statement:
${mergeSQL}`);
        } catch (err) {
            // Add error message to results
            allResults.push(`${debugInfo}

-- Error processing table ${targetTableName}:
-- ${err}
-- CREATE/ALTER TABLE Statement:
${createTableSQL}

-- Attempted Operation:
${err.operation || 'Unknown operation'}`);
        }
    }
    
    if (allResults.length === 0) {
        return "Error: No valid JSON data found in the table";
    }
    
    // Return all results separated by a divider
    return allResults.join('\n\n-- ============================================\n\n');
$$;

-- Execute the procedure
CALL CREATE_TABLE_FROM_JSON();

--DROP TABLE IF EXISTS APRICOT_DEV_RAW.COMMON.apricot_116558_data_4;
--DROP TABLE IF EXISTS APRICOT_DEV_RAW.COMMON.apricot_116558_data_62;