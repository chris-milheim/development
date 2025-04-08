CREATE OR REPLACE PROCEDURE CREATE_TABLE_FROM_JSON()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Get all rows from the table
    var getJSONData = snowflake.createStatement({
        sqlText: `SELECT * FROM APRICOT_DEV_RAW.COMMON.dev_cvi_raw_poc`
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
                default:
                    return dataType.toUpperCase();
            }
        }

        // Function to generate column definition
        function generateColumnDef(column, includeDefaults = true) {
            let def = `${column.name} ${getSQLDataType(column.data_type, column.max_length)}`;
            if (!column.nullable) {
                def += ' NOT NULL';
            }
            if (includeDefaults && column.default_value !== null) {
                if (column.data_type.toUpperCase() === 'TIMESTAMP') {
                    // For TIMESTAMP, cast the default value to TIMESTAMP
                    def += ` DEFAULT TIMESTAMP_NTZ_FROM_PARTS(1970, 1, 1, 0, 0, 0)`;
                } else {
                    def += ` DEFAULT ${column.default_value === '' ? "''" : column.default_value}`;
                }
            }
            return def;
        }

        // Generate CREATE TABLE statement
        let createTableSQL = `CREATE TABLE IF NOT EXISTS ${targetTableName} (\n`;
        
        // Add columns
        const columnDefs = JSON_DATA.columns.map(col => generateColumnDef(col, true));
        createTableSQL += columnDefs.join(',\n');
        
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
            
            // Insert data using appropriate method based on whether we needed to alter the table
            if (needsAlter || existingColumns.size === 0) {
                // Use INSERT INTO when we've altered the table or created it
                var insertSQL = `INSERT INTO ${targetTableName}\n`;
                insertSQL += `SELECT\n`;
                
                // Add column list for INSERT
                const columnList = JSON_DATA.columns.map(col => 
                    `f.value:${col.name}::${getSQLDataType(col.data_type, col.max_length)}`
                ).join(',\n    ');
                
                insertSQL += `    ${columnList}\n`;
                insertSQL += `FROM TABLE(FLATTEN(input => PARSE_JSON('${JSON.stringify(JSON_DATA.data)}'))) f;`;
                
                var insertStmt = snowflake.createStatement({
                    sqlText: insertSQL
                });
                insertStmt.execute();
            } else {
                // Use COPY INTO when no schema changes were needed
                // Create a temporary table to stage the JSON data
                var tempTableName = `${targetTableName}_TEMP`;
                var createTempTableSQL = `CREATE OR REPLACE TEMPORARY TABLE ${tempTableName} (data VARIANT);`;
                var createTempTableStmt = snowflake.createStatement({
                    sqlText: createTempTableSQL
                });
                createTempTableStmt.execute();
                
                // Insert JSON data into temporary table
                var insertTempSQL = `INSERT INTO ${tempTableName} SELECT PARSE_JSON('${JSON.stringify(JSON_DATA.data)}');`;
                var insertTempStmt = snowflake.createStatement({
                    sqlText: insertTempSQL
                });
                insertTempStmt.execute();
                
                // Create a temporary stage
                var stageName = `${targetTableName}_STAGE`;
                var createStageSQL = `CREATE OR REPLACE STAGE ${stageName};`;
                var createStageStmt = snowflake.createStatement({
                    sqlText: createStageSQL
                });
                createStageStmt.execute();
                
                // Copy data from temporary table to stage
                var copyToStageSQL = `COPY INTO @${stageName}/data.json FROM ${tempTableName} FILE_FORMAT = (TYPE = 'JSON');`;
                var copyToStageStmt = snowflake.createStatement({
                    sqlText: copyToStageSQL
                });
                copyToStageStmt.execute();
                
                // Use COPY INTO with the stage
                var copySQL = `COPY INTO ${targetTableName}\n`;
                copySQL += `FROM @${stageName}\n`;
                copySQL += `FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)\n`;
                copySQL += `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n`;
                copySQL += `FORCE = TRUE;`;
                
                var copyStmt = snowflake.createStatement({
                    sqlText: copySQL
                });
                copyStmt.execute();
                
                // Clean up
                var dropStageSQL = `DROP STAGE IF EXISTS ${stageName};`;
                var dropStageStmt = snowflake.createStatement({
                    sqlText: dropStageSQL
                });
                dropStageStmt.execute();
                
                var dropTempTableSQL = `DROP TABLE IF EXISTS ${tempTableName};`;
                var dropTempTableStmt = snowflake.createStatement({
                    sqlText: dropTempTableSQL
                });
                dropTempTableStmt.execute();
            }
            
            // Add success message to results
            allResults.push(`${debugInfo}

-- Table ${targetTableName} created/altered and data loaded successfully
-- CREATE/ALTER TABLE Statement:
${existingColumns.size === 0 ? createTableSQL : (needsAlter ? alterTableSQL : 'No schema changes needed')}

-- Data Load Method: ${needsAlter || existingColumns.size === 0 ? 'INSERT INTO' : 'COPY INTO'}
-- Data Load Statement:
${needsAlter || existingColumns.size === 0 ? insertSQL : copySQL}`);
        } catch (err) {
            // Add error message to results
            allResults.push(`${debugInfo}

-- Error processing table ${targetTableName}:
-- ${err}
-- CREATE/ALTER TABLE Statement:
${existingColumns.size === 0 ? createTableSQL : (needsAlter ? alterTableSQL : 'No schema changes needed')}

-- Data Load Method: ${needsAlter || existingColumns.size === 0 ? 'INSERT INTO' : 'COPY INTO'}
-- Data Load Statement:
${needsAlter || existingColumns.size === 0 ? insertSQL : copySQL}`);
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