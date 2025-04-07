CREATE OR REPLACE PROCEDURE DEBUG_CREATE_TABLE()
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
                    // For TIMESTAMP, use a valid default like '1970-01-01 00:00:00'
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
        if (needsAlter || existingColumns.size === 0) {
            // Use INSERT INTO when we've altered the table or created it
            dataLoadSQL = `INSERT INTO ${targetTableName}\n`;
            dataLoadSQL += `SELECT\n`;
            
            // Add column list for INSERT
            const columnList = JSON_DATA.columns.map(col => 
                `f.value:${col.name}::${getSQLDataType(col.data_type, col.max_length)}`
            ).join(',\n    ');
            
            dataLoadSQL += `    ${columnList}\n`;
            dataLoadSQL += `FROM TABLE(FLATTEN(input => PARSE_JSON('${JSON.stringify(JSON_DATA.data)}'))) f;`;
        } else {
            // Use COPY INTO when no schema changes were needed
            var tempTableName = `${targetTableName}_TEMP`;
            var stageName = `${targetTableName}_STAGE`;
            
            dataLoadSQL = `-- Create temporary table\n`;
            dataLoadSQL += `CREATE OR REPLACE TEMPORARY TABLE ${tempTableName} (data VARIANT);\n\n`;
            
            dataLoadSQL += `-- Insert JSON data into temporary table\n`;
            dataLoadSQL += `INSERT INTO ${tempTableName} SELECT PARSE_JSON('${JSON.stringify(JSON_DATA.data)}');\n\n`;
            
            dataLoadSQL += `-- Create stage\n`;
            dataLoadSQL += `CREATE OR REPLACE STAGE ${stageName};\n\n`;
            
            dataLoadSQL += `-- Copy data from temporary table to stage\n`;
            dataLoadSQL += `COPY INTO @${stageName}/data.json FROM ${tempTableName} FILE_FORMAT = (TYPE = 'JSON');\n\n`;
            
            dataLoadSQL += `-- Copy data from stage to target table\n`;
            dataLoadSQL += `COPY INTO ${targetTableName}\n`;
            dataLoadSQL += `FROM @${stageName}\n`;
            dataLoadSQL += `FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)\n`;
            dataLoadSQL += `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n`;
            dataLoadSQL += `FORCE = TRUE;\n\n`;
            
            dataLoadSQL += `-- Clean up\n`;
            dataLoadSQL += `DROP STAGE IF EXISTS ${stageName};\n`;
            dataLoadSQL += `DROP TABLE IF EXISTS ${tempTableName};`;
        }
        
        // Add to results
        allResults.push(`${debugInfo}

-- CREATE TABLE Statement:
${createTableSQL}

-- Check Table Schema Statement:
${checkTableSQL}

-- ALTER TABLE Statements:
${needsAlter ? alterTableSQL : 'No schema changes needed'}

-- Data Load Method: ${needsAlter || existingColumns.size === 0 ? 'INSERT INTO' : 'COPY INTO'}
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