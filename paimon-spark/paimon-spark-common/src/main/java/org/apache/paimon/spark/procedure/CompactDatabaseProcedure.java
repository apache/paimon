/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Compact database procedure. Usage:
 *
 * <pre><code>
 *  -- compact all databases
 *  CALL sys.compact_database()
 *
 *  -- compact some databases (accept regular expression)
 *  CALL sys.compact_database(including_databases => 'db1|db2')
 *
 *  -- compact some tables (accept regular expression)
 *  CALL sys.compact_database(including_databases => 'db1', including_tables => 'table1|table2')
 *
 *  -- exclude some tables (accept regular expression)
 *  CALL sys.compact_database(including_databases => 'db1', including_tables => '.*', excluding_tables => 'ignore_table')
 *
 *  -- set table options ('k=v,...')
 *  CALL sys.compact_database(including_databases => 'db1', options => 'key1=value1,key2=value2')
 * </code></pre>
 */
public class CompactDatabaseProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CompactDatabaseProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.optional("including_databases", StringType),
                ProcedureParameter.optional("including_tables", StringType),
                ProcedureParameter.optional("excluding_tables", StringType),
                ProcedureParameter.optional("options", StringType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.StringType, true, Metadata.empty())
                    });

    protected CompactDatabaseProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        String includingDatabases = args.isNullAt(0) ? ".*" : args.getString(0);
        String includingTables = args.isNullAt(1) ? ".*" : args.getString(1);
        String excludingTables = args.isNullAt(2) ? null : args.getString(2);
        String options = args.isNullAt(3) ? null : args.getString(3);

        Pattern databasePattern = Pattern.compile(includingDatabases);
        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                StringUtils.isNullOrWhitespaceOnly(excludingTables)
                        ? null
                        : Pattern.compile(excludingTables);

        Catalog paimonCatalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();

        int successCount = 0;
        int failedCount = 0;

        try {
            List<String> databases = paimonCatalog.listDatabases();
            for (String databaseName : databases) {
                Matcher databaseMatcher = databasePattern.matcher(databaseName);
                if (!databaseMatcher.matches()) {
                    LOG.debug("Database '{}' is excluded by pattern.", databaseName);
                    continue;
                }

                List<String> tables = paimonCatalog.listTables(databaseName);
                for (String tableName : tables) {
                    String fullTableName = String.format("%s.%s", databaseName, tableName);

                    if (!shouldCompactTable(fullTableName, includingPattern, excludingPattern)) {
                        LOG.debug("Table '{}' is excluded by pattern.", fullTableName);
                        continue;
                    }

                    try {
                        Table table =
                                paimonCatalog.getTable(Identifier.create(databaseName, tableName));
                        if (!(table instanceof FileStoreTable)) {
                            LOG.warn(
                                    "Only FileStoreTable supports compact action. "
                                            + "Table '{}' type is '{}'.",
                                    fullTableName,
                                    table.getClass().getName());
                            continue;
                        }

                        compactTable(fullTableName, options);
                        successCount++;
                        LOG.info("Successfully compacted table: {}", fullTableName);
                    } catch (Exception e) {
                        failedCount++;
                        LOG.error("Failed to compact table: {}", fullTableName, e);
                    }
                }
            }
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }

        String result =
                String.format(
                        "Compact database finished. Success: %d, Failed: %d",
                        successCount, failedCount);
        return new InternalRow[] {newInternalRow(UTF8String.fromString(result))};
    }

    private boolean shouldCompactTable(
            String fullTableName, Pattern includingPattern, Pattern excludingPattern) {
        boolean shouldCompact = includingPattern.matcher(fullTableName).matches();
        if (excludingPattern != null) {
            shouldCompact = shouldCompact && !excludingPattern.matcher(fullTableName).matches();
        }
        return shouldCompact;
    }

    private void compactTable(String tableName, String options) throws Exception {
        LOG.info("Start to compact table: {}", tableName);

        // Build CompactProcedure and call it for each table
        CompactProcedure compactProcedure =
                (CompactProcedure)
                        CompactProcedure.builder().withTableCatalog(tableCatalog()).build();

        // Create InternalRow with the parameters for CompactProcedure
        // Parameters: table, partitions, compact_strategy, order_strategy, order_by, where,
        // options, partition_idle_time
        InternalRow compactArgs =
                newInternalRow(
                        UTF8String.fromString(tableName), // table
                        null, // partitions
                        null, // compact_strategy
                        null, // order_strategy
                        null, // order_by
                        null, // where
                        options == null ? null : UTF8String.fromString(options), // options
                        null // partition_idle_time
                        );

        InternalRow[] result = compactProcedure.call(compactArgs);

        if (result.length > 0 && !result[0].getBoolean(0)) {
            throw new RuntimeException("Compact failed for table: " + tableName);
        }
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<CompactDatabaseProcedure>() {
            @Override
            public CompactDatabaseProcedure doBuild() {
                return new CompactDatabaseProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "This procedure executes compact action on all tables in database(s).";
    }
}
