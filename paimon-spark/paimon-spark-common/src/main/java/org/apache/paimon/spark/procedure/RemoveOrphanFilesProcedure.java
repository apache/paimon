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
import org.apache.paimon.operation.LocalOrphanFilesClean;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.spark.orphan.SparkOrphanFilesClean;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Remove orphan files procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.remove_orphan_files(table => 'tableId', [older_than => '2023-10-31 12:00:00'])
 *
 *  CALL sys.remove_orphan_files(table => 'databaseName.*', [older_than => '2023-10-31 12:00:00'])
 * </code></pre>
 */
public class RemoveOrphanFilesProcedure extends BaseProcedure {

    private static final Logger LOG =
            LoggerFactory.getLogger(RemoveOrphanFilesProcedure.class.getName());

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("older_than", StringType),
                ProcedureParameter.optional("dry_run", BooleanType),
                ProcedureParameter.optional("parallelism", IntegerType),
                ProcedureParameter.optional("mode", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", LongType, true, Metadata.empty())
                    });

    private RemoveOrphanFilesProcedure(TableCatalog tableCatalog) {
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
        org.apache.paimon.catalog.Identifier identifier;
        String tableId = args.getString(0);
        Preconditions.checkArgument(
                tableId != null && !tableId.isEmpty(),
                "Cannot handle an empty tableId for argument %s",
                tableId);

        if (tableId.endsWith(".*")) {
            identifier = org.apache.paimon.catalog.Identifier.fromString(tableId);
        } else {
            identifier =
                    org.apache.paimon.catalog.Identifier.fromString(
                            toIdentifier(args.getString(0), PARAMETERS[0].name()).toString());
        }
        LOG.info("identifier is {}.", identifier);

        Catalog catalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();
        String mode = args.isNullAt(4) ? "DISTRIBUTED" : args.getString(4);

        long deletedFiles;
        try {
            switch (mode.toUpperCase(Locale.ROOT)) {
                case "LOCAL":
                    deletedFiles =
                            LocalOrphanFilesClean.executeDatabaseOrphanFiles(
                                    catalog,
                                    identifier.getDatabaseName(),
                                    identifier.getTableName(),
                                    OrphanFilesClean.olderThanMillis(
                                            args.isNullAt(1) ? null : args.getString(1)),
                                    OrphanFilesClean.createFileCleaner(
                                            catalog, !args.isNullAt(2) && args.getBoolean(2)),
                                    args.isNullAt(3) ? null : args.getInt(3));
                    break;
                case "DISTRIBUTED":
                    deletedFiles =
                            SparkOrphanFilesClean.executeDatabaseOrphanFiles(
                                    catalog,
                                    identifier.getDatabaseName(),
                                    identifier.getTableName(),
                                    OrphanFilesClean.olderThanMillis(
                                            args.isNullAt(1) ? null : args.getString(1)),
                                    OrphanFilesClean.createFileCleaner(
                                            catalog, !args.isNullAt(2) && args.getBoolean(2)),
                                    args.isNullAt(3) ? null : args.getInt(3));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported mode: " + mode);
            }
            return new InternalRow[] {newInternalRow(deletedFiles)};
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<RemoveOrphanFilesProcedure>() {
            @Override
            public RemoveOrphanFilesProcedure doBuild() {
                return new RemoveOrphanFilesProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "RemoveOrphanFilesProcedure";
    }
}
