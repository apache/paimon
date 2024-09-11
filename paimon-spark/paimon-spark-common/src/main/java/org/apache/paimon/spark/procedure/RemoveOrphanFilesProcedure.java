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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.operation.OrphanFilesClean.executeOrphanFilesClean;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
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
                ProcedureParameter.optional("parallelism", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", StringType, true, Metadata.empty())
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
        String parallelism = args.isNullAt(3) ? null : args.getString(3);
        Map<String, String> dynamicOptions =
                StringUtils.isNullOrWhitespaceOnly(parallelism)
                        ? Collections.emptyMap()
                        : new HashMap<String, String>() {
                            {
                                put(CoreOptions.DELETE_FILE_THREAD_NUM.key(), parallelism);
                            }
                        };

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

        List<OrphanFilesClean> tableCleans;
        try {
            tableCleans =
                    OrphanFilesClean.createOrphanFilesCleans(
                            ((WithPaimonCatalog) tableCatalog()).paimonCatalog(),
                            dynamicOptions,
                            identifier.getDatabaseName(),
                            identifier.getObjectName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String olderThan = args.isNullAt(1) ? null : args.getString(1);
        if (!StringUtils.isNullOrWhitespaceOnly(olderThan)) {
            tableCleans.forEach(clean -> clean.olderThan(olderThan));
        }

        boolean dryRun = !args.isNullAt(2) && args.getBoolean(2);
        if (dryRun) {
            tableCleans.forEach(clean -> clean.fileCleaner(path -> {}));
        }

        String[] result = executeOrphanFilesClean(tableCleans);
        List<InternalRow> rows = new ArrayList<>();
        Arrays.stream(result)
                .forEach(line -> rows.add(newInternalRow(UTF8String.fromString(line))));

        return rows.toArray(new InternalRow[0]);
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
