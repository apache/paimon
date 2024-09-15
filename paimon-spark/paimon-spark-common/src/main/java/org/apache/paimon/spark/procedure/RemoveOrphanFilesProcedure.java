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
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.LocalOrphanFilesClean;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.api.java.JavaSparkContext;
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
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
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
                ProcedureParameter.optional("parallelism", IntegerType)
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

        List<LocalOrphanFilesClean> tableCleans;
        try {
            Catalog catalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();
            tableCleans =
                    LocalOrphanFilesClean.createOrphanFilesCleans(
                            catalog,
                            identifier.getDatabaseName(),
                            identifier.getObjectName(),
                            OrphanFilesClean.olderThanMillis(
                                    args.isNullAt(1) ? null : args.getString(1)),
                            OrphanFilesClean.createFileCleaner(
                                    catalog, !args.isNullAt(2) && args.getBoolean(2)),
                            args.isNullAt(3) ? null : args.getInt(3));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        int parallelism =
                args.isNullAt(3) ? Runtime.getRuntime().availableProcessors() : args.getInt(3);
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark().sparkContext());
        List<Path> rets =
                javaSparkContext
                        .parallelize(tableCleans, parallelism)
                        .mapPartitions(
                                iterator -> {
                                    ArrayList<Path> paths = new ArrayList<>();
                                    while (iterator.hasNext()) {
                                        LocalOrphanFilesClean fileClean = iterator.next();
                                        paths.addAll(fileClean.clean());
                                    }
                                    return paths.iterator();
                                })
                        .collect();
        String[] result =
                LocalOrphanFilesClean.showDeletedFiles(rets, LocalOrphanFilesClean.SHOW_LIMIT)
                        .toArray(new String[0]);

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
