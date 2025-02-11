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

import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.utils.Preconditions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Procedure to remove unexisting data files from manifest entries. See {@code
 * RemoveUnexistingFilesAction} in {@code paimon-flink-common} module for detailed use cases.
 *
 * <pre><code>
 *  -- remove unexisting data files in table `mydb.myt`
 *  CALL sys.remove_unexisting_files(`table` => 'mydb.myt')
 *
 *  -- only check what files will be removed, but not really remove them (dry run)
 *  CALL sys.remove_unexisting_files(`table` => 'mydb.myt', `dry_run` = true)
 * </code></pre>
 *
 * <p>Note that user is on his own risk using this procedure, which may cause data loss when used
 * outside from the use cases above.
 */
public class RemoveUnexistingFilesProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(RemoveUnexistingFilesProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.optional("dry_run", DataTypes.BooleanType),
                ProcedureParameter.optional("parallelism", DataTypes.IntegerType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("fileName", DataTypes.StringType, false, Metadata.empty())
                    });

    private RemoveUnexistingFilesProcedure(TableCatalog tableCatalog) {
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
        String tableId = args.getString(0);
        Preconditions.checkArgument(
                tableId != null && !tableId.isEmpty(),
                "Cannot handle an empty tableId for argument %s",
                tableId);
        org.apache.paimon.catalog.Identifier identifier =
                org.apache.paimon.catalog.Identifier.fromString(
                        toIdentifier(args.getString(0), PARAMETERS[0].name()).toString());
        LOG.info("identifier is {}.", identifier);

        String[] result =
                SparkRemoveUnexistingFiles.execute(
                        ((WithPaimonCatalog) tableCatalog()).paimonCatalog(),
                        identifier.getDatabaseName(),
                        identifier.getTableName(),
                        !args.isNullAt(1) && args.getBoolean(1),
                        args.isNullAt(2) ? null : args.getInt(2));
        return Arrays.stream(result)
                .map(path -> newInternalRow(UTF8String.fromString(path)))
                .toArray(InternalRow[]::new);
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<RemoveUnexistingFilesProcedure>() {
            @Override
            public RemoveUnexistingFilesProcedure doBuild() {
                return new RemoveUnexistingFilesProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "RemoveUnexistingFilesProcedure";
    }
}
