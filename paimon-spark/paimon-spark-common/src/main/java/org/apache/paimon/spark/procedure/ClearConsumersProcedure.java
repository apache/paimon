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

import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.regex.Pattern;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Clear consumers procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  -- clear all consumers in the table
 *  CALL sys.clear_consumers('tableId')
 *
 * -- clear some consumers in the table (accept regular expression)
 *  CALL sys.clear_consumers('tableId', 'includingConsumers')
 *
 * -- exclude some consumers (accept regular expression)
 *  CALL sys.clear_consumers('tableId', 'includingConsumers', 'excludingConsumers')
 * </code></pre>
 */
public class ClearConsumersProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("includingConsumers", StringType),
                ProcedureParameter.optional("excludingConsumers", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected ClearConsumersProcedure(TableCatalog tableCatalog) {
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
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String includingConsumers = args.isNullAt(1) ? null : args.getString(1);
        String excludingConsumers = args.isNullAt(2) ? null : args.getString(2);
        Pattern includingPattern =
                StringUtils.isNullOrWhitespaceOnly(includingConsumers)
                        ? Pattern.compile(".*")
                        : Pattern.compile(includingConsumers);
        Pattern excludingPattern =
                StringUtils.isNullOrWhitespaceOnly(excludingConsumers)
                        ? null
                        : Pattern.compile(excludingConsumers);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    ConsumerManager consumerManager =
                            new ConsumerManager(
                                    fileStoreTable.fileIO(),
                                    fileStoreTable.location(),
                                    fileStoreTable.snapshotManager().branch());
                    consumerManager.clearConsumers(includingPattern, excludingPattern);

                    InternalRow outputRow = newInternalRow(true);
                    return new InternalRow[] {outputRow};
                });
    }

    public static ProcedureBuilder builder() {
        return new Builder<ClearConsumersProcedure>() {
            @Override
            public ClearConsumersProcedure doBuild() {
                return new ClearConsumersProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ClearConsumersProcedure";
    }
}
