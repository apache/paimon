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
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.ExpireChangelogImpl;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ProcedureUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** A procedure to expire changelogs. */
public class ExpireChangelogsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("retain_max", IntegerType),
                ProcedureParameter.optional("retain_min", IntegerType),
                ProcedureParameter.optional("older_than", StringType),
                ProcedureParameter.optional("max_deletes", IntegerType),
                ProcedureParameter.optional("delete_all", BooleanType),
                ProcedureParameter.optional("options", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "deleted_changelogs_count", IntegerType, false, Metadata.empty())
                    });

    private ExpireChangelogsProcedure(TableCatalog tableCatalog) {
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
        Integer retainMax = args.isNullAt(1) ? null : args.getInt(1);
        Integer retainMin = args.isNullAt(2) ? null : args.getInt(2);
        String olderThanStr = args.isNullAt(3) ? null : args.getString(3);
        Integer maxDeletes = args.isNullAt(4) ? null : args.getInt(4);
        boolean deleteAll = !args.isNullAt(5) && args.getBoolean(5);
        String options = args.isNullAt(6) ? null : args.getString(6);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    checkArgument(
                            table instanceof FileStoreTable,
                            "Only FileStoreTable supports expire_changelogs procedure. The table type is '%s'.",
                            table.getClass().getName());

                    if (deleteAll) {
                        checkArgument(
                                retainMax == null
                                        && retainMin == null
                                        && olderThanStr == null
                                        && maxDeletes == null
                                        && StringUtils.isNullOrWhitespaceOnly(options),
                                "delete_all cannot be used with retain_max, retain_min, older_than, max_deletes or options.");
                        ExpireChangelogImpl expireChangelogs =
                                (ExpireChangelogImpl) table.newExpireChangelog();
                        int deleted = expireChangelogs.expireAllDeletedCount();
                        return new InternalRow[] {newInternalRow(deleted)};
                    }

                    HashMap<String, String> dynamicOptions = new HashMap<>();
                    ProcedureUtils.putAllOptions(dynamicOptions, options);
                    table = table.copy(dynamicOptions);
                    ExpireChangelogImpl expireChangelogs =
                            (ExpireChangelogImpl) table.newExpireChangelog();
                    CoreOptions tableOptions = ((FileStoreTable) table).store().options();
                    ExpireConfig.Builder builder =
                            ProcedureUtils.fillInChangelogOptions(
                                    tableOptions, retainMax, retainMin, olderThanStr, maxDeletes);
                    int deleted = expireChangelogs.config(builder.build()).expire();
                    return new InternalRow[] {newInternalRow(deleted)};
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ExpireChangelogsProcedure>() {
            @Override
            public ExpireChangelogsProcedure doBuild() {
                return new ExpireChangelogsProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ExpireChangelogsProcedure";
    }
}
