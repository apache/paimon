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
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.table.ExpireSnapshots;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

/** A procedure to expire snapshots. */
public class ExpireSnapshotsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("retain_max", IntegerType),
                ProcedureParameter.optional("retain_min", IntegerType),
                ProcedureParameter.optional("older_than", TimestampType),
                ProcedureParameter.optional("max_deletes", IntegerType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "deleted_snapshots_count", IntegerType, false, Metadata.empty())
                    });

    protected ExpireSnapshotsProcedure(TableCatalog tableCatalog) {
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
        Long olderThanMills = args.isNullAt(3) ? null : args.getLong(3) / 1000;
        Integer maxDeletes = args.isNullAt(4) ? null : args.getInt(4);
        return modifyPaimonTable(
                tableIdent,
                table -> {
                    Map<String, String> options = new HashMap<>(table.options());
                    if (retainMax != null) {
                        options.put(
                                CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(),
                                String.valueOf(retainMax));
                        if (!options.containsKey(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key())) {
                            options.put(
                                    CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(),
                                    String.valueOf(retainMax));
                        }
                    }
                    if (retainMin != null) {
                        options.put(
                                CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(),
                                String.valueOf(retainMin));
                        if (!options.containsKey(CoreOptions.CHANGELOG_NUM_RETAINED_MIN.key())) {
                            options.put(
                                    CoreOptions.CHANGELOG_NUM_RETAINED_MIN.key(),
                                    String.valueOf(retainMin));
                        }
                    }
                    if (olderThanMills != null) {
                        long snapshotRetainMs = System.currentTimeMillis() - olderThanMills;
                        options.put(
                                CoreOptions.SNAPSHOT_TIME_RETAINED.key(),
                                String.format("%dms", snapshotRetainMs));
                        if (!options.containsKey(CoreOptions.CHANGELOG_TIME_RETAINED.key())) {
                            options.put(
                                    CoreOptions.CHANGELOG_TIME_RETAINED.key(),
                                    String.format("%dms", snapshotRetainMs));
                        }
                    }
                    if (maxDeletes != null) {
                        options.put(
                                CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key(),
                                String.valueOf(maxDeletes));
                    }
                    CoreOptions newOption = new CoreOptions(options);
                    SchemaValidation.validateSnapshotAndChangelogRetainOption(newOption);
                    ExpireSnapshots expireSnapshots = table.newExpireSnapshots(newOption);
                    int deleted = expireSnapshots.expire();
                    return new InternalRow[] {newInternalRow(deleted)};
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ExpireSnapshotsProcedure>() {
            @Override
            public ExpireSnapshotsProcedure doBuild() {
                return new ExpireSnapshotsProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ExpireSnapshotsProcedure";
    }
}
