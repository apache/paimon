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

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.utils.TimeUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.Duration;
import java.util.Set;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** The procedure supports creating tags from snapshots commit-time or watermark. */
public class CreateTagFromTimestampProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("tag", StringType),
                ProcedureParameter.optional("timestamp", LongType),
                ProcedureParameter.optional("time_retained", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("tagName", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("snapshot", DataTypes.LongType, true, Metadata.empty()),
                        new StructField("commit_time", DataTypes.LongType, true, Metadata.empty()),
                        new StructField("watermark", DataTypes.StringType, true, Metadata.empty())
                    });

    protected CreateTagFromTimestampProcedure(TableCatalog tableCatalog) {
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
        String tag = args.getString(1);
        Long timestamp = args.getLong(2);
        Duration timeRetained =
                args.isNullAt(3) ? null : TimeUtils.parseDuration(args.getString(3));

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
                    Snapshot snapshot = snapshotManager.laterOrEqualTimeMills(timestamp);

                    Set<Snapshot> sortedTagsSnapshots = fileStoreTable.tagManager().tags().keySet();

                    // compare to tagsSnapshot.
                    for (Snapshot tagSnapshot : sortedTagsSnapshots) {
                        if (timestamp <= tagSnapshot.timeMillis()) {
                            if (snapshot == null
                                    || tagSnapshot.timeMillis() < snapshot.timeMillis()) {
                                snapshot = tagSnapshot;
                            }
                            break;
                        }
                    }

                    SnapshotNotExistException.checkNotNull(
                            snapshot,
                            String.format(
                                    "Could not find any snapshot whose commit-time later than %s.",
                                    timestamp));

                    fileStoreTable.createTag(tag, snapshot.id(), timeRetained);

                    InternalRow outputRow =
                            newInternalRow(
                                    UTF8String.fromString(tag),
                                    snapshot.id(),
                                    snapshot.timeMillis(),
                                    UTF8String.fromString(String.valueOf(snapshot.watermark())));

                    return new InternalRow[] {outputRow};
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<CreateTagFromTimestampProcedure>() {
            @Override
            public CreateTagFromTimestampProcedure doBuild() {
                return new CreateTagFromTimestampProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "CreateTagFromTimestampProcedure";
    }
}
