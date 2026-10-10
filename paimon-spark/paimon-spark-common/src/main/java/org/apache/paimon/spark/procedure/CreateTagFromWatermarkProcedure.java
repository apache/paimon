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

/** The procedure supports creating tags from snapshot watermarks. */
public class CreateTagFromWatermarkProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("tag", StringType),
                ProcedureParameter.required("watermark", LongType),
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

    private CreateTagFromWatermarkProcedure(TableCatalog tableCatalog) {
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
        Long watermark = args.getLong(2);
        Duration timeRetained =
                args.isNullAt(3) ? null : TimeUtils.parseDuration(args.getString(3));

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
                    Snapshot snapshot = snapshotManager.laterOrEqualWatermark(watermark);

                    Set<Snapshot> sortedTagsSnapshots = fileStoreTable.tagManager().tags().keySet();
                    for (Snapshot tagSnapshot : sortedTagsSnapshots) {
                        if (tagSnapshot.watermark() != null
                                && watermark <= tagSnapshot.watermark()) {
                            // Equal watermarks keep the earlier snapshot. Comparing with <=
                            // would replace an earlier live snapshot with a later tag.
                            if (snapshot == null
                                    || snapshot.watermark() == null
                                    || tagSnapshot.watermark() < snapshot.watermark()
                                    || (tagSnapshot.watermark().equals(snapshot.watermark())
                                            && tagSnapshot.id() < snapshot.id())) {
                                snapshot = tagSnapshot;
                            }
                            break;
                        }
                    }

                    SnapshotNotExistException.checkNotNull(
                            snapshot,
                            String.format(
                                    "Could not find any snapshot whose watermark later than %s.",
                                    watermark));

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
        return new BaseProcedure.Builder<CreateTagFromWatermarkProcedure>() {
            @Override
            public CreateTagFromWatermarkProcedure doBuild() {
                return new CreateTagFromWatermarkProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "CreateTagFromWatermarkProcedure";
    }
}
