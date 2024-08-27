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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Set;

/** The procedure supports creating tags from snapshots watermark. */
public class CreateTagFromWatermarkProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "create_tag_from_watermark";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "tag", type = @DataTypeHint(value = "STRING")),
                @ArgumentHint(name = "timestamp", type = @DataTypeHint("bigint")),
                @ArgumentHint(
                        name = "time_retained",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
            })
    @DataTypeHint("ROW< tagName STRING, snapshot BIGINT, `commit_time` BIGINT, `watermark` STRING>")
    public Row[] call(
            ProcedureContext procedureContext,
            String tableId,
            String tagName,
            Long timestamp,
            @Nullable String timeRetained)
            throws Catalog.TableNotExistException {
        FileStoreTable fileStoreTable = (FileStoreTable) table(tableId);
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();

        Snapshot snapshot = snapshotManager.laterOrEqualWatermark(timestamp);

        Set<Snapshot> sortedTagsSnapshots = fileStoreTable.tagManager().tags().keySet();

        for (Snapshot tagSnapshot : sortedTagsSnapshots) {
            if (tagSnapshot.watermark() != null && timestamp <= tagSnapshot.watermark()) {
                if (snapshot == null
                        || snapshot.watermark() == null
                        || tagSnapshot.watermark() < snapshot.watermark()) {
                    snapshot = tagSnapshot;
                }
                break;
            }
        }

        SnapshotNotExistException.checkNotNull(
                snapshot,
                String.format(
                        "Could not find any snapshot whose watermark later than %s.", timestamp));

        fileStoreTable.createTag(tagName, snapshot.id(), toDuration(timeRetained));

        return new Row[] {
            Row.of(
                    tagName,
                    snapshot.id(),
                    snapshot.timeMillis(),
                    String.valueOf(snapshot.watermark()))
        };
    }

    @Nullable
    private static Duration toDuration(@Nullable String s) {
        if (s == null) {
            return null;
        }

        return TimeUtils.parseDuration(s);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
