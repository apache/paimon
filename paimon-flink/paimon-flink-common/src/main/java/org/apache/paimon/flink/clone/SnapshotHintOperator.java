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

package org.apache.paimon.flink.clone;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/** Create snapshot hint files after copying a table. */
public class SnapshotHintOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo>, BoundedOneInput {

    private final Map<String, String> targetCatalogConfig;

    private Catalog targetCatalog;
    private Set<String> identifiers;

    public SnapshotHintOperator(Map<String, String> targetCatalogConfig) {
        this.targetCatalogConfig = targetCatalogConfig;
    }

    @Override
    public void open() throws Exception {
        targetCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(targetCatalogConfig));
        identifiers = new HashSet<>();
    }

    @Override
    public void processElement(StreamRecord<CloneFileInfo> streamRecord) throws Exception {
        String identifier = streamRecord.getValue().getTargetIdentifier();
        identifiers.add(identifier);
    }

    @Override
    public void endInput() throws Exception {
        for (String identifier : identifiers) {
            FileStoreTable targetTable =
                    (FileStoreTable) targetCatalog.getTable(Identifier.fromString(identifier));
            commitSnapshotHintInTargetTable(targetTable.snapshotManager());
        }
    }

    private void commitSnapshotHintInTargetTable(SnapshotManager targetTableSnapshotManager)
            throws IOException {
        OptionalLong optionalSnapshotId =
                targetTableSnapshotManager.safelyGetAllSnapshots().stream()
                        .mapToLong(Snapshot::id)
                        .max();
        if (optionalSnapshotId.isPresent()) {
            long snapshotId = optionalSnapshotId.getAsLong();
            targetTableSnapshotManager.commitEarliestHint(snapshotId);
            targetTableSnapshotManager.commitLatestHint(snapshotId);
            for (Snapshot snapshot : targetTableSnapshotManager.safelyGetAllSnapshots()) {
                if (snapshot.id() != snapshotId) {
                    targetTableSnapshotManager
                            .fileIO()
                            .deleteQuietly(targetTableSnapshotManager.snapshotPath(snapshot.id()));
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (targetCatalog != null) {
            targetCatalog.close();
        }
    }
}
