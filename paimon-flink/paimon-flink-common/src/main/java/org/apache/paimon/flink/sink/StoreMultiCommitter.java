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

package org.apache.paimon.flink.sink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** {@link Committer} for dynamic store. */
public class StoreMultiCommitter implements Committer {

    private final Catalog catalog;
    private TableCommitImpl commit;
    private Map<Identifier, TableCommitImpl> tableCommits;
    private Map<Identifier, StoreCommitter> tableCommitters;
    private Lock.Factory lockFactory = Lock.emptyFactory();
    private StoreCommitter committer;

    public StoreMultiCommitter(Catalog catalog) {
        this.catalog = catalog;
        this.tableCommitters = new HashMap<>();
    }

    @Override
    public List<ManifestCommittable> filterRecoveredCommittables(
            List<ManifestCommittable> globalCommittables) {
        List<ManifestCommittable> res = new LinkedList<>();
        // key by table id
        Map<Identifier, List<ManifestCommittable>> committableMap =
                globalCommittables.stream()
                        .collect(Collectors.groupingBy(StoreMultiCommitter::getTableId));

        for (Map.Entry<Identifier, List<ManifestCommittable>> entry : committableMap.entrySet()) {
            Identifier tableId = entry.getKey();
            List<ManifestCommittable> committableList = entry.getValue();
            StoreCommitter committer = getStoreCommitter(tableId, committableList.get(0));
            List<ManifestCommittable> filteredCommittables =
                    committer.filterRecoveredCommittables(committableList);
            res.addAll(filteredCommittables);
        }

        return res;
    }

    @Override
    public ManifestCommittable combine(
            long checkpointId, long watermark, List<Committable> committables) {
        ManifestCommittable manifestCommittable = new ManifestCommittable(checkpointId, watermark);
        for (Committable committable : committables) {
            MultiTableCommittable multiTableCommittable = (MultiTableCommittable) committable;
            switch (committable.kind()) {
                case FILE:
                    CommitMessage file = (CommitMessage) committable.wrappedCommittable();
                    manifestCommittable.addFileCommittable(file);
                    break;
                case LOG_OFFSET:
                    LogOffsetCommittable offset =
                            (LogOffsetCommittable) committable.wrappedCommittable();
                    manifestCommittable.addLogOffset(offset.bucket(), offset.offset());
                    break;
            }
            manifestCommittable.setDatabase(multiTableCommittable.getDatabase());
            manifestCommittable.setTable(multiTableCommittable.getTable());
            manifestCommittable.setCommitUser(multiTableCommittable.getCommitUser());
        }
        return manifestCommittable;
    }

    @Override
    public void commit(List<ManifestCommittable> committables)
            throws IOException, InterruptedException {

        // key by table id
        Map<Identifier, List<ManifestCommittable>> committableMap =
                committables.stream()
                        .collect(Collectors.groupingBy(StoreMultiCommitter::getTableId));

        for (Map.Entry<Identifier, List<ManifestCommittable>> entry : committableMap.entrySet()) {
            Identifier tableId = entry.getKey();
            List<ManifestCommittable> committableList = entry.getValue();
            for (ManifestCommittable committable : committableList) {
                StoreCommitter committer = getStoreCommitter(tableId, committable);
                committer.commit(committables);
            }
        }
    }

    private StoreCommitter getStoreCommitter(Identifier tableId, ManifestCommittable committable) {
        StoreCommitter committer =
                tableCommitters.computeIfAbsent(
                        tableId,
                        id -> {
                            try {
                                FileStoreTable table = (FileStoreTable) catalog.getTable(id);
                                return new StoreCommitter(
                                        table.newCommit(committable.getCommitUser())
                                                .withLock(lockFactory.create())
                                                .ignoreEmptyCommit(false));
                            } catch (Catalog.TableNotExistException e) {
                                return null;
                            }
                        });

        if (committer == null) {
            throw new RuntimeException(
                    String.format(
                            "Failed to get committer for table %s.%s.",
                            committable.getDatabase(), committable.getTable()));
        }
        return committer;
    }

    public static Identifier getTableId(ManifestCommittable committable) {
        return Identifier.create(committable.getDatabase(), committable.getTable());
    }

    @Override
    public void close() throws Exception {
        for (StoreCommitter committer : tableCommitters.values()) {
            committer.close();
        }
    }
}
