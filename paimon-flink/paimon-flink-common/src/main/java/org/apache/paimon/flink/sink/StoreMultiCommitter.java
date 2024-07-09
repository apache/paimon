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
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link StoreMultiCommitter} for multiple dynamic store. During the commit process, it will group
 * the WrappedManifestCommittables by their table identifier and use different committers to commit
 * to different tables.
 */
public class StoreMultiCommitter
        implements Committer<MultiTableCommittable, WrappedManifestCommittable> {

    private final Catalog catalog;
    private final String commitUser;
    @Nullable private final OperatorMetricGroup flinkMetricGroup;

    // To make the commit behavior consistent with that of Committer,
    //    StoreMultiCommitter manages multiple committers which are
    //    referenced by table id.
    private final Map<Identifier, StoreCommitter> tableCommitters;

    // Currently, only compact_database job needs to ignore empty commit and set dynamic options
    private final boolean ignoreEmptyCommit;
    private final Map<String, String> dynamicOptions;

    public StoreMultiCommitter(
            Catalog.Loader catalogLoader,
            String commitUser,
            @Nullable OperatorMetricGroup flinkMetricGroup) {
        this(catalogLoader, commitUser, flinkMetricGroup, false, Collections.emptyMap());
    }

    public StoreMultiCommitter(
            Catalog.Loader catalogLoader,
            String commitUser,
            @Nullable OperatorMetricGroup flinkMetricGroup,
            boolean ignoreEmptyCommit,
            Map<String, String> dynamicOptions) {
        this.catalog = catalogLoader.load();
        this.commitUser = commitUser;
        this.flinkMetricGroup = flinkMetricGroup;
        this.ignoreEmptyCommit = ignoreEmptyCommit;
        this.dynamicOptions = dynamicOptions;
        this.tableCommitters = new HashMap<>();
    }

    @Override
    public boolean forceCreatingSnapshot() {
        return true;
    }

    @Override
    public WrappedManifestCommittable combine(
            long checkpointId, long watermark, List<MultiTableCommittable> committables) {
        WrappedManifestCommittable wrappedManifestCommittable =
                new WrappedManifestCommittable(checkpointId, watermark);
        return combine(checkpointId, watermark, wrappedManifestCommittable, committables);
    }

    @Override
    public WrappedManifestCommittable combine(
            long checkpointId,
            long watermark,
            WrappedManifestCommittable wrappedManifestCommittable,
            List<MultiTableCommittable> committables) {
        for (MultiTableCommittable committable : committables) {
            ManifestCommittable manifestCommittable =
                    wrappedManifestCommittable.computeCommittableIfAbsent(
                            Identifier.create(committable.getDatabase(), committable.getTable()),
                            checkpointId,
                            watermark);

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
        }
        return wrappedManifestCommittable;
    }

    @Override
    public void commit(List<WrappedManifestCommittable> committables)
            throws IOException, InterruptedException {
        if (committables.isEmpty()) {
            return;
        }

        // key by table id
        Map<Identifier, List<ManifestCommittable>> committableMap = groupByTable(committables);
        committableMap.keySet().forEach(this::getStoreCommitter);

        long checkpointId = committables.get(0).checkpointId();
        long watermark = committables.get(0).watermark();
        for (Map.Entry<Identifier, StoreCommitter> entry : tableCommitters.entrySet()) {
            List<ManifestCommittable> committableList = committableMap.get(entry.getKey());
            StoreCommitter committer = entry.getValue();
            if (committableList != null) {
                committer.commit(committableList);
            } else {
                // try best to commit empty snapshot, but tableCommitters may not contain all tables
                if (committer.forceCreatingSnapshot()) {
                    ManifestCommittable combine =
                            committer.combine(checkpointId, watermark, Collections.emptyList());
                    committer.commit(Collections.singletonList(combine));
                }
            }
        }
    }

    @Override
    public int filterAndCommit(
            List<WrappedManifestCommittable> globalCommittables, boolean checkAppendFiles)
            throws IOException {
        int result = 0;
        for (Map.Entry<Identifier, List<ManifestCommittable>> entry :
                groupByTable(globalCommittables).entrySet()) {
            result +=
                    getStoreCommitter(entry.getKey())
                            .filterAndCommit(entry.getValue(), checkAppendFiles);
        }
        return result;
    }

    private Map<Identifier, List<ManifestCommittable>> groupByTable(
            List<WrappedManifestCommittable> committables) {
        return committables.stream()
                .flatMap(
                        wrapped -> {
                            Map<Identifier, ManifestCommittable> manifestCommittables =
                                    wrapped.manifestCommittables();
                            return manifestCommittables.entrySet().stream()
                                    .map(entry -> Tuple2.of(entry.getKey(), entry.getValue()));
                        })
                .collect(
                        Collectors.groupingBy(
                                t -> t.f0, Collectors.mapping(t -> t.f1, Collectors.toList())));
    }

    @Override
    public Map<Long, List<MultiTableCommittable>> groupByCheckpoint(
            Collection<MultiTableCommittable> committables) {
        Map<Long, List<MultiTableCommittable>> grouped = new HashMap<>();
        for (MultiTableCommittable c : committables) {
            grouped.computeIfAbsent(c.checkpointId(), k -> new ArrayList<>()).add(c);
        }
        return grouped;
    }

    private StoreCommitter getStoreCommitter(Identifier tableId) {
        StoreCommitter committer = tableCommitters.get(tableId);

        if (committer == null) {
            FileStoreTable table;
            try {
                table = (FileStoreTable) catalog.getTable(tableId).copy(dynamicOptions);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(
                        String.format(
                                "Failed to get committer for table %s", tableId.getFullName()),
                        e);
            }
            committer =
                    new StoreCommitter(
                            table.newCommit(commitUser).ignoreEmptyCommit(ignoreEmptyCommit),
                            flinkMetricGroup);
            tableCommitters.put(tableId, committer);
        }

        return committer;
    }

    @Override
    public void close() throws Exception {
        for (StoreCommitter committer : tableCommitters.values()) {
            committer.close();
        }
    }
}
