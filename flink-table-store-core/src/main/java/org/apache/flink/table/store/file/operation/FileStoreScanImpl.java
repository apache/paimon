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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.predicate.And;
import org.apache.flink.table.store.file.predicate.Equal;
import org.apache.flink.table.store.file.predicate.Literal;
import org.apache.flink.table.store.file.predicate.Or;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.RowDataToObjectArrayConverter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Default implementation of {@link FileStoreScan}. */
public class FileStoreScanImpl implements FileStoreScan {

    private final RowDataToObjectArrayConverter partitionConverter;
    private final FileStorePathFactory pathFactory;
    private final ManifestFile.Factory manifestFileFactory;
    private final ManifestList manifestList;

    private Predicate partitionFilter;
    private Predicate keyFilter;
    private Predicate valueFilter;

    private Long specifiedSnapshotId = null;
    private Integer specifiedBucket = null;
    private List<ManifestFileMeta> specifiedManifests = null;
    private boolean isIncremental = false;

    public FileStoreScanImpl(
            RowType partitionType,
            FileStorePathFactory pathFactory,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory) {
        this.partitionConverter = new RowDataToObjectArrayConverter(partitionType);
        this.pathFactory = pathFactory;
        this.manifestFileFactory = manifestFileFactory;
        this.manifestList = manifestListFactory.create();
    }

    @Override
    public Long latestSnapshot() {
        return pathFactory.latestSnapshotId();
    }

    @Override
    public boolean snapshotExists(long snapshotId) {
        Path path = pathFactory.toSnapshotPath(snapshotId);
        try {
            return path.getFileSystem().exists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return Snapshot.fromPath(pathFactory.toSnapshotPath(snapshotId));
    }

    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(List<BinaryRowData> partitions) {
        Function<BinaryRowData, Predicate> partitionToPredicate =
                p -> {
                    List<Predicate> fieldPredicates = new ArrayList<>();
                    Object[] partitionObjects = partitionConverter.convert(p);
                    for (int i = 0; i < partitionConverter.getArity(); i++) {
                        Literal l =
                                new Literal(
                                        partitionConverter.rowType().getTypeAt(i),
                                        partitionObjects[i]);
                        fieldPredicates.add(new Equal(i, l));
                    }
                    return fieldPredicates.stream().reduce(And::new).get();
                };
        Optional<Predicate> predicate =
                partitions.stream()
                        .filter(p -> p.getArity() > 0)
                        .map(partitionToPredicate)
                        .reduce(Or::new);
        if (predicate.isPresent()) {
            return withPartitionFilter(predicate.get());
        } else {
            return this;
        }
    }

    @Override
    public FileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan withValueFilter(Predicate predicate) {
        this.valueFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan withBucket(int bucket) {
        this.specifiedBucket = bucket;
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(long snapshotId) {
        this.specifiedSnapshotId = snapshotId;
        if (specifiedManifests != null) {
            throw new IllegalStateException("Cannot set both snapshot id and manifests.");
        }
        return this;
    }

    @Override
    public FileStoreScan withManifestList(List<ManifestFileMeta> manifests) {
        this.specifiedManifests = manifests;
        if (specifiedSnapshotId != null) {
            throw new IllegalStateException("Cannot set both snapshot id and manifests.");
        }
        return this;
    }

    @Override
    public FileStoreScan withIncremental(boolean isIncremental) {
        this.isIncremental = isIncremental;
        return this;
    }

    @Override
    public Plan plan() {
        List<ManifestFileMeta> manifests = specifiedManifests;
        Long snapshotId = specifiedSnapshotId;
        if (manifests == null) {
            if (snapshotId == null) {
                snapshotId = pathFactory.latestSnapshotId();
            }
            if (snapshotId == null) {
                manifests = Collections.emptyList();
            } else {
                Snapshot snapshot = snapshot(snapshotId);
                manifests =
                        isIncremental
                                ? manifestList.read(snapshot.deltaManifestList())
                                : snapshot.readAllManifests(manifestList);
            }
        }

        final Long readSnapshot = snapshotId;
        final List<ManifestFileMeta> readManifests = manifests;

        List<ManifestEntry> entries;
        try {
            entries =
                    FileUtils.COMMON_IO_FORK_JOIN_POOL
                            .submit(
                                    () ->
                                            readManifests
                                                    .parallelStream()
                                                    .filter(this::filterManifestFileMeta)
                                                    .flatMap(m -> readManifestFileMeta(m).stream())
                                                    .filter(this::filterManifestEntry)
                                                    .collect(Collectors.toList()))
                            .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to read ManifestEntry list concurrently", e);
        }

        Map<ManifestEntry.Identifier, ManifestEntry> map = new HashMap<>();
        for (ManifestEntry entry : entries) {
            ManifestEntry.Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    Preconditions.checkState(
                            !map.containsKey(identifier),
                            "Trying to add file %s which is already added. "
                                    + "Manifest might be corrupted.",
                            identifier);
                    map.put(identifier, entry);
                    break;
                case DELETE:
                    Preconditions.checkState(
                            map.containsKey(identifier),
                            "Trying to delete file %s which is not previously added. "
                                    + "Manifest might be corrupted.",
                            identifier);
                    map.remove(identifier);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
        List<ManifestEntry> files = new ArrayList<>(map.values());

        return new Plan() {
            @Nullable
            @Override
            public Long snapshotId() {
                return readSnapshot;
            }

            @Override
            public List<ManifestEntry> files() {
                return files;
            }
        };
    }

    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        return partitionFilter == null
                || partitionFilter.test(
                        manifest.numAddedFiles() + manifest.numDeletedFiles(),
                        manifest.partitionStats());
    }

    private boolean filterManifestEntry(ManifestEntry entry) {
        return (partitionFilter == null
                        || partitionFilter.test(partitionConverter.convert(entry.partition())))
                && (keyFilter == null
                        || keyFilter.test(entry.file().rowCount(), entry.file().keyStats()))
                && (valueFilter == null
                        || valueFilter.test(entry.file().rowCount(), entry.file().valueStats()))
                && (specifiedBucket == null || entry.bucket() == specifiedBucket);
    }

    private List<ManifestEntry> readManifestFileMeta(ManifestFileMeta manifest) {
        return manifestFileFactory.create().read(manifest.fileName());
    }
}
