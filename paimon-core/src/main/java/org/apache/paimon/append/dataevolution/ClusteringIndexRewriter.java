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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexRefreshPlanner;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.DataEvolutionUtils.materializedRowIdRanges;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Rewrites scalar index postings after clustering output files receive their final row IDs. */
public final class ClusteringIndexRewriter {

    // The current metadata protocol represents coverage with contiguous ranges. Bound both
    // tiny-file fan-out and repeated source scans until sparse coverage has its own protocol.
    private static final int MAX_COVERAGE_FRAGMENTS = 64;

    private ClusteringIndexRewriter() {}

    public static List<IndexManifestEntry> rewrite(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            SchemaManager schemaManager,
            CoreOptions options,
            @Nullable IOManager ioManager,
            List<ManifestEntry> dataFiles,
            List<IndexManifestEntry> indexFiles,
            Set<String> mappedClusteringOutputs,
            Set<String> rowIdPreservingOutputs,
            long newSnapshotId)
            throws IOException {
        if (indexFiles.stream()
                .noneMatch(
                        entry ->
                                entry.kind() == FileKind.DELETE
                                        && entry.indexFile().globalIndexMeta() != null)) {
            return indexFiles;
        }
        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> groups =
                dataFiles.stream()
                        .collect(Collectors.groupingBy(e -> Pair.of(e.partition(), e.bucket())));
        Map<Pair<BinaryRow, Integer>, List<IndexManifestEntry>> deletes = new HashMap<>();
        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry entry : indexFiles) {
            if (entry.kind() == FileKind.DELETE && entry.indexFile().globalIndexMeta() != null) {
                deletes.computeIfAbsent(
                                Pair.of(entry.partition(), entry.bucket()), k -> new ArrayList<>())
                        .add(entry);
            } else {
                result.add(entry);
            }
        }
        for (Map.Entry<Pair<BinaryRow, Integer>, List<IndexManifestEntry>> group :
                deletes.entrySet()) {
            List<ManifestEntry> files =
                    groups.getOrDefault(group.getKey(), Collections.emptyList());
            List<DataFileMeta> after =
                    files.stream()
                            .filter(e -> e.kind() == FileKind.ADD)
                            .map(ManifestEntry::file)
                            .collect(Collectors.toList());
            List<ManifestEntry> before =
                    files.stream()
                            .filter(e -> e.kind() == FileKind.DELETE)
                            .map(
                                    e ->
                                            ManifestEntry.create(
                                                    FileKind.ADD,
                                                    e.partition(),
                                                    e.bucket(),
                                                    e.totalBuckets(),
                                                    e.file()))
                            .collect(Collectors.toList());
            if (before.isEmpty()) {
                result.addAll(group.getValue());
                continue;
            }
            List<Range> removed =
                    materializedRowIdRanges(
                            before.stream().map(ManifestEntry::file).collect(Collectors.toList()),
                            after.stream()
                                    .filter(
                                            file ->
                                                    rowIdPreservingOutputs.contains(
                                                            file.fileName()))
                                    .collect(Collectors.toList()));
            if (removed.isEmpty()) {
                // Preserve explicitly requested index deletions for ordinary compaction callers.
                result.addAll(group.getValue());
                continue;
            }
            after.removeIf(file -> rowIdPreservingOutputs.contains(file.fileName()));
            // Engines without a permutation still invalidate affected indexes. Independently
            // compacted ranges in the same partition retain their original index files.
            if (after.stream()
                    .anyMatch(file -> !mappedClusteringOutputs.contains(file.fileName()))) {
                group.getValue().stream()
                        .filter(
                                entry ->
                                        !Range.and(
                                                        Collections.singletonList(
                                                                entry.indexFile()
                                                                        .globalIndexMeta()
                                                                        .rowRange()),
                                                        removed)
                                                .isEmpty())
                        .forEach(result::add);
                continue;
            }
            after.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));
            long firstAssignedRowId =
                    after.isEmpty() ? Long.MAX_VALUE : after.get(0).nonNullFirstRowId();
            DataFilePathFactory dataPath =
                    pathFactory.createDataFilePathFactory(
                            group.getKey().getLeft(), group.getKey().getRight());
            try (DiskMapping mapping = new DiskMapping(removed, ioManager)) {
                for (DataFileMeta file : after) {
                    readMapping(
                            fileIO,
                            dataPath,
                            file,
                            (ordinal, oldId) ->
                                    mapping.put(oldId, file.nonNullFirstRowId() + ordinal));
                }
                RowType rowType =
                        schemaManager
                                .latestOrThrow("Missing clustering table schema")
                                .logicalRowType();
                Map<Pair<String, List<Integer>>, List<IndexManifestEntry>> validSources =
                        new LinkedHashMap<>();
                for (IndexManifestEntry entry : group.getValue()) {
                    GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
                    if (Range.and(Collections.singletonList(meta.rowRange()), removed).isEmpty()) {
                        continue;
                    }
                    result.add(entry);
                    String type = entry.indexFile().indexType();
                    if (!type.equals("btree") && !type.equals("bitmap")) {
                        continue;
                    }
                    // Dropped indexed columns can leave old index files in the manifest.
                    if (!meta.getIndexedFieldIds().stream().allMatch(rowType::containsField)) {
                        continue;
                    }
                    List<DataField> fields = meta.getIndexedFields(rowType);
                    IndexManifestEntry source =
                            new IndexManifestEntry(
                                    FileKind.ADD,
                                    entry.partition(),
                                    entry.bucket(),
                                    entry.indexFile());
                    // Updating row IDs cannot repair keys from an older indexed-column value.
                    if (!DataEvolutionIndexSourceMeta.isDataEvolutionMeta(meta.sourceMeta())
                            || !DataEvolutionGlobalIndexRefreshPlanner.findIndexesToRefresh(
                                            schemaManager,
                                            before,
                                            Collections.singletonList(source),
                                            fields)
                                    .isEmpty()) {
                        continue;
                    }
                    validSources
                            .computeIfAbsent(
                                    Pair.of(type, meta.getIndexedFieldIds()),
                                    ignored -> new ArrayList<>())
                            .add(entry);
                }
                for (Map.Entry<Pair<String, List<Integer>>, List<IndexManifestEntry>> sourceGroup :
                        validSources.entrySet()) {
                    List<IndexManifestEntry> sources = sourceGroup.getValue();
                    String type = sourceGroup.getKey().getLeft();
                    GlobalIndexMeta firstMeta = sources.get(0).indexFile().globalIndexMeta();
                    List<DataField> fields = firstMeta.getIndexedFields(rowType);
                    RowRangeIndex coverage =
                            RowRangeIndex.create(
                                    sources.stream()
                                            .map(
                                                    source ->
                                                            source.indexFile()
                                                                    .globalIndexMeta()
                                                                    .rowRange())
                                            .collect(Collectors.toList()),
                                    false);
                    String sourceName = sources.get(0).indexFile().fileName();
                    List<Range> freshFragments = new ArrayList<>();
                    // Source shards can interleave after sorting. Their union, rather than each
                    // original shard, defines the coverage shared by the rewritten payload files.
                    for (DataFileMeta file : after) {
                        final long[] start = {-1};
                        final long[] end = {-1};
                        readMapping(
                                fileIO,
                                dataPath,
                                file,
                                (ordinal, oldId) -> {
                                    long newId = file.nonNullFirstRowId() + ordinal;
                                    if (coverage.intersects(oldId, oldId)) {
                                        if (start[0] < 0) {
                                            start[0] = newId;
                                        }
                                        end[0] = newId;
                                    } else if (start[0] >= 0) {
                                        addFragment(
                                                freshFragments,
                                                new Range(start[0], end[0]),
                                                sourceName,
                                                firstAssignedRowId);
                                        start[0] = -1;
                                    }
                                });
                        if (start[0] >= 0) {
                            addFragment(
                                    freshFragments,
                                    new Range(start[0], end[0]),
                                    sourceName,
                                    firstAssignedRowId);
                        }
                    }
                    GlobalIndexer indexer =
                            GlobalIndexer.create(type, fields, Options.fromMap(options.toMap()));
                    IndexPathFactory indexPath = pathFactory.globalIndexFileFactory();
                    GlobalIndexFileReadWrite indexIO =
                            new GlobalIndexFileReadWrite(fileIO, indexPath);
                    for (IndexManifestEntry source : sources) {
                        GlobalIndexMeta meta = source.indexFile().globalIndexMeta();
                        List<Range> fragments = new ArrayList<>();
                        for (Range untouched : meta.rowRange().exclude(removed)) {
                            addFragment(
                                    fragments,
                                    untouched,
                                    source.indexFile().fileName(),
                                    firstAssignedRowId);
                        }
                        for (Range fresh : freshFragments) {
                            addFragment(
                                    fragments,
                                    fresh,
                                    source.indexFile().fileName(),
                                    firstAssignedRowId);
                        }
                        GlobalIndexIOMeta input =
                                new GlobalIndexIOMeta(
                                        indexPath.toPath(source.indexFile()),
                                        source.indexFile().fileSize(),
                                        source.indexFile().rowCount(),
                                        meta.indexMeta());
                        for (Range fragment : fragments) {
                            List<ResultEntry> rewritten =
                                    indexer.rewriteRowIds(
                                            indexIO,
                                            Collections.singletonList(input),
                                            meta.rowRange().count(),
                                            fragment.count(),
                                            relative -> {
                                                try {
                                                    long target =
                                                            mapping.get(
                                                                    relative
                                                                            + meta.rowRangeStart());
                                                    return target >= fragment.from
                                                                    && target <= fragment.to
                                                            ? target - fragment.from
                                                            : -1;
                                                } catch (IOException e) {
                                                    throw new UncheckedIOException(e);
                                                }
                                            },
                                            indexIO);
                            // Only newly assigned coverage shares a fresh baseline. Old fragments
                            // keep their source's baseline, even when another shard is older.
                            byte[] sourceMeta =
                                    fragment.from >= firstAssignedRowId
                                            ? new DataEvolutionIndexSourceMeta(newSnapshotId)
                                                    .serialize()
                                            : meta.sourceMeta();
                            for (IndexFileMeta rewrittenFile :
                                    GlobalIndexBuilderUtils.toIndexFileMetas(
                                            fileIO,
                                            indexPath,
                                            options,
                                            fragment,
                                            fields,
                                            type,
                                            rewritten,
                                            sourceMeta)) {
                                result.add(
                                        new IndexManifestEntry(
                                                FileKind.ADD,
                                                source.partition(),
                                                source.bucket(),
                                                rewrittenFile));
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    private static void addFragment(
            List<Range> fragments, Range fragment, String fileName, long firstAssignedRowId) {
        if (!fragments.isEmpty()) {
            Range previous = fragments.get(fragments.size() - 1);
            if (previous.to == fragment.from - 1
                    && (previous.from >= firstAssignedRowId)
                            == (fragment.from >= firstAssignedRowId)) {
                fragments.set(fragments.size() - 1, new Range(previous.from, fragment.to));
                return;
            }
        }
        checkArgument(
                fragments.size() < MAX_COVERAGE_FRAGMENTS,
                "Clustering exceeds %s index coverage fragments for %s. "
                        + "Build smaller, fully covered index shards before clustering this range.",
                MAX_COVERAGE_FRAGMENTS,
                fileName);
        fragments.add(fragment);
    }

    private static void readMapping(
            FileIO fileIO,
            DataFilePathFactory pathFactory,
            DataFileMeta file,
            ClusteringRowIdMapping.RowIdConsumer consumer)
            throws IOException {
        ClusteringRowIdMapping.read(
                fileIO,
                pathFactory.toAlignedPath(ClusteringRowIdMapping.fileName(file.fileName()), file),
                file.rowCount(),
                consumer);
    }

    /**
     * Disk-backed inverse permutation; memory is proportional to selected ranges, not row count.
     */
    private static final class DiskMapping implements Closeable {
        private final NavigableMap<Long, Pair<Range, Long>> offsets = new TreeMap<>();
        private final File path;
        private final RandomAccessFile file;

        private DiskMapping(List<Range> ranges, @Nullable IOManager ioManager) throws IOException {
            long rows = 0;
            for (Range range : ranges) {
                offsets.put(range.from, Pair.of(range, rows));
                rows = Math.addExact(rows, range.count());
            }
            long length = Math.multiplyExact(rows, Long.BYTES);
            path =
                    ioManager == null
                            ? File.createTempFile("paimon-cluster-row-id-", ".mapping")
                            : ioManager.createChannel().getPathFile();
            RandomAccessFile opened = null;
            try {
                opened = new RandomAccessFile(path, "rw");
                opened.setLength(length);
                file = opened;
            } catch (IOException | RuntimeException | Error failure) {
                if (opened != null) {
                    try {
                        opened.close();
                    } catch (IOException closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                try {
                    Files.deleteIfExists(path.toPath());
                } catch (IOException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }

        @Nullable
        private Long offset(long oldId) {
            Map.Entry<Long, Pair<Range, Long>> entry = offsets.floorEntry(oldId);
            if (entry == null || oldId > entry.getValue().getLeft().to) {
                return null;
            }
            return Math.multiplyExact(
                    entry.getValue().getRight() + oldId - entry.getKey(), Long.BYTES);
        }

        private void put(long oldId, long newId) throws IOException {
            Long offset = offset(oldId);
            checkArgument(
                    offset != null, "Clustering output refers to unselected row ID %s", oldId);
            file.seek(offset);
            checkArgument(file.readLong() == 0, "Duplicate clustering row ID %s", oldId);
            file.seek(offset);
            file.writeLong(Math.addExact(newId, 1));
        }

        private long get(long oldId) throws IOException {
            Long offset = offset(oldId);
            if (offset == null) {
                return oldId;
            }
            file.seek(offset);
            return file.readLong() - 1;
        }

        @Override
        public void close() throws IOException {
            try {
                file.close();
            } finally {
                Files.deleteIfExists(path.toPath());
            }
        }
    }
}
