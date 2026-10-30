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

package org.apache.paimon.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.globalindex.EsSearchQuery.require;

/** Fixed-snapshot, index-only ES search planning, independent of a particular engine. */
public final class EsIndexSearch {
    private EsIndexSearch() {}

    /** Java SDK entry point for the shared ES search request and index reader. */
    public static EsSearchResult search(Catalog catalog, String envelope) throws Exception {
        Execution execution = prepare(catalog, envelope);
        TopK top = new TopK(execution.query.limit());
        for (Shard shard : execution.plan.shards) {
            top.addAll(shard.search(execution.query));
        }
        return execution.finish(top.hits());
    }

    public static Execution prepare(Catalog catalog, String envelope)
            throws Catalog.TableNotExistException {
        EsSearchRequest request = EsSearchRequest.parse(envelope);
        Table table = catalog.getTable(targetIdentifier(request));
        require(
                table instanceof FileStoreTable,
                "paimon_es_search requires a Paimon file-store table");
        return prepare((FileStoreTable) table, request);
    }

    public static Execution prepare(FileStoreTable original, EsSearchRequest request) {
        targetIdentifier(request);
        FileStoreTable table =
                request.snapshotId == null
                        ? original
                        : original.copy(
                                Collections.singletonMap(
                                        "scan.snapshot-id", request.snapshotId.toString()));
        Plan plan = plan(table, request.indexFields);
        Map<Integer, String> fields = new HashMap<>();
        for (DataField field : plan.table.rowType().getFields()) {
            fields.put(field.id(), field.name());
        }
        EsSearchQuery query =
                request.query.mapFields(alias -> fields.get(request.bindings.get(alias)));
        return new Execution(request, plan, query);
    }

    private static Identifier targetIdentifier(EsSearchRequest request) {
        Identifier identifier = Identifier.create(request.database, request.table);
        require(
                DEFAULT_MAIN_BRANCH.equals(identifier.getBranchNameOrDefault()),
                "paimon_es_search supports the main branch only");
        return identifier;
    }

    /** A prepared query can be scheduled by an engine or executed locally by the Java SDK. */
    public static final class Execution {
        public final EsSearchRequest request;
        public final Plan plan;
        public final EsSearchQuery query;

        private Execution(EsSearchRequest request, Plan plan, EsSearchQuery query) {
            this.request = request;
            this.plan = plan;
            this.query = query;
        }

        /** Finish a globally reduced window, apply pagination once, and fetch the pinned source. */
        public EsSearchResult finish(List<Hit> window) throws Exception {
            require(window.size() <= query.limit(), "ES search result window overflow");
            List<Hit> ordered = new ArrayList<>(window);
            ordered.sort(EsIndexSearch::compareHits);
            Set<Long> ids = new HashSet<>();
            for (Hit hit : ordered) {
                require(
                        hit.rowId >= 0 && Float.isFinite(hit.score) && ids.add(hit.rowId),
                        "Invalid or repeated global result row ID");
            }
            List<Hit> selected =
                    new ArrayList<>(
                            ordered.subList(
                                    Math.min(request.from, ordered.size()),
                                    Math.min(request.from + request.size, ordered.size())));
            return new EsSearchResult(request, plan, selected);
        }
    }

    public static int compareHits(Hit a, Hit b) {
        int score = Float.compare(b.score, a.score);
        return score == 0 ? Long.compare(a.rowId, b.rowId) : score;
    }

    /** Bounded global reduction shared by the Java SDK and Flink workers/coordinator. */
    public static final class TopK {
        private final int limit;
        private final PriorityQueue<Hit> queue;

        public TopK(int limit) {
            require(limit > 0 && limit <= EsSearchQuery.MAX_WINDOW, "Invalid top-K limit");
            this.limit = limit;
            this.queue = new PriorityQueue<>((a, b) -> compareHits(b, a));
        }

        public void addAll(Iterable<Hit> hits) {
            for (Hit hit : hits) {
                if (queue.size() < limit) {
                    queue.add(hit);
                } else if (compareHits(hit, queue.peek()) < 0) {
                    queue.poll();
                    queue.add(hit);
                }
            }
        }

        public List<Hit> hits() {
            List<Hit> result = new ArrayList<>(queue);
            result.sort(EsIndexSearch::compareHits);
            return result;
        }
    }

    /** Planning retains the catalog-supplied FileIO and opens no index readers. */
    public static Plan plan(FileStoreTable original, List<Integer> indexFields) {
        require(
                DEFAULT_MAIN_BRANCH.equals(original.snapshotManager().branch()),
                "paimon_es_search supports the main branch only");
        TableQueryAuthResult.rejectSearchUnderQueryAuth(original);
        require(
                original.coreOptions().dataEvolutionEnabled() && original.primaryKeys().isEmpty(),
                "paimon_es_search requires an append-only data-evolution table");
        require(
                !indexFields.isEmpty() && new HashSet<>(indexFields).size() == indexFields.size(),
                "Select one nonempty index field group without duplicate IDs");
        Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(original);
        require(snapshot != null, "The table has no committed snapshot");
        FileStoreTable table =
                original.copy(
                        Collections.singletonMap("scan.snapshot-id", Long.toString(snapshot.id())));
        List<DataField> fields = new ArrayList<>();
        for (int id : indexFields) {
            DataField field =
                    table.rowType().getFields().stream()
                            .filter(candidate -> candidate.id() == id)
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Unknown index field ID: " + id));
            fields.add(field);
        }
        List<Shard> shards = new ArrayList<>();
        List<DataFileMeta> liveFiles = files(table, snapshot);
        List<IndexManifestEntry> selectedEntries = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (IndexManifestEntry entry :
                table.store()
                        .newIndexFileHandler()
                        .scan(
                                snapshot,
                                value -> "es-index".equals(value.indexFile().indexType()))) {
            IndexFileMeta file = entry.indexFile();
            GlobalIndexMeta global = file.globalIndexMeta();
            if (global == null || !global.getIndexedFieldIds().equals(indexFields)) {
                continue;
            }
            require(
                    shards.size() < 10000 && names.add(file.fileName()),
                    "Too many or duplicate ES index shards");
            require(
                    global.rowRangeStart() >= 0
                            && global.rowRangeEnd() >= global.rowRangeStart()
                            && global.rowRangeEnd() < Long.MAX_VALUE,
                    "Invalid ES index row range");
            Range range = new Range(global.rowRangeStart(), global.rowRangeEnd());
            long sourceId = DataEvolutionIndexSourceMeta.fromIndexFile(file).scanSnapshotId();
            require(sourceId <= snapshot.id(), "Index source snapshot is in the future");
            selectedEntries.add(entry);
            shards.add(
                    new Shard(
                            table,
                            fields,
                            file,
                            table.store().newIndexFileHandler().filePath(entry).toString(),
                            range.from,
                            range.to));
        }
        require(!shards.isEmpty(), "No es-index/ESM1 index matches the selected field group");
        // Reuse Paimon's per-column sequence check. The original construction snapshot may
        // already have expired; checking current manifests does not require retaining it.
        require(
                DataEvolutionGlobalIndexRefreshPlanner.findIndexesToRefresh(
                                table, snapshot, null, selectedEntries, fields)
                        .isEmpty(),
                "ES indexed columns changed since construction; refresh the index");
        shards.sort(Comparator.comparingLong(value -> value.from));
        long previousEnd = -1;
        List<Range> covered = new ArrayList<>();
        for (Shard shard : shards) {
            require(
                    shard.from > previousEnd,
                    "Overlapping ES index generations; rebuild the index");
            previousEnd = shard.to;
            covered.add(new Range(shard.from, shard.to));
        }
        List<Range> live = new ArrayList<>();
        for (DataFileMeta file : liveFiles) {
            require(file.firstRowId() != null, "Data file has no tracked row IDs");
            live.add(file.nonNullRowIdRange());
        }
        List<Range> mergedCovered = Range.sortAndMergeOverlap(covered, true);
        List<Range> mergedLive = Range.sortAndMergeOverlap(live, true);
        require(
                sameRanges(mergedCovered, mergedLive),
                "ES index does not exactly cover the selected snapshot; build missing indexes");
        return new Plan(table, snapshot.id(), shards);
    }

    private static boolean sameRanges(List<Range> a, List<Range> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i).from != b.get(i).from || a.get(i).to != b.get(i).to) {
                return false;
            }
        }
        return true;
    }

    private static List<DataFileMeta> files(FileStoreTable table, Snapshot snapshot) {
        List<DataFileMeta> files = new ArrayList<>();
        for (Split split :
                table.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withMode(ScanMode.ALL)
                        .read()
                        .splits()) {
            require(split instanceof DataSplit, "Unexpected ES search data split");
            require(
                    !split.deletionFiles().orElse(Collections.emptyList()).stream()
                            .anyMatch(Objects::nonNull),
                    "This ES search entry does not support snapshots with deletion vectors");
            files.addAll(((DataSplit) split).dataFiles());
            require(files.size() <= 100000, "ES search metadata exceeds its file limit");
        }
        return files;
    }

    /** Serializable values only: index readers are opened and closed by each task. */
    public static final class Shard implements Serializable {
        private static final long serialVersionUID = 1L;
        private final FileStoreTable table;
        private final List<DataField> fields;
        private final long fileSize;
        private final byte[] indexMeta;
        private final String path;
        private final long from;
        private final long to;

        private Shard(
                FileStoreTable table,
                List<DataField> fields,
                IndexFileMeta file,
                String path,
                long from,
                long to) {
            this.table = table;
            this.fields = new ArrayList<>(fields);
            this.fileSize = file.fileSize();
            this.indexMeta = file.globalIndexMeta().indexMeta();
            this.path = path;
            this.from = from;
            this.to = to;
        }

        public List<Hit> search(EsSearchQuery query) throws IOException {
            GlobalIndexer indexer =
                    GlobalIndexer.create("es-index", fields, Options.fromMap(table.options()));
            GlobalIndexIOMeta meta = new GlobalIndexIOMeta(new Path(path), fileSize, indexMeta);
            try (GlobalIndexReader reader =
                    indexer.createReader(
                            value -> table.fileIO().newInputStream(value.filePath()),
                            Collections.singletonList(meta),
                            to - from + 1,
                            null,
                            null)) {
                ScoredGlobalIndexResult result = reader.visitEsSearch(query).join();
                List<Hit> hits = new ArrayList<>();
                for (long relative : result.results()) {
                    require(
                            relative >= 0 && relative <= to - from,
                            "ES search returned an invalid relative row ID");
                    float score = result.scoreGetter().score(relative);
                    require(Float.isFinite(score), "ES search returned a nonfinite score");
                    hits.add(new Hit(Math.addExact(from, relative), score));
                }
                require(hits.size() <= query.limit(), "ES result window overflow");
                return hits;
            }
        }
    }

    /** Index score and absolute Paimon row identity returned by an executor. */
    public static final class Hit implements Serializable {
        private static final long serialVersionUID = 1L;
        public final long rowId;
        public final float score;

        public Hit(long rowId, float score) {
            this.rowId = rowId;
            this.score = score;
        }
    }

    /** Immutable snapshot selection with serializable shard work and optional source fetching. */
    public static final class Plan implements Serializable {
        private static final long serialVersionUID = 1L;
        public final FileStoreTable table;
        public final long snapshotId;
        public final List<Shard> shards;

        private Plan(FileStoreTable table, long snapshotId, List<Shard> shards) {
            this.table = table;
            this.snapshotId = snapshotId;
            this.shards = Collections.unmodifiableList(shards);
        }

        /** Source schema includes the tracked _ROW_ID field used to validate every fetched row. */
        public RowType sourceType() {
            return SpecialFields.rowTypeWithRowId(table.rowType());
        }

        /** Fetch only the globally selected hits from the exact planned snapshot. */
        public Map<Long, byte[]> fetch(List<Hit> hits) throws Exception {
            Map<Long, byte[]> source = new HashMap<>();
            if (hits.isEmpty()) {
                return source;
            }
            require(hits.size() <= EsSearchQuery.MAX_WINDOW, "Source window exceeds its limit");
            Set<Long> expected = new HashSet<>();
            List<Range> ranges = new ArrayList<>();
            for (Hit hit : hits) {
                require(expected.add(hit.rowId), "Duplicate source row ID");
                ranges.add(new Range(hit.rowId, hit.rowId));
            }
            RowType type = sourceType();
            int idPosition = type.getFieldIndex(SpecialFields.ROW_ID.name());
            ReadBuilder builder =
                    table.newReadBuilder()
                            .withReadType(type)
                            .withRowRanges(Range.sortAndMergeOverlap(ranges, true));
            InternalRowSerializer serializer = new InternalRowSerializer(type);
            long bytes = 0;
            try (CloseableIterator<InternalRow> rows =
                    builder.newRead()
                            .createReader(builder.newScan().plan())
                            .toCloseableIterator()) {
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    long id = row.getLong(idPosition);
                    if (!expected.contains(id)) {
                        continue;
                    }
                    require(!source.containsKey(id), "Source read repeated a row ID");
                    DataOutputSerializer output = new DataOutputSerializer(1024);
                    serializer.serialize(row, output);
                    bytes = Math.addExact(bytes, output.length());
                    require(bytes <= 32L * 1024 * 1024, "ES search source exceeds 32 MiB");
                    source.put(id, output.getCopyOfBuffer());
                }
            }
            require(
                    source.keySet().equals(expected),
                    "ES search hits are missing from the planned snapshot");
            return source;
        }
    }
}
