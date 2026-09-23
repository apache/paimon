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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner.IndexMetaFileGroup;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Between;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Contains;
import org.apache.paimon.predicate.EndsWith;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Like;
import org.apache.paimon.predicate.NotBetween;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.NotIn;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.StartsWith;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexOptions.BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE;
import static org.apache.paimon.globalindex.btree.BTreeIndexOptions.BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE;
import static org.apache.paimon.utils.SerializationUtils.deserializedBytes;
import static org.apache.paimon.utils.SerializationUtils.serializeBytes;

/**
 * A deferred scalar index query, represented as leaf predicates combined with AND/OR.
 *
 * <p>Planning fixes predicate support and scan budgets from metadata without opening index files.
 * Each data split receives the overlapping groups; its reader executes the plan to obtain row IDs.
 */
class GlobalIndexScanPlan {

    /** The query for a leaf node; null for an AND/OR node. */
    @Nullable private final LeafPredicate predicate;

    /** For compound nodes, true means OR and false means AND; ignored for leaves. */
    private final boolean union;

    /** Supported child plans of a compound node; empty for leaves. */
    private final List<GlobalIndexScanPlan> children;

    /** A leaf's selected index groups. An empty list means no matches, not unsupported. */
    private final List<IndexGroup> groups;

    private GlobalIndexScanPlan(
            @Nullable LeafPredicate predicate,
            boolean union,
            List<GlobalIndexScanPlan> children,
            List<IndexGroup> groups) {
        this.predicate = predicate;
        this.union = union;
        this.children = children;
        this.groups = groups;
    }

    static boolean supports(List<IndexFileMeta> files) {
        return files.stream()
                .allMatch(
                        file ->
                                "es-index".equals(file.indexType())
                                        || ("btree".equals(file.indexType())
                                                        || "bitmap".equals(file.indexType()))
                                                && file.globalIndexMeta()
                                                                .getIndexedFieldIds()
                                                                .size()
                                                        == 1);
    }

    @Nullable
    static GlobalIndexScanPlan create(
            RowType rowType,
            Predicate predicate,
            List<IndexFileMeta> files,
            IndexPathFactory pathFactory,
            Options options) {
        Map<Integer, List<IndexMetaFileGroup>> groupsByField =
                DataEvolutionGlobalIndexScanner.groupIndexFiles(files);
        Map<IndexMetaFileGroup, List<IndexGroup>> converted = new LinkedHashMap<>();
        Map<Integer, List<IndexGroup>> groups = new LinkedHashMap<>();
        groupsByField.forEach(
                (fieldId, fieldGroups) -> {
                    List<IndexGroup> indexGroups = new ArrayList<>();
                    for (IndexMetaFileGroup group : fieldGroups) {
                        indexGroups.addAll(
                                converted.computeIfAbsent(
                                        group,
                                        key -> IndexGroup.fromMetadata(key, rowType, pathFactory)));
                    }
                    groups.put(fieldId, indexGroups);
                });
        return create(predicate, rowType, groups, options);
    }

    @Nullable
    private static GlobalIndexScanPlan create(
            Predicate predicate,
            RowType rowType,
            Map<Integer, List<IndexGroup>> groups,
            Options options) {
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) predicate;
            Optional<FieldRef> field = leaf.fieldRefOptional();
            if (!field.isPresent()) {
                return null;
            }
            List<IndexGroup> fieldGroups = groups.get(rowType.getField(field.get().name()).id());
            if (fieldGroups == null) {
                return null;
            }
            List<IndexGroup> selectedGroups = new ArrayList<>();
            for (IndexGroup group : fieldGroups) {
                if ("es-index".equals(group.type)) {
                    if (!supportsESPredicate(leaf.function())) {
                        return null;
                    }
                    selectedGroups.add(group);
                    continue;
                }
                long budget =
                        options.get(
                                        "btree".equals(group.type)
                                                ? BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE
                                                : BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE)
                                .getBytes();
                SortedFileIndexPlanner planner =
                        new SortedFileIndexPlanner(
                                group.files, KeySerializer.create(group.field.type()), budget);
                Optional<List<GlobalIndexIOMeta>> selected =
                        leaf.function().visit(planner, field.get(), leaf.literals());
                // UnionGlobalIndexReader requires every original group to support the leaf.
                if (!selected.isPresent()) {
                    return null;
                }
                if (!selected.get().isEmpty()) {
                    selectedGroups.add(
                            new IndexGroup(
                                    group.type,
                                    group.field,
                                    group.extraFields,
                                    group.range,
                                    selected.get()));
                }
            }
            return new GlobalIndexScanPlan(leaf, false, Collections.emptyList(), selectedGroups);
        }
        CompoundPredicate compound = (CompoundPredicate) predicate;
        boolean union = compound.function() instanceof Or;
        List<GlobalIndexScanPlan> children = new ArrayList<>();
        for (Predicate child : GlobalIndexEvaluator.normalizedChildren(compound)) {
            GlobalIndexScanPlan plan = create(child, rowType, groups, options);
            if (plan == null) {
                if (union) {
                    return null;
                }
            } else {
                children.add(plan);
            }
        }
        return children.isEmpty()
                ? null
                : new GlobalIndexScanPlan(null, union, children, Collections.emptyList());
    }

    private static boolean supportsESPredicate(LeafFunction function) {
        // ES scalar visitors return matches or conservative candidates for every indexed field.
        return function instanceof Equal
                || function instanceof NotEqual
                || function instanceof LessThan
                || function instanceof LessOrEqual
                || function instanceof GreaterThan
                || function instanceof GreaterOrEqual
                || function instanceof Between
                || function instanceof NotBetween
                || function instanceof In
                || function instanceof NotIn
                || function instanceof StartsWith
                || function instanceof EndsWith
                || function instanceof Contains
                || function instanceof Like
                || function instanceof IsNull
                || function instanceof IsNotNull;
    }

    boolean isEmpty() {
        if (predicate != null) {
            return groups.isEmpty();
        }
        return union
                ? children.stream().allMatch(GlobalIndexScanPlan::isEmpty)
                : children.stream().anyMatch(GlobalIndexScanPlan::isEmpty);
    }

    /** Residual predicates discarded during planning must not expand unindexed coverage. */
    Set<Integer> contributingFieldIds(RowType rowType) {
        Set<Integer> fields = new HashSet<>();
        if (predicate != null) {
            fields.add(rowType.getField(predicate.fieldRefOptional().get().name()).id());
        } else {
            for (GlobalIndexScanPlan child : children) {
                fields.addAll(child.contributingFieldIds(rowType));
            }
        }
        return fields;
    }

    /** Keep the original offsets and budget decisions while removing unrelated groups. */
    GlobalIndexScanPlan forRanges(List<Range> ranges) {
        List<IndexGroup> selected = new ArrayList<>();
        for (IndexGroup group : groups) {
            if (ranges.stream()
                    .anyMatch(
                            range ->
                                    Range.intersect(
                                            group.range.from,
                                            group.range.to,
                                            range.from,
                                            range.to))) {
                selected.add(group);
            }
        }
        List<GlobalIndexScanPlan> selectedChildren = new ArrayList<>();
        for (GlobalIndexScanPlan child : children) {
            selectedChildren.add(child.forRanges(ranges));
        }
        // A supported leaf without a local group remains an empty result, not unsupported.
        return new GlobalIndexScanPlan(predicate, union, selectedChildren, selected);
    }

    GlobalIndexResult evaluate(FileIO fileIO, Options options, List<Range> ranges)
            throws IOException {
        ExecutorService executor =
                GlobalIndexReadThreadPool.getExecutorService(options.get(GLOBAL_INDEX_THREAD_NUM));
        return evaluate(fileIO, options, ranges, executor);
    }

    private GlobalIndexResult evaluate(
            FileIO fileIO, Options options, List<Range> ranges, ExecutorService executor)
            throws IOException {
        GlobalIndexResult result = null;
        if (predicate != null) {
            result = GlobalIndexResult.createEmpty();
            for (IndexGroup group : groups) {
                GlobalIndexResult splitRows = localSplitRows(ranges, group.range);
                if (splitRows.results().isEmpty()) {
                    continue;
                }
                GlobalIndexer indexer =
                        GlobalIndexerFactoryUtils.load(group.type)
                                .create(group.field, group.extraFields, options);
                try (GlobalIndexReader reader =
                        "btree".equals(group.type)
                                ? indexer.createReader(
                                        meta -> fileIO.newInputStream(meta.filePath()),
                                        group.files,
                                        group.range.count(),
                                        splitRows.results().toRangeList(),
                                        executor)
                                : indexer.createReader(
                                        meta -> fileIO.newInputStream(meta.filePath()),
                                        group.files,
                                        group.range.count(),
                                        executor)) {
                    Optional<GlobalIndexResult> matches =
                            predicate
                                    .function()
                                    .visit(
                                            reader,
                                            predicate.fieldRefOptional().get(),
                                            predicate.literals())
                                    .get();
                    if (!matches.isPresent()) {
                        throw new IOException(
                                "Index predicate became unsupported after planning: " + predicate);
                    }
                    // Clip in index-local coordinates before offset() iterates the retained rows.
                    result = result.or(splitRows.and(matches.get()).offset(group.range.from));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while evaluating lazy index split", e);
                } catch (ExecutionException e) {
                    throw new IOException("Failed to evaluate lazy index split", e.getCause());
                }
            }
        } else {
            for (GlobalIndexScanPlan child : children) {
                GlobalIndexResult matches = child.evaluate(fileIO, options, ranges, executor);
                result =
                        result == null ? matches : union ? result.or(matches) : result.and(matches);
            }
        }
        return result == null ? GlobalIndexResult.createEmpty() : result;
    }

    private static GlobalIndexResult localSplitRows(List<Range> ranges, Range groupRange) {
        List<Range> localRanges = new ArrayList<>();
        for (Range range : ranges) {
            long from = Math.max(range.from, groupRange.from);
            long to = Math.min(range.to, groupRange.to);
            if (from <= to) {
                localRanges.add(new Range(from - groupRange.from, to - groupRange.from));
            }
        }
        return GlobalIndexResult.fromRanges(localRanges);
    }

    void serialize(DataOutputView out) throws IOException {
        out.writeByte(predicate != null ? 0 : union ? 2 : 1);
        if (predicate != null) {
            serializeBytes(out, InstantiationUtil.serializeObject(predicate));
            out.writeInt(groups.size());
            for (IndexGroup group : groups) {
                writeString(out, group.type);
                writeString(out, JsonSerdeUtil.toJson(group.field));
                out.writeInt(group.extraFields.size());
                for (DataField field : group.extraFields) {
                    writeString(out, JsonSerdeUtil.toJson(field));
                }
                out.writeLong(group.range.from);
                out.writeLong(group.range.to);
                out.writeInt(group.files.size());
                for (GlobalIndexIOMeta file : group.files) {
                    writeString(out, file.filePath().toString());
                    out.writeLong(file.fileSize());
                    out.writeLong(file.rowCount());
                    serializeBytes(out, file.metadata());
                }
            }
        } else {
            out.writeInt(children.size());
            for (GlobalIndexScanPlan child : children) {
                child.serialize(out);
            }
        }
    }

    static GlobalIndexScanPlan deserialize(DataInputView in) throws IOException {
        int type = in.readByte();
        if (type == 0) {
            Predicate predicate;
            try {
                predicate =
                        InstantiationUtil.deserializeObject(
                                deserializedBytes(in), GlobalIndexScanPlan.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize lazy index predicate", e);
            }
            if (!(predicate instanceof LeafPredicate)) {
                throw new IOException("Expected a leaf index predicate");
            }
            List<IndexGroup> groups = new ArrayList<>();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String indexType = readString(in);
                DataField field = JsonSerdeUtil.fromJson(readString(in), DataField.class);
                List<DataField> extraFields = new ArrayList<>();
                int extraFieldCount = in.readInt();
                for (int j = 0; j < extraFieldCount; j++) {
                    extraFields.add(JsonSerdeUtil.fromJson(readString(in), DataField.class));
                }
                Range range = new Range(in.readLong(), in.readLong());
                List<GlobalIndexIOMeta> files = new ArrayList<>();
                int fileCount = in.readInt();
                for (int j = 0; j < fileCount; j++) {
                    Path path = new Path(readString(in));
                    long fileSize = in.readLong();
                    long rowCount = in.readLong();
                    files.add(
                            new GlobalIndexIOMeta(path, fileSize, rowCount, deserializedBytes(in)));
                }
                groups.add(new IndexGroup(indexType, field, extraFields, range, files));
            }
            return new GlobalIndexScanPlan(
                    (LeafPredicate) predicate, false, Collections.emptyList(), groups);
        }
        if (type != 1 && type != 2) {
            throw new IOException("Unknown index plan node: " + type);
        }
        List<GlobalIndexScanPlan> children = new ArrayList<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            children.add(deserialize(in));
        }
        return new GlobalIndexScanPlan(null, type == 2, children, Collections.emptyList());
    }

    static void writeString(DataOutputView out, String value) throws IOException {
        serializeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    static String readString(DataInputView in) throws IOException {
        return new String(deserializedBytes(in), StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GlobalIndexScanPlan)) {
            return false;
        }
        GlobalIndexScanPlan that = (GlobalIndexScanPlan) obj;
        return union == that.union
                && Objects.equals(predicate, that.predicate)
                && children.equals(that.children)
                && groups.equals(that.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicate, union, children, groups);
    }

    /**
     * Files read together by one index reader, grouped by primary column, index type and row range.
     * A group may overlap multiple data splits; primary and extra column predicates can share it.
     */
    private static class IndexGroup {
        /** Factory identifier used to create the index reader on the worker. */
        private final String type;

        /** The index's primary column, which need not be the column queried by this leaf. */
        private final DataField field;

        /** Other columns in the same physical index, in their original index field order. */
        private final List<DataField> extraFields;

        /** Original global row-ID range; its start is the offset for index-relative row IDs. */
        private final Range range;

        /** Selected file paths and metadata; contains no query results or open readers. */
        private final List<GlobalIndexIOMeta> files;

        private IndexGroup(
                String type,
                DataField field,
                List<DataField> extraFields,
                Range range,
                List<GlobalIndexIOMeta> files) {
            this.type = type;
            this.field = field;
            this.extraFields = new ArrayList<>(extraFields);
            this.range = range;
            this.files = new ArrayList<>(files);
        }

        private static List<IndexGroup> fromMetadata(
                IndexMetaFileGroup group, RowType rowType, IndexPathFactory pathFactory) {
            DataField field = group.indexField(rowType);
            List<DataField> extraFields = group.extraFields(rowType);
            List<IndexGroup> result = new ArrayList<>();
            group.metas()
                    .forEach(
                            (type, ranges) ->
                                    ranges.forEach(
                                            (range, metas) -> {
                                                List<GlobalIndexIOMeta> files = new ArrayList<>();
                                                for (IndexFileMeta meta : metas) {
                                                    files.add(
                                                            DataEvolutionGlobalIndexScanner
                                                                    .toGlobalMeta(
                                                                            meta, pathFactory));
                                                }
                                                result.add(
                                                        new IndexGroup(
                                                                type,
                                                                field,
                                                                extraFields,
                                                                range,
                                                                files));
                                            }));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof IndexGroup)) {
                return false;
            }
            IndexGroup that = (IndexGroup) obj;
            return type.equals(that.type)
                    && field.equals(that.field)
                    && extraFields.equals(that.extraFields)
                    && range.equals(that.range)
                    && files.equals(that.files);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, field, extraFields, range, files);
        }
    }
}
