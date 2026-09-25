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
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.predicate.PredicateVisitor.collectFieldIds;
import static org.apache.paimon.utils.SerializationUtils.deserializedBytes;
import static org.apache.paimon.utils.SerializationUtils.serializeBytes;

/**
 * A scalar index query to evaluate for one data split, represented as leaf predicates combined with
 * AND/OR.
 *
 * <p>Each data split receives the overlapping index groups; its reader executes the predicate to
 * obtain row IDs.
 */
class GlobalIndexQuery {

    /** A leaf or paired range query; null for an AND/OR node. */
    @Nullable private final Predicate predicate;

    /** For compound nodes, true means OR and false means AND; ignored for leaves. */
    private final boolean union;

    /** Indexed child queries of a compound node; empty for leaves. */
    private final List<GlobalIndexQuery> children;

    /** A query's index groups; empty when none overlap this data split. */
    private final List<IndexGroup> groups;

    private GlobalIndexQuery(
            @Nullable Predicate predicate,
            boolean union,
            List<GlobalIndexQuery> children,
            List<IndexGroup> groups) {
        this.predicate = predicate;
        this.union = union;
        this.children = children;
        this.groups = groups;
    }

    @Nullable
    static GlobalIndexQuery create(
            RowType rowType,
            Predicate predicate,
            List<IndexFileMeta> files,
            IndexPathFactory pathFactory) {
        Map<Integer, List<IndexMetaFileGroup>> groupsByField =
                DataEvolutionGlobalIndexScanner.groupIndexFiles(files);
        Map<Integer, List<IndexGroup>> groups = new LinkedHashMap<>();
        groupsByField.forEach(
                (fieldId, fieldGroups) -> {
                    List<IndexGroup> indexGroups = new ArrayList<>();
                    for (IndexMetaFileGroup group : fieldGroups) {
                        indexGroups.addAll(IndexGroup.fromMetadata(group, rowType, pathFactory));
                    }
                    groups.put(fieldId, indexGroups);
                });
        return createForPredicate(predicate, rowType, groups);
    }

    @Nullable
    private static GlobalIndexQuery createForPredicate(
            Predicate predicate, RowType rowType, Map<Integer, List<IndexGroup>> groups) {
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) predicate;
            Optional<FieldRef> field = leaf.fieldRefOptional();
            if (!field.isPresent()) {
                return null;
            }
            return createForIndexedField(leaf, field.get(), rowType, groups);
        }
        CompoundPredicate compound = (CompoundPredicate) predicate;
        boolean union = compound.function() instanceof Or;
        List<GlobalIndexQuery> children = new ArrayList<>();
        List<Predicate> predicates = GlobalIndexEvaluator.normalizedChildren(compound);
        for (int i = 0; i < predicates.size(); i++) {
            Predicate child = predicates.get(i);
            GlobalIndexQuery query = null;
            if (!union && GlobalIndexEvaluator.isRangeBound(child)) {
                LeafPredicate first = (LeafPredicate) child;
                for (int j = i + 1; j < predicates.size(); j++) {
                    Predicate other = predicates.get(j);
                    if (!GlobalIndexEvaluator.isRangeBound(other)) {
                        continue;
                    }
                    LeafPredicate second = (LeafPredicate) other;
                    if (GlobalIndexEvaluator.isLowerBound(first)
                                    == GlobalIndexEvaluator.isLowerBound(second)
                            || !first.fieldRefOptional().equals(second.fieldRefOptional())) {
                        continue;
                    }
                    LeafPredicate lower = GlobalIndexEvaluator.isLowerBound(first) ? first : second;
                    LeafPredicate upper = GlobalIndexEvaluator.isLowerBound(first) ? second : first;
                    Predicate rangeQuery =
                            new CompoundPredicate(And.INSTANCE, Arrays.asList(lower, upper));
                    query =
                            createForIndexedField(
                                    rangeQuery, lower.fieldRefOptional().get(), rowType, groups);
                    if (query != null) {
                        predicates.remove(j);
                        break;
                    }
                }
            }
            if (query == null) {
                query = createForPredicate(child, rowType, groups);
            }
            if (query == null) {
                if (union) {
                    return null;
                }
            } else {
                children.add(query);
            }
        }
        return children.isEmpty()
                ? null
                : new GlobalIndexQuery(null, union, children, Collections.emptyList());
    }

    @Nullable
    private static GlobalIndexQuery createForIndexedField(
            Predicate predicate,
            FieldRef field,
            RowType rowType,
            Map<Integer, List<IndexGroup>> groups) {
        List<IndexGroup> fieldGroups = groups.get(rowType.getField(field.name()).id());
        if (fieldGroups == null) {
            return null;
        }
        List<IndexGroup> selectedGroups = new ArrayList<>();
        for (IndexGroup group : fieldGroups) {
            List<GlobalIndexIOMeta> selectedFiles =
                    GlobalIndexerFactoryUtils.selectFiles(
                            group.type, group.field, group.extraFields, predicate, group.files);
            if (!selectedFiles.isEmpty()) {
                selectedGroups.add(
                        selectedFiles == group.files
                                ? group
                                : new IndexGroup(
                                        group.type,
                                        group.field,
                                        group.extraFields,
                                        group.range,
                                        selectedFiles));
            }
        }
        return new GlobalIndexQuery(predicate, false, Collections.emptyList(), selectedGroups);
    }

    boolean isEmpty() {
        if (predicate != null) {
            return groups.isEmpty();
        }
        return union
                ? children.stream().allMatch(GlobalIndexQuery::isEmpty)
                : children.stream().anyMatch(GlobalIndexQuery::isEmpty);
    }

    /** Residual predicates discarded during planning must not expand unindexed coverage. */
    Set<Integer> contributingFieldIds(RowType rowType) {
        Set<Integer> fields = new HashSet<>();
        if (predicate != null) {
            fields.addAll(collectFieldIds(rowType, predicate));
        } else {
            for (GlobalIndexQuery child : children) {
                fields.addAll(child.contributingFieldIds(rowType));
            }
        }
        return fields;
    }

    /** Keep original row-ID offsets while removing groups unrelated to this data split. */
    GlobalIndexQuery forRanges(List<Range> ranges) {
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
        List<GlobalIndexQuery> selectedChildren = new ArrayList<>();
        for (GlobalIndexQuery child : children) {
            selectedChildren.add(child.forRanges(ranges));
        }
        // A query without a local group evaluates to an empty result.
        return new GlobalIndexQuery(predicate, union, selectedChildren, selected);
    }

    GlobalIndexResult evaluate(FileIO fileIO, Options options, List<Range> ranges)
            throws IOException {
        ExecutorService executor =
                GlobalIndexReadThreadPool.getExecutorService(options.get(GLOBAL_INDEX_THREAD_NUM));
        return evaluateWithExecutor(fileIO, options, ranges, executor);
    }

    private GlobalIndexResult evaluateWithExecutor(
            FileIO fileIO, Options options, List<Range> ranges, ExecutorService executor)
            throws IOException {
        if (predicate == null) {
            GlobalIndexResult result = null;
            for (GlobalIndexQuery child : children) {
                GlobalIndexResult matches =
                        child.evaluateWithExecutor(fileIO, options, ranges, executor);
                result =
                        result == null ? matches : union ? result.or(matches) : result.and(matches);
            }
            return result == null ? GlobalIndexResult.createEmpty() : result;
        }
        Function<GlobalIndexReader, CompletableFuture<Optional<GlobalIndexResult>>> query =
                predicateQuery();
        GlobalIndexResult result = GlobalIndexResult.createEmpty();
        for (IndexGroup group : groups) {
            GlobalIndexResult splitRows = localSplitRows(ranges, group.range);
            if (splitRows.results().isEmpty()) {
                continue;
            }
            GlobalIndexer indexer =
                    GlobalIndexerFactoryUtils.load(group.type)
                            .create(group.field, group.extraFields, options);
            try (GlobalIndexReader reader =
                    indexer.createReader(
                            meta -> fileIO.newInputStream(meta.filePath()),
                            group.files,
                            group.range.count(),
                            splitRows.results().toRangeList(),
                            executor)) {
                Optional<GlobalIndexResult> matches = query.apply(reader).get();
                if (!matches.isPresent()) {
                    throw new IOException("Index reader does not support predicate: " + predicate);
                }
                // Clip in index-local coordinates before offset() iterates the retained rows.
                result = result.or(splitRows.and(matches.get()).offset(group.range.from));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while evaluating index query split", e);
            } catch (ExecutionException e) {
                throw new IOException("Failed to evaluate index query split", e.getCause());
            }
        }
        return result;
    }

    private Function<GlobalIndexReader, CompletableFuture<Optional<GlobalIndexResult>>>
            predicateQuery() {
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) predicate;
            return reader ->
                    leaf.function().visit(reader, leaf.fieldRefOptional().get(), leaf.literals());
        }
        List<Predicate> bounds = ((CompoundPredicate) predicate).children();
        LeafPredicate lower = (LeafPredicate) bounds.get(0);
        LeafPredicate upper = (LeafPredicate) bounds.get(1);
        FieldRef field = lower.fieldRefOptional().get();
        return reader ->
                reader.visitRange(
                        field,
                        lower.literals().get(0),
                        upper.literals().get(0),
                        lower.function() instanceof GreaterOrEqual,
                        upper.function() instanceof LessOrEqual);
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
                    out.writeBoolean(file.metadata() != null);
                    if (file.metadata() != null) {
                        serializeBytes(out, file.metadata());
                    }
                }
            }
        } else {
            out.writeInt(children.size());
            for (GlobalIndexQuery child : children) {
                child.serialize(out);
            }
        }
    }

    static GlobalIndexQuery deserialize(DataInputView in) throws IOException {
        int type = in.readByte();
        if (type == 0) {
            Predicate predicate;
            try {
                predicate =
                        InstantiationUtil.deserializeObject(
                                deserializedBytes(in), GlobalIndexQuery.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize index query predicate", e);
            }
            if (!(predicate instanceof LeafPredicate)
                    && !(predicate instanceof CompoundPredicate)) {
                throw new IOException("Expected an index predicate");
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
                    byte[] metadata = in.readBoolean() ? deserializedBytes(in) : null;
                    files.add(new GlobalIndexIOMeta(path, fileSize, rowCount, metadata));
                }
                groups.add(new IndexGroup(indexType, field, extraFields, range, files));
            }
            return new GlobalIndexQuery(predicate, false, Collections.emptyList(), groups);
        }
        if (type != 1 && type != 2) {
            throw new IOException("Unknown index query node: " + type);
        }
        List<GlobalIndexQuery> children = new ArrayList<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            children.add(deserialize(in));
        }
        return new GlobalIndexQuery(null, type == 2, children, Collections.emptyList());
    }

    static void writeString(DataOutputView out, String value) throws IOException {
        serializeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    static String readString(DataInputView in) throws IOException {
        return new String(deserializedBytes(in), StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GlobalIndexQuery)) {
            return false;
        }
        GlobalIndexQuery that = (GlobalIndexQuery) obj;
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

        /** File paths and metadata; contains no query results or open readers. */
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
