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

package org.apache.paimon.table.source;

import org.apache.paimon.globalindex.CentroidIndexFile;
import org.apache.paimon.globalindex.CentroidIndexMeta;
import org.apache.paimon.globalindex.bitmap.CentroidBitmapIndex;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Binds an exact scalar predicate to the bitmap files within a vector split. */
final class CentroidBitmapFilter {

    private CentroidBitmapFilter() {}

    @Nullable
    static IOFunction<Integer, RoaringNavigableMap64> create(
            FileStoreTable table,
            @Nullable Predicate filter,
            List<IndexFileMeta> files,
            Map<Long, RowType> buildSchemas) {
        if (filter == null || files.isEmpty()) {
            return null;
        }
        GlobalIndexMeta meta = files.get(0).globalIndexMeta();
        if (!CentroidIndexMeta.hasScalarIndexes(meta.indexMeta())) {
            return null;
        }
        RowType rowType = table.rowType();
        List<LeafPredicate> predicates = new ArrayList<>();
        List<DataField> fields = new ArrayList<>();
        for (Predicate predicate : PredicateBuilder.splitAnd(filter)) {
            LeafPredicate leaf = pointLookup(predicate);
            if (leaf == null) {
                return null;
            }
            Optional<FieldRef> fieldRef = leaf.fieldRefOptional();
            if (!fieldRef.isPresent()
                    || (!Equal.INSTANCE.equals(leaf.function())
                            && !In.INSTANCE.equals(leaf.function())
                            && !IsNotNull.INSTANCE.equals(leaf.function())
                            && !IsNull.INSTANCE.equals(leaf.function()))
                    || !rowType.containsField(fieldRef.get().name())) {
                return null;
            }
            DataField field = rowType.getField(fieldRef.get().name());
            if (!field.type().equalsIgnoreNullable(fieldRef.get().type())
                    || meta.extraFieldIds() == null
                    || !Arrays.stream(meta.extraFieldIds()).anyMatch(id -> id == field.id())) {
                return null;
            }
            predicates.add(leaf);
            fields.add(field);
        }

        IndexPathFactory paths = table.store().pathFactory().globalIndexFileFactory();
        Map<Integer, CentroidIndexFile> bitmaps = new HashMap<>();
        Map<Integer, CentroidIndexFile> shards = new HashMap<>();
        CentroidIndexFile router = null;
        long schemaId = -1;
        for (IndexFileMeta file : files) {
            CentroidIndexFile entry = (CentroidIndexFile) CentroidIndexMeta.file(file, paths);
            if (entry.routingModel()) {
                checkState(router == null, "Multiple centroid routing models.");
                router = entry;
            } else if (entry.scalarIndex()) {
                checkState(
                        bitmaps.put(entry.centroid(), entry) == null, "Duplicate centroid bitmap.");
                long bitmapSchemaId = CentroidBitmapIndex.schemaId(entry.metadata());
                checkState(
                        schemaId == -1 || schemaId == bitmapSchemaId,
                        "Mixed centroid bitmap schemas.");
                schemaId = bitmapSchemaId;
            } else if (entry.rowCount() > 0) {
                checkState(
                        shards.put(entry.centroid(), entry) == null, "Duplicate centroid shard.");
            }
        }
        checkState(router != null, "Missing centroid routing model.");
        checkState(
                shards.keySet().equals(bitmaps.keySet()),
                "Centroid bitmap files do not cover the shard group.");
        if (bitmaps.isEmpty()) {
            return null;
        }
        for (CentroidIndexFile bitmap : bitmaps.values()) {
            CentroidIndexFile shard = shards.get(bitmap.centroid());
            checkState(
                    bitmap.modelId().equals(router.modelId())
                            && shard.modelId().equals(router.modelId())
                            && bitmap.rowCount() == shard.rowCount(),
                    "Centroid bitmap and vector shard belong to different builds.");
        }

        RowType buildType =
                schemaId == table.schema().id()
                        ? rowType
                        : buildSchemas.computeIfAbsent(
                                schemaId, id -> table.schemaManager().schema(id).logicalRowType());
        for (DataField field : fields) {
            if (!buildType.containsField(field.id())
                    || !field.type().equalsIgnoreNullable(buildType.getField(field.id()).type())) {
                return null;
            }
        }
        return centroid ->
                CentroidBitmapIndex.match(
                        file -> table.fileIO().newInputStream(file.filePath()),
                        bitmaps.get(centroid),
                        rowType,
                        buildType,
                        predicates);
    }

    @Nullable
    private static LeafPredicate pointLookup(Predicate predicate) {
        if (predicate instanceof LeafPredicate) {
            return (LeafPredicate) predicate;
        }
        // PredicateBuilder represents short IN lists as an OR of equalities.
        LeafPredicate first = null;
        List<Object> values = new ArrayList<>();
        for (Predicate child : PredicateBuilder.splitOr(predicate)) {
            if (!(child instanceof LeafPredicate)) {
                return null;
            }
            LeafPredicate leaf = (LeafPredicate) child;
            if (!Equal.INSTANCE.equals(leaf.function()) || !leaf.fieldRefOptional().isPresent()) {
                return null;
            }
            if (first == null) {
                first = leaf;
            } else if (!first.fieldRefOptional().equals(leaf.fieldRefOptional())) {
                return null;
            }
            values.add(leaf.literals().get(0));
        }
        return first == null ? null : LeafPredicate.of(first.transform(), In.INSTANCE, values);
    }
}
