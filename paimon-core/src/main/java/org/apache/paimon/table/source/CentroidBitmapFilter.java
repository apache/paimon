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
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundFunction;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.NotIn;
import org.apache.paimon.predicate.Or;
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
        if (!CentroidIndexMeta.hasBitmapFields(meta)) {
            return null;
        }
        int[] extraFieldIds = meta.extraFieldIds();
        RowType rowType = table.rowType();
        List<DataField> fields = new ArrayList<>();
        Predicate predicate = bind(filter, rowType, extraFieldIds, fields);
        if (predicate == null) {
            return null;
        }

        IndexPathFactory paths = table.store().pathFactory().globalIndexFileFactory();
        Map<Integer, CentroidIndexFile> bitmaps = new HashMap<>();
        int nlist = -1;
        long vectorCount = 0;
        long schemaId = -1;
        for (IndexFileMeta file : files) {
            CentroidIndexFile entry = (CentroidIndexFile) CentroidIndexMeta.file(file, paths);
            if (entry.routingModel()) {
                checkState(nlist == -1, "Multiple centroid routing models.");
                nlist = entry.nlist();
            } else if (entry.scalarIndex()) {
                checkState(
                        Arrays.equals(extraFieldIds, file.globalIndexMeta().extraFieldIds()),
                        "Mixed centroid bitmap fields.");
                checkState(
                        bitmaps.put(entry.fieldId(), entry) == null,
                        "Duplicate centroid bitmap for field %s.",
                        entry.fieldId());
                long bitmapSchemaId =
                        DataEvolutionIndexSourceMeta.fromIndexFile(file).sourceSchemaId();
                checkState(bitmapSchemaId >= 0, "Missing centroid bitmap build schema.");
                checkState(
                        schemaId == -1 || schemaId == bitmapSchemaId,
                        "Mixed centroid bitmap schemas.");
                schemaId = bitmapSchemaId;
            } else {
                vectorCount = Math.addExact(vectorCount, entry.rowCount());
            }
        }
        checkState(nlist >= 0, "Missing centroid routing model.");
        if (vectorCount == 0) {
            checkState(bitmaps.isEmpty(), "Unexpected bitmap for an empty centroid range.");
            return null;
        }
        checkState(
                bitmaps.size() == extraFieldIds.length,
                "Centroid bitmap files do not cover the extra fields.");
        for (int fieldId : extraFieldIds) {
            CentroidIndexFile bitmap = bitmaps.get(fieldId);
            checkState(bitmap != null, "Missing centroid bitmap for field %s.", fieldId);
            checkState(
                    bitmap.rowCount() == vectorCount,
                    "Centroid bitmap row count differs from the total vector count.");
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
        return CentroidBitmapIndex.filter(
                file -> table.fileIO().newInputStream(file.filePath()),
                bitmaps,
                rowType,
                buildType,
                predicate,
                meta.rowRange().count(),
                nlist);
    }

    @Nullable
    private static Predicate bind(
            Predicate predicate, RowType rowType, int[] extraFieldIds, List<DataField> fields) {
        if (predicate instanceof CompoundPredicate) {
            CompoundFunction function = ((CompoundPredicate) predicate).function();
            List<Predicate> children;
            if (Or.INSTANCE.equals(function)) {
                children = PredicateBuilder.splitOr(predicate);
                LeafPredicate in = sameColumnIn(children);
                if (in != null) {
                    return bind(in, rowType, extraFieldIds, fields);
                }
            } else if (And.INSTANCE.equals(function)) {
                children = PredicateBuilder.splitAnd(predicate);
            } else {
                return null;
            }
            if (children.isEmpty()) {
                return null;
            }

            List<Predicate> boundChildren = new ArrayList<>(children.size());
            for (Predicate child : children) {
                Predicate bound = bind(child, rowType, extraFieldIds, fields);
                if (bound == null) {
                    return null;
                }
                boundChildren.add(bound);
            }
            return new CompoundPredicate(function, boundChildren);
        }
        if (!(predicate instanceof LeafPredicate)) {
            return null;
        }
        LeafPredicate leaf = (LeafPredicate) predicate;
        Optional<FieldRef> fieldRef = leaf.fieldRefOptional();
        if (!fieldRef.isPresent()
                || (!Equal.INSTANCE.equals(leaf.function())
                        && !NotEqual.INSTANCE.equals(leaf.function())
                        && !In.INSTANCE.equals(leaf.function())
                        && !NotIn.INSTANCE.equals(leaf.function())
                        && !IsNotNull.INSTANCE.equals(leaf.function())
                        && !IsNull.INSTANCE.equals(leaf.function()))
                || !rowType.containsField(fieldRef.get().name())) {
            return null;
        }
        DataField field = rowType.getField(fieldRef.get().name());
        if (!field.type().equalsIgnoreNullable(fieldRef.get().type())
                || Arrays.stream(extraFieldIds).noneMatch(id -> id == field.id())) {
            return null;
        }
        fields.add(field);
        return leaf;
    }

    @Nullable
    private static LeafPredicate sameColumnIn(List<Predicate> children) {
        // PredicateBuilder represents short IN lists as an OR of equalities.
        LeafPredicate first = null;
        List<Object> values = new ArrayList<>();
        for (Predicate child : children) {
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
