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

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateRemapper;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.NestedProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * The physical read type and ordered transformations for one reader. Create a new instance for each
 * split so authorization rules and query settings cannot leak between readers.
 */
public final class ReadTransform {

    private final RowType readType;
    private final RowType outputType;
    @Nullable private final TableQueryAuthResult authResult;
    private final Map<String, Transform> masking;
    @Nullable private final Predicate filter;

    private ReadTransform(
            RowType readType,
            RowType outputType,
            @Nullable TableQueryAuthResult authResult,
            Map<String, Transform> masking,
            @Nullable Predicate filter) {
        this.readType = readType;
        this.outputType = outputType;
        this.authResult = authResult;
        this.masking = masking;
        this.filter = filter;
    }

    public static ReadTransform create(
            RowType tableType,
            RowType outputType,
            @Nullable Predicate queryFilter,
            boolean executeFilter,
            @Nullable TableQueryAuthResult authResult,
            Set<String> resolvedBlobViewFields) {
        Map<String, Transform> masks =
                authResult == null ? Collections.emptyMap() : authResult.extractColumnMasking();
        // Without executeFilter, engines still rely on us to evaluate conjuncts on masked columns.
        Predicate filter =
                executeFilter ? queryFilter : maskedQueryFilter(queryFilter, masks.keySet());
        RowType filterType =
                executeFilter && queryFilter != null
                        ? withFilterFields(tableType, outputType, queryFilter)
                        : outputType;
        Set<String> required = requiredFields(outputType, filter, authResult);
        if (authResult != null) {
            authResult.validateReadType(tableType, filterType, required, resolvedBlobViewFields);
        }
        RowType readType = TypeUtils.withMissingFields(tableType, filterType, required);

        Map<String, Transform> selectedMasks = new HashMap<>();
        Set<String> active = new HashSet<>(filterType.getFieldNames());
        active.addAll(required);
        for (Map.Entry<String, Transform> mask : masks.entrySet()) {
            if (active.contains(mask.getKey())) {
                selectedMasks.put(mask.getKey(), mask.getValue());
            }
        }
        if (filter != null) {
            try {
                filter = PredicateRemapper.remap(filter, readType);
            } catch (RuntimeException e) {
                if (executeFilter) {
                    throw e;
                }
                throw new IllegalStateException(
                        "Filter on masked columns "
                                + masks.keySet()
                                + " cannot be evaluated on read schema "
                                + readType.getFieldNames(),
                        e);
            }
        }
        return new ReadTransform(readType, outputType, authResult, selectedMasks, filter);
    }

    /**
     * Columns needed in addition to the output projection. Planning passes the full query filter so
     * its files survive pruning; readers pass the query predicate they will actually execute.
     */
    public static Set<String> requiredFields(
            RowType outputType,
            @Nullable Predicate queryFilter,
            @Nullable TableQueryAuthResult authResult) {
        Set<String> fields = new HashSet<>(PredicateVisitor.collectFieldNames(queryFilter));
        if (authResult != null) {
            Set<String> visible = new HashSet<>(outputType.getFieldNames());
            visible.addAll(fields);
            fields.addAll(authResult.authFields(new ArrayList<>(visible), queryFilter));
        }
        return fields;
    }

    public RowType readType() {
        return readType;
    }

    /** Apply authorization, masking, the query filter, and the output projection, in that order. */
    public RecordReader<InternalRow> apply(RecordReader<InternalRow> reader) {
        if (authResult != null) {
            reader = authResult.doAuth(reader, readType, authResult.extractPredicate(), masking);
        }
        if (filter != null) {
            reader = reader.filter(filter::test);
        }
        NestedProjectedRow projection = NestedProjectedRow.create(readType, outputType);
        return projection == null ? reader : reader.transform(projection::replaceRow);
    }

    @Nullable
    private static Predicate maskedQueryFilter(
            @Nullable Predicate filter, Set<String> maskTargets) {
        return filter == null || maskTargets.isEmpty()
                ? null
                : TableQueryAuthResult.retainFields(filter, maskTargets);
    }

    private static RowType withFilterFields(
            RowType tableType, RowType readType, Predicate predicate) {
        Set<String> fields = PredicateVisitor.collectFieldNames(predicate);
        RowType widened = TypeUtils.withMissingFields(tableType, readType, fields);
        List<DataField> fullFields = new ArrayList<>(widened.getFields());
        for (int i = 0; i < fullFields.size(); i++) {
            DataField field = fullFields.get(i);
            if (fields.contains(field.name()) && tableType.containsField(field.name())) {
                // Filter operands use their full type; nested pruning is restored after filtering.
                fullFields.set(i, tableType.getField(field.name()));
            }
        }
        checkArgument(
                widened.getFieldNames().containsAll(fields),
                "Cannot execute filter on fields %s with read type %s.",
                fields,
                widened);
        return widened.copy(fullFields);
    }
}
