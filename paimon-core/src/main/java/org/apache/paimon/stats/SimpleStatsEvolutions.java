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

package org.apache.paimon.stats;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.paimon.schema.SchemaEvolutionUtil.createIndexCastMapping;
import static org.apache.paimon.schema.SchemaEvolutionUtil.devolveFilters;

/** Converters to create col stats array serializer. */
public class SimpleStatsEvolutions {

    private final Function<Long, List<DataField>> schemaFields;
    private final long tableSchemaId;
    private final List<DataField> tableDataFields;
    private final AtomicReference<List<DataField>> tableFields;
    private final ConcurrentMap<Long, SimpleStatsEvolution> evolutions;

    public SimpleStatsEvolutions(Function<Long, List<DataField>> schemaFields, long tableSchemaId) {
        this.schemaFields = schemaFields;
        this.tableSchemaId = tableSchemaId;
        this.tableDataFields = schemaFields.apply(tableSchemaId);
        this.tableFields = new AtomicReference<>();
        this.evolutions = new ConcurrentHashMap<>();
    }

    public SimpleStatsEvolution getOrCreate(long dataSchemaId) {
        return evolutions.computeIfAbsent(
                dataSchemaId,
                id -> {
                    if (tableSchemaId == id) {
                        return new SimpleStatsEvolution(
                                new RowType(schemaFields.apply(id)), null, null);
                    }

                    // Get atomic schema fields.
                    List<DataField> schemaTableFields =
                            tableFields.updateAndGet(v -> v == null ? tableDataFields : v);
                    List<DataField> dataFields = schemaFields.apply(id);
                    IndexCastMapping indexCastMapping =
                            createIndexCastMapping(schemaTableFields, schemaFields.apply(id));
                    @Nullable int[] indexMapping = indexCastMapping.getIndexMapping();
                    // Create col stats array serializer with schema evolution
                    return new SimpleStatsEvolution(
                            new RowType(dataFields),
                            indexMapping,
                            indexCastMapping.getCastMapping());
                });
    }

    /**
     * If the file's schema id != current table schema id, convert the filter to evolution safe
     * filter or null if can't.
     */
    @Nullable
    public Predicate tryDevolveFilter(long dataSchemaId, @Nullable Predicate filter) {
        if (filter == null || dataSchemaId == tableSchemaId) {
            return filter;
        }

        // Filter p1 && p2, if only p1 is safe, we can return only p1 to try best filter and let the
        // compute engine to perform p2.
        List<Predicate> filters = PredicateBuilder.splitAnd(filter);
        List<Predicate> devolved =
                devolveFilters(tableDataFields, schemaFields.apply(dataSchemaId), filters, false);

        return devolved.isEmpty() ? null : PredicateBuilder.and(devolved);
    }

    /**
     * Filter unsafe filter, for example, filter is 'a > 9', old type is String, new type is Int, if
     * records are 9, 10 and 11, the evolved filter is not safe.
     */
    @Nullable
    public Predicate filterUnsafeFilter(
            long dataSchemaId, @Nullable Predicate filter, boolean keepNewFieldFilter) {
        if (filter == null || dataSchemaId == tableSchemaId) {
            return filter;
        }

        List<Predicate> filters = PredicateBuilder.splitAnd(filter);
        List<DataField> oldSchema = schemaFields.apply(dataSchemaId);
        List<Predicate> result = new ArrayList<>();
        for (Predicate predicate : filters) {
            if (!devolveFilters(
                            tableDataFields,
                            oldSchema,
                            singletonList(predicate),
                            keepNewFieldFilter)
                    .isEmpty()) {
                result.add(predicate);
            }
        }
        return result.isEmpty() ? null : PredicateBuilder.and(result);
    }

    public List<DataField> tableDataFields() {
        return tableDataFields;
    }
}
