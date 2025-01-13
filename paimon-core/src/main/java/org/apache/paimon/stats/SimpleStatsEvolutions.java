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
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.paimon.schema.SchemaEvolutionUtil.createIndexCastMapping;

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

    @Nullable
    public Predicate tryDevolveFilter(long dataSchemaId, Predicate filter) {
        if (tableSchemaId == dataSchemaId) {
            return filter;
        }
        List<Predicate> devolved =
                Objects.requireNonNull(
                        SchemaEvolutionUtil.devolveDataFilters(
                                schemaFields.apply(tableSchemaId),
                                schemaFields.apply(dataSchemaId),
                                Collections.singletonList(filter)));
        return devolved.isEmpty() ? null : devolved.get(0);
    }

    public List<DataField> tableDataFields() {
        return tableDataFields;
    }
}
