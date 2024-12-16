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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FieldIdentifier;
import org.apache.paimon.types.RowType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link ProcessFunction} to handle schema changes. New schema is represented by a list of {@link
 * DataField}s.
 *
 * <p>NOTE: To avoid concurrent schema changes, the parallelism of this {@link ProcessFunction} must
 * be 1.
 */
public class UpdatedDataFieldsProcessFunction
        extends UpdatedDataFieldsProcessFunctionBase<List<DataField>, Void> {

    private final SchemaManager schemaManager;

    private final Identifier identifier;

    private Set<FieldIdentifier> latestFields;

    public UpdatedDataFieldsProcessFunction(
            SchemaManager schemaManager, Identifier identifier, CatalogLoader catalogLoader) {
        super(catalogLoader);
        this.schemaManager = schemaManager;
        this.identifier = identifier;
        this.latestFields = new HashSet<>();
    }

    @Override
    public void processElement(
            List<DataField> updatedDataFields, Context context, Collector<Void> collector)
            throws Exception {
        List<DataField> actualUpdatedDataFields =
                updatedDataFields.stream()
                        .filter(
                                dataField ->
                                        !latestDataFieldContain(new FieldIdentifier(dataField)))
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(actualUpdatedDataFields)) {
            return;
        }
        for (SchemaChange schemaChange :
                extractSchemaChanges(schemaManager, actualUpdatedDataFields)) {
            applySchemaChange(schemaManager, schemaChange, identifier);
        }
        /**
         * Here, actualUpdatedDataFields cannot be used to update latestFields because there is a
         * non-SchemaChange.AddColumn scenario. Otherwise, the previously existing fields cannot be
         * modified again.
         */
        updateLatestFields();
    }

    private boolean latestDataFieldContain(FieldIdentifier dataField) {
        return latestFields.stream().anyMatch(previous -> Objects.equals(previous, dataField));
    }

    private void updateLatestFields() {
        RowType oldRowType = schemaManager.latest().get().logicalRowType();
        Set<FieldIdentifier> fieldIdentifiers =
                oldRowType.getFields().stream()
                        .map(item -> new FieldIdentifier(item))
                        .collect(Collectors.toSet());
        latestFields = fieldIdentifiers;
    }
}
