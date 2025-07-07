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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FieldIdentifier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link ProcessFunction} to handle schema changes. New schema is represented by a {@link
 * CdcSchema}.
 *
 * <p>NOTE: To avoid concurrent schema changes, the parallelism of this {@link ProcessFunction} must
 * be 1.
 */
public class MultiTableUpdatedDataFieldsProcessFunction
        extends UpdatedDataFieldsProcessFunctionBase<Tuple2<Identifier, CdcSchema>, Void> {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiTableUpdatedDataFieldsProcessFunction.class);

    private final Map<Identifier, SchemaManager> schemaManagers = new HashMap<>();

    private final Map<Identifier, Set<FieldIdentifier>> latestFieldsMap = new HashMap<>();

    public MultiTableUpdatedDataFieldsProcessFunction(
            CatalogLoader catalogLoader, TypeMapping typeMapping) {
        super(catalogLoader, typeMapping);
    }

    @Override
    public void processElement(
            Tuple2<Identifier, CdcSchema> updatedSchema, Context context, Collector<Void> collector)
            throws Exception {
        Identifier tableId = updatedSchema.f0;
        SchemaManager schemaManager =
                schemaManagers.computeIfAbsent(
                        tableId,
                        id -> {
                            FileStoreTable table;
                            try {
                                table = (FileStoreTable) catalog.getTable(tableId);
                            } catch (Catalog.TableNotExistException e) {
                                return null;
                            }
                            return new SchemaManager(table.fileIO(), table.location());
                        });
        if (Objects.isNull(schemaManager)) {
            LOG.error("Failed to get schema manager for table " + tableId);
            return;
        }

        Set<FieldIdentifier> latestFields =
                latestFieldsMap.computeIfAbsent(tableId, id -> new HashSet<>());
        List<DataField> actualUpdatedDataFields =
                actualUpdatedDataFields(updatedSchema.f1.fields(), latestFields);

        if (actualUpdatedDataFields.isEmpty() && updatedSchema.f1.comment() == null) {
            return;
        }

        CdcSchema actualUpdatedSchema =
                new CdcSchema(
                        actualUpdatedDataFields,
                        updatedSchema.f1.primaryKeys(),
                        updatedSchema.f1.comment());
        for (SchemaChange schemaChange : extractSchemaChanges(schemaManager, actualUpdatedSchema)) {
            applySchemaChange(schemaManager, schemaChange, tableId, actualUpdatedSchema);
        }
        /*
         * Here, actualUpdatedDataFields cannot be used to update latestFields because there is a
         * non-SchemaChange.AddColumn scenario. Otherwise, the previously existing fields cannot be
         * modified again.
         */
        latestFieldsMap.put(tableId, updateLatestFields(schemaManager));
    }
}
