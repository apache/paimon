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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link ProcessFunction} to handle schema changes. New schema is represented by a list of {@link
 * DataField}s.
 *
 * <p>NOTE: To avoid concurrent schema changes, the parallelism of this {@link ProcessFunction} must
 * be 1.
 */
public class UpdatedDataFieldsProcessFunction
        extends UpdatedDataFieldsProcessFunctionBase<List<DataField>, Void>
        implements CheckpointedFunction {

    private final SchemaManager schemaManager;

    private final Identifier identifier;

    private final List<DataField> latestSchemaList;

    public UpdatedDataFieldsProcessFunction(
            SchemaManager schemaManager, Identifier identifier, Catalog.Loader catalogLoader) {
        super(catalogLoader);
        this.schemaManager = schemaManager;
        this.identifier = identifier;
        this.latestSchemaList = new ArrayList<>();
    }

    @Override
    public void processElement(
            List<DataField> updatedDataFields, Context context, Collector<Void> collector)
            throws Exception {
        List<DataField> actualUpdatedDataFields =
                updatedDataFields.stream()
                        .filter(dataField -> !dataFieldContainIgnoreId(dataField))
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(actualUpdatedDataFields)) {
            return;
        }
        for (SchemaChange schemaChange :
                extractSchemaChanges(schemaManager, actualUpdatedDataFields)) {
            applySchemaChange(schemaManager, schemaChange, identifier);
        }
        latestSchemaList.addAll(actualUpdatedDataFields);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // nothing
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        RowType oldRowType = schemaManager.latest().get().logicalRowType();
        latestSchemaList.addAll(oldRowType.getFields());
    }

    private boolean dataFieldContainIgnoreId(DataField dataField) {
        return latestSchemaList.stream()
                .anyMatch(previous -> DataField.dataFieldEqualsIgnoreId(previous, dataField));
    }
}
