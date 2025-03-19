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
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * A {@link ProcessFunction} to handle schema changes. New schema is represented by a {@link
 * CdcSchema}.
 *
 * <p>NOTE: To avoid concurrent schema changes, the parallelism of this {@link ProcessFunction} must
 * be 1.
 */
public class UpdatedDataFieldsProcessFunction
        extends UpdatedDataFieldsProcessFunctionBase<CdcSchema, Void> {

    private final SchemaManager schemaManager;

    private final Identifier identifier;

    public UpdatedDataFieldsProcessFunction(
            SchemaManager schemaManager,
            Identifier identifier,
            CatalogLoader catalogLoader,
            TypeMapping typeMapping) {
        super(catalogLoader, typeMapping);
        this.schemaManager = schemaManager;
        this.identifier = identifier;
    }

    @Override
    public void processElement(CdcSchema updatedSchema, Context context, Collector<Void> collector)
            throws Exception {
        for (SchemaChange schemaChange : extractSchemaChanges(schemaManager, updatedSchema)) {
            applySchemaChange(schemaManager, schemaChange, identifier);
        }
    }
}
