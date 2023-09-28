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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProcessFunction} to handle schema changes. New schema is represented by a list of {@link
 * DataField}s.
 *
 * <p>NOTE: To avoid concurrent schema changes, the parallelism of this {@link ProcessFunction} must
 * be 1.
 */
public class MultiTableUpdatedDataFieldsProcessFunction
        extends UpdatedDataFieldsProcessFunctionBase<Tuple2<Identifier, Schema>, Void> {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiTableUpdatedDataFieldsProcessFunction.class);

    public MultiTableUpdatedDataFieldsProcessFunction(Catalog.Loader catalogLoader) {
        super(catalogLoader);
    }

    @Override
    public void processElement(
            Tuple2<Identifier, Schema> tableChange, Context context, Collector<Void> collector)
            throws Exception {
        Identifier tableId = tableChange.f0;
        try {
            commitSchemaChange(tableChange.f0, tableChange.f1);
        } catch (Catalog.TableNotExistException e) {
            LOG.error("Failed to get schema manager for table {}.", tableId);
        }
    }
}
