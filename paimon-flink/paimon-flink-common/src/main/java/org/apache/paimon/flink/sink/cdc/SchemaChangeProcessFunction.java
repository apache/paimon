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

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProcessFunction} to handle {@link SchemaChange}.
 *
 * <p>NOTE: To avoid concurrent schema changes, the parallelism of this {@link ProcessFunction} must
 * be 1.
 */
public class SchemaChangeProcessFunction extends ProcessFunction<SchemaChange, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeProcessFunction.class);

    private final SchemaManager schemaManager;

    public SchemaChangeProcessFunction(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    @Override
    public void processElement(
            SchemaChange schemaChange, Context context, Collector<Void> collector)
            throws Exception {
        Preconditions.checkArgument(
                schemaChange instanceof SchemaChange.AddColumn,
                "Currently, only SchemaChange.AddColumn is supported.");
        try {
            schemaManager.commitChanges(schemaChange);
        } catch (Exception e) {
            // This is normal. For example when a table is split into multiple database tables, all
            // these tables will be added the same column. However schemaManager can't handle
            // duplicated column adds, so we just catch the exception and log it.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to perform schema change {}", schemaChange, e);
            }
        }
    }
}
