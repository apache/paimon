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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.utils.TableEnvironmentUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Abstract base of {@link Action} for table. */
public abstract class TableActionBase extends FlinkActionEnvironmentBase {

    private static final Logger LOG = LoggerFactory.getLogger(TableActionBase.class);

    protected Table table;

    TableActionBase(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig) {
        super(warehouse, databaseName, tableName, catalogConfig);
        try {
            table = catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            LOG.error("Table doesn't exist in given path.", e);
            System.err.println("Table doesn't exist in given path.");
            throw new RuntimeException(e);
        }
    }

    /** Sink {@link DataStream} dataStream to table with Flink Table API in batch environment. */
    protected void batchSink(DataStream<RowData> dataStream) {
        List<Transformation<?>> transformations =
                Collections.singletonList(
                        new FlinkSinkBuilder((FileStoreTable) table)
                                .withInput(dataStream)
                                .build()
                                .getTransformation());

        List<String> sinkIdentifierNames = Collections.singletonList(identifier.getFullName());

        TableEnvironmentUtils.executeInternal(batchTEnv, transformations, sinkIdentifierNames);
    }

    /**
     * The {@link CoreOptions.MergeEngine}s will process -U/-D records in different ways, but we
     * want these records to be sunk directly. This method is a workaround. Actions that may produce
     * -U/-D records can call this to disable merge engine settings and force compaction.
     */
    protected void changeIgnoreMergeEngine() {
        if (CoreOptions.fromMap(table.options()).mergeEngine()
                != CoreOptions.MergeEngine.DEDUPLICATE) {
            Map<String, String> dynamicOptions = new HashMap<>();
            dynamicOptions.put(
                    CoreOptions.MERGE_ENGINE.key(), CoreOptions.MergeEngine.DEDUPLICATE.toString());
            // force compaction
            dynamicOptions.put(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(), "1");
            Preconditions.checkArgument(
                    table instanceof FileStoreTable, "Only supports FileStoreTable.");
            table = ((FileStoreTable) table).internalCopyWithoutCheck(dynamicOptions);
        }
    }
}
