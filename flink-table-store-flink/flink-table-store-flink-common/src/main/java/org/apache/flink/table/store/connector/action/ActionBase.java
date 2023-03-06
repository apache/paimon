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

package org.apache.flink.table.store.connector.action;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.catalog.CatalogContext;
import org.apache.flink.table.store.connector.FlinkCatalog;
import org.apache.flink.table.store.connector.LogicalTypeConversion;
import org.apache.flink.table.store.connector.sink.FlinkSinkBuilder;
import org.apache.flink.table.store.connector.utils.TableEnvironmentUtils;
import org.apache.flink.table.store.file.catalog.Catalog;
import org.apache.flink.table.store.file.catalog.CatalogFactory;
import org.apache.flink.table.store.file.catalog.Identifier;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.options.CatalogOptions;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DataTypeCasts;
import org.apache.flink.table.types.logical.LogicalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.catalog.Catalog.DEFAULT_DATABASE;

/** Abstract base of {@link Action}. */
public abstract class ActionBase implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(ActionBase.class);

    protected Catalog catalog;

    protected final FlinkCatalog flinkCatalog;

    protected StreamExecutionEnvironment env;

    protected StreamTableEnvironment tEnv;

    protected Identifier identifier;

    protected Table table;

    ActionBase(String warehouse, String databaseName, String tableName) {
        this(warehouse, databaseName, tableName, new Options());
    }

    ActionBase(String warehouse, String databaseName, String tableName, Options options) {
        identifier = new Identifier(databaseName, tableName);
        catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                new Options().set(CatalogOptions.WAREHOUSE, warehouse)));
        flinkCatalog = new FlinkCatalog(catalog, "table-store", DEFAULT_DATABASE);

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());

        // register flink catalog to table environment
        tEnv.registerCatalog(flinkCatalog.getName(), flinkCatalog);
        tEnv.useCatalog(flinkCatalog.getName());

        try {
            table = catalog.getTable(identifier);
            if (options.toMap().size() > 0) {
                table = table.copy(options.toMap());
            }
        } catch (Catalog.TableNotExistException e) {
            LOG.error("Table doesn't exist in given path.", e);
            System.err.println("Table doesn't exist in given path.");
            throw new RuntimeException(e);
        }
    }

    /**
     * Extract {@link LogicalType}s from Flink {@link org.apache.flink.table.types.DataType}s and
     * convert to Table Store {@link DataType}s.
     */
    protected List<DataType> toTableStoreDataTypes(
            List<org.apache.flink.table.types.DataType> flinkDataTypes) {
        return flinkDataTypes.stream()
                .map(org.apache.flink.table.types.DataType::getLogicalType)
                .map(LogicalTypeConversion::toDataType)
                .collect(Collectors.toList());
    }

    /**
     * Check whether each {@link DataType} of actualTypes is compatible with that of expectedTypes
     * respectively.
     */
    protected boolean compatibleCheck(List<DataType> actualTypes, List<DataType> expectedTypes) {
        if (actualTypes.size() != expectedTypes.size()) {
            return false;
        }

        for (int i = 0; i < actualTypes.size(); i++) {
            if (!DataTypeCasts.supportsCompatibleCast(actualTypes.get(i), expectedTypes.get(i))) {
                return false;
            }
        }

        return true;
    }

    /** Sink {@link DataStream} dataStream to table with Flink Table API in batch environment. */
    protected void batchSink(DataStream<RowData> dataStream) {
        List<Transformation<?>> transformations =
                Collections.singletonList(
                        new FlinkSinkBuilder((FileStoreTable) table)
                                .withInput(dataStream)
                                .withLockFactory(
                                        Lock.factory(
                                                catalog.lockFactory().orElse(null), identifier))
                                .build()
                                .getTransformation());

        List<String> sinkIdentifierNames = Collections.singletonList(identifier.getFullName());

        TableEnvironmentUtils.executeInternal(tEnv, transformations, sinkIdentifierNames);
    }
}
