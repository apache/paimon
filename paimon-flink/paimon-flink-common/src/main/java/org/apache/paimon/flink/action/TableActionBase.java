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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.table.Table;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Abstract base of {@link Action} for table. */
public abstract class TableActionBase extends ActionBase {

    protected Table table;
    protected final Identifier identifier;

    TableActionBase(String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(catalogConfig);
        identifier = new Identifier(databaseName, tableName);
        try {
            table = catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    /** Sink {@link DataStream} dataStream to table with Flink Table API in batch environment. */
    public TableResult batchSink(DataStream<RowData> dataStream) {
        List<Transformation<?>> transformations =
                Collections.singletonList(
                        new FlinkSinkBuilder(table)
                                .forRowData(dataStream)
                                .build()
                                .getTransformation());

        List<String> sinkIdentifierNames = Collections.singletonList(identifier.getFullName());

        return executeInternal(transformations, sinkIdentifierNames);
    }

    /**
     * Invoke {@code TableEnvironmentImpl#executeInternal(List<Transformation<?>>, List<String>)}
     * from a {@link StreamTableEnvironment} instance through reflecting.
     */
    private TableResult executeInternal(
            List<Transformation<?>> transformations, List<String> sinkIdentifierNames) {
        Class<?> clazz = batchTEnv.getClass().getSuperclass().getSuperclass();
        try {
            Method executeInternal =
                    clazz.getDeclaredMethod("executeInternal", List.class, List.class);
            executeInternal.setAccessible(true);

            return (TableResult)
                    executeInternal.invoke(batchTEnv, transformations, sinkIdentifierNames);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                    "Failed to get 'TableEnvironmentImpl#executeInternal(List, List)' method "
                            + "from given StreamTableEnvironment instance by Java reflection. This is unexpected.",
                    e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(
                    "Failed to invoke 'TableEnvironmentImpl#executeInternal(List, List)' method "
                            + "from given StreamTableEnvironment instance by Java reflection. This is unexpected.",
                    e);
        }
    }
}
