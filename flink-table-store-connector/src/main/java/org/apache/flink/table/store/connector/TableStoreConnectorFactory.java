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

package org.apache.flink.table.store.connector;

import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.file.catalog.CatalogLock;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.options.Options;

import javax.annotation.Nullable;

import static org.apache.flink.table.store.CoreOptions.AUTO_CREATE;
import static org.apache.flink.table.store.connector.FlinkCatalogFactory.IDENTIFIER;

/** A table store {@link DynamicTableFactory} to create source and sink. */
public class TableStoreConnectorFactory extends AbstractTableStoreFactory {

    @Nullable private final CatalogLock.Factory lockFactory;

    public TableStoreConnectorFactory() {
        this(null);
    }

    public TableStoreConnectorFactory(@Nullable CatalogLock.Factory lockFactory) {
        this.lockFactory = lockFactory;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        if (isFlinkTable(context)) {
            // only Flink 1.14 temporary table will come here
            return FactoryUtil.createTableSource(
                    null,
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        }
        createTableIfNeeded(context);
        return super.createDynamicTableSource(context);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        if (isFlinkTable(context)) {
            // only Flink 1.14 temporary table will come here
            return FactoryUtil.createTableSink(
                    null,
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        }
        createTableIfNeeded(context);
        TableStoreSink sink = (TableStoreSink) super.createDynamicTableSink(context);
        sink.setLockFactory(lockFactory);
        return sink;
    }

    private void createTableIfNeeded(Context context) {
        ResolvedCatalogTable table = context.getCatalogTable();
        Options options = Options.fromMap(table.getOptions());
        if (options.get(AUTO_CREATE)) {
            try {
                Path tablePath = CoreOptions.path(table.getOptions());
                SchemaManager schemaManager =
                        new SchemaManager(
                                FileIO.get(tablePath, createCatalogContext(context)), tablePath);
                if (!schemaManager.latest().isPresent()) {
                    schemaManager.createTable(FlinkCatalog.fromCatalogTable(table));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean isFlinkTable(Context context) {
        String identifier = context.getCatalogTable().getOptions().get(FactoryUtil.CONNECTOR.key());
        return identifier != null && !IDENTIFIER.equals(identifier);
    }
}
