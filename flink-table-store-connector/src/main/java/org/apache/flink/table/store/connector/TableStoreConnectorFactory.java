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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.connector.sink.TableStoreSink;
import org.apache.flink.table.store.file.catalog.CatalogLock;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.store.CoreOptions.PATH;
import static org.apache.flink.table.store.connector.FlinkCatalogFactory.DEFAULT_DATABASE;
import static org.apache.flink.table.store.connector.FlinkCatalogFactory.IDENTIFIER;

/** A table store {@link DynamicTableFactory} to create source and sink. */
public class TableStoreConnectorFactory extends AbstractTableStoreFactory {

    protected static final ConfigOption<String> CATALOG_NAME =
            ConfigOptions.key("catalog-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table-store catalog name.");

    protected static final ConfigOption<String> CATALOG_TABLE =
            ConfigOptions.key("catalog-table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table-store catalog table name.");

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
        String identifier = context.getCatalogTable().getOptions().get(FactoryUtil.CONNECTOR.key());
        if (identifier != null && !IDENTIFIER.equals(identifier)) {
            // only Flink 1.14 temporary table will come here
            return FactoryUtil.createTableSource(
                    null,
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        } else if (IDENTIFIER.equals(identifier)) { // create table-store table with connector='table-store'
            createTableLoader(context);
        }

        return super.createDynamicTableSource(context);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        String identifier = context.getCatalogTable().getOptions().get(FactoryUtil.CONNECTOR.key());
        if (identifier != null && !IDENTIFIER.equals(identifier)) {
            // only Flink 1.14 temporary table will come here
            return FactoryUtil.createTableSink(
                    null,
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        } else if (IDENTIFIER.equals(identifier)) { // create table-store table with connector='table-store'
            createTableLoader(context);
        }

        TableStoreSink sink = (TableStoreSink) super.createDynamicTableSink(context);
        sink.setLockFactory(lockFactory);
        return sink;
    }

    private static void createTableLoader(Context context) {
        ObjectPath defaultObjectPath = context.getObjectIdentifier().toObjectPath();
        Map<String, String> tableOpts = context.getCatalogTable().getOptions();

        String catalogName = tableOpts.get(CATALOG_NAME.key());
        String catalogDatabase = tableOpts.getOrDefault(DEFAULT_DATABASE.key(), defaultObjectPath.getDatabaseName());
        String catalogTable = tableOpts.getOrDefault(CATALOG_TABLE.key(), defaultObjectPath.getObjectName());

        FlinkCatalog catalog = FlinkCatalogFactory.createCatalog(catalogName, Configuration.fromMap(tableOpts));

        ObjectPath objectPath = new ObjectPath(catalogDatabase, catalogTable);

        if (!catalog.tableExists(objectPath)) {
            try {
                catalog.createTable(objectPath, context.getCatalogTable(), false);
            } catch (TableAlreadyExistException e) {
                throw new RuntimeException(String.format("Table (or view) %s already exists in Catalog %s.", catalogTable, catalogName));
            } catch (DatabaseNotExistException e) {
                throw new RuntimeException(String.format("Database %s does not exist in Catalog %s.", catalogDatabase, catalogName));
            }
        }

        Path tablePath = catalog.catalog().getTableLocation(objectPath);
        context.getCatalogTable().getOptions().put(PATH.key(), tablePath.toString());
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CATALOG_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(CATALOG_TABLE);
        return options;
    }
}
