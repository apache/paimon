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

package org.apache.paimon.presto;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.StringUtils;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Presto {@link ConnectorMetadata}. */
public class PrestoMetadata implements ConnectorMetadata {

    private final Catalog catalog;
    private final TypeManager typeManager;

    public PrestoMetadata(Options catalogOptions, TypeManager typeManager) {
        this.catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        this.typeManager = typeManager;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return listSchemaNames();
    }

    private List<String> listSchemaNames() {
        return catalog.listDatabases();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schemaName),
                "schemaName cannot be null or empty");
        return catalog.databaseExists(schemaName);
    }

    @Override
    public void createSchema(
            ConnectorSession session, String schemaName, Map<String, Object> properties) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schemaName),
                "schemaName cannot be null or empty");
        try {
            catalog.createDatabase(schemaName, true);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schemaName),
                "schemaName cannot be null or empty");
        try {
            catalog.dropDatabase(schemaName, true, true);
        } catch (Catalog.DatabaseNotExistException | Catalog.DatabaseNotEmptyException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PrestoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return getTableHandle(tableName);
    }

    public PrestoTableHandle getTableHandle(SchemaTableName tableName) {
        Identifier tablePath = new Identifier(tableName.getSchemaName(), tableName.getTableName());
        byte[] serializedTable;
        try {
            serializedTable = InstantiationUtil.serializeObject(catalog.getTable(tablePath));
        } catch (Catalog.TableNotExistException e) {
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new PrestoTableHandle(
                tableName.getSchemaName(), tableName.getTableName(), serializedTable);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns) {
        PrestoTableHandle handle = (PrestoTableHandle) table;
        ConnectorTableLayout layout =
                new ConnectorTableLayout(
                        new PrestoTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(
            ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session, ConnectorTableHandle table) {
        return ((PrestoTableHandle) table).tableMetadata(typeManager);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        PrestoTableHandle table = (PrestoTableHandle) tableHandle;
        Map<String, ColumnHandle> handleMap = new HashMap<>();
        for (ColumnMetadata column : table.columnMetadatas(typeManager)) {
            handleMap.put(column.getName(), table.columnHandle(column.getName(), typeManager));
        }
        return handleMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((PrestoColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        List<SchemaTableName> tables = new ArrayList<>();
        schemaName
                .map(Collections::singletonList)
                .orElseGet(catalog::listDatabases)
                .forEach(schema -> tables.addAll(listTables(schema)));
        return tables;
    }

    private List<SchemaTableName> listTables(String schema) {
        try {
            return catalog.listTables(schema).stream()
                    .map(table -> new SchemaTableName(schema, table))
                    .collect(toList());
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void renameTable(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SchemaTableName newTableName) {
        PrestoTableHandle oldTableHandle = (PrestoTableHandle) tableHandle;
        try {
            catalog.renameTable(
                    new Identifier(oldTableHandle.getSchemaName(), oldTableHandle.getTableName()),
                    new Identifier(newTableName.getSchemaName(), newTableName.getTableName()),
                    true);
        } catch (Catalog.TableNotExistException | Catalog.TableAlreadyExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        PrestoTableHandle prestoTableHandle = (PrestoTableHandle) tableHandle;
        try {
            catalog.dropTable(
                    new Identifier(
                            prestoTableHandle.getSchemaName(), prestoTableHandle.getTableName()),
                    true);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = Collections.singletonList(prefix.toSchemaTableName());
        } else {
            tableNames = listTables(session, Optional.of(prefix.getSchemaName()));
        }

        return tableNames.stream()
                .collect(
                        toMap(
                                Function.identity(),
                                table ->
                                        getTableHandle(session, table)
                                                .columnMetadatas(typeManager)));
    }
}
