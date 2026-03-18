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

package org.apache.paimon.flink.pipeline.cdc.schema;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.pipeline.cdc.util.PaimonToFlinkCDCTypeConverter.convertPaimonSchemaToFlinkCDCSchema;

/** The {@link MetadataAccessor} for cdc source. */
public class CDCMetadataAccessor implements MetadataAccessor {
    private static final Logger LOG = LoggerFactory.getLogger(CDCMetadataAccessor.class);

    private final Catalog catalog;

    public CDCMetadataAccessor(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public List<String> listNamespaces() {
        throw new UnsupportedOperationException("Paimon does not support namespaces");
    }

    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        if (namespace != null) {
            throw new UnsupportedOperationException("Paimon does not support namespaces");
        }

        return catalog.listDatabases();
    }

    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName) {
        if (namespace != null) {
            throw new UnsupportedOperationException("Paimon does not support namespaces");
        }

        List<String> databaseNames;
        if (schemaName == null) {
            databaseNames = catalog.listDatabases();
        } else {
            databaseNames = Collections.singletonList(schemaName);
        }

        List<TableId> tableIds = new ArrayList<>();
        for (String databaseName : databaseNames) {
            try {
                for (String tableName : catalog.listTables(databaseName)) {
                    Identifier identifier = Identifier.create(databaseName, tableName);
                    try {
                        Table table = catalog.getTable(identifier);
                        if (!(table instanceof FileStoreTable)) {
                            LOG.info(
                                    "listTables found table {}, but it is a {} instead of a FileStoreTable. Skipping this table.",
                                    identifier,
                                    table.getClass().getSimpleName());
                            continue;
                        }
                    } catch (Catalog.TableNotExistException e) {
                        LOG.warn(
                                "Table {} does not exist. Perhaps it is dropped in the middle of this method. Skipping this table.",
                                identifier,
                                e);
                        continue;
                    }

                    tableIds.add(TableId.tableId(databaseName, tableName));
                }
            } catch (Catalog.DatabaseNotExistException e) {
                LOG.warn(
                        "Database {} does not exist. Perhaps it is dropped in the middle of this method",
                        databaseName,
                        e);
            }
        }
        return tableIds;
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        if (tableId.getNamespace() != null) {
            throw new UnsupportedOperationException("Paimon does not support namespaces");
        }

        try {
            Identifier identifier =
                    Identifier.create(tableId.getSchemaName(), tableId.getTableName());
            Table table = catalog.getTable(identifier);
            if (!(table instanceof FileStoreTable)) {
                throw new RuntimeException(
                        String.format(
                                "Table %s is not a file store table, but a %s, which is not supported by CDC",
                                identifier, table.getClass().getName()));
            }

            return convertPaimonSchemaToFlinkCDCSchema(((FileStoreTable) table).schema());
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }
}
