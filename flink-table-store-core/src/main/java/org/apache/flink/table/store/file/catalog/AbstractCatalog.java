/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.catalog;

import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.system.SystemTableLoader;
import org.apache.flink.table.store.utils.StringUtils;

import static org.apache.flink.table.store.table.system.SystemTableLoader.SYSTEM_TABLES;

/** Common implementation of {@link Catalog}. */
public abstract class AbstractCatalog implements Catalog {

    protected static final String DB_SUFFIX = ".db";

    protected final FileIO fileIO;

    protected AbstractCatalog(FileIO fileIO) {
        this.fileIO = fileIO;
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        if (isSystemTable(identifier)) {
            String[] splits = tableAndSystemName(identifier);
            String table = splits[0];
            String type = splits[1];
            Identifier originidentifier = new Identifier(identifier.getDatabaseName(), table);
            if (!tableExists(originidentifier)) {
                throw new TableNotExistException(identifier);
            }
            Path location = getDataTableLocation(originidentifier);
            return SystemTableLoader.load(type, fileIO, location);
        } else {
            TableSchema tableSchema = getDataTableSchema(identifier);
            return FileStoreTableFactory.create(
                    fileIO, getDataTableLocation(identifier), tableSchema);
        }
    }

    @Override
    public boolean tableExists(Identifier identifier) {
        Identifier tableIdentifier = identifier;
        if (isSystemTable(identifier)) {
            String[] splits = tableAndSystemName(identifier);
            if (!SYSTEM_TABLES.contains(splits[1].toLowerCase())) {
                return false;
            }
            tableIdentifier = new Identifier(identifier.getDatabaseName(), splits[0]);
        }
        return dataTableExists(tableIdentifier);
    }

    protected Path databasePath(String database) {
        return new Path(warehouse(), database + DB_SUFFIX);
    }

    protected abstract String warehouse();

    protected abstract TableSchema getDataTableSchema(Identifier identifier)
            throws TableNotExistException;

    protected abstract boolean dataTableExists(Identifier identifier);

    @VisibleForTesting
    public Path getDataTableLocation(Identifier identifier) {
        if (identifier.getObjectName().contains(SYSTEM_TABLE_SPLITTER)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Table name[%s] cannot contain '%s' separator",
                            identifier.getObjectName(), SYSTEM_TABLE_SPLITTER));
        }
        return new Path(databasePath(identifier.getDatabaseName()), identifier.getObjectName());
    }

    private boolean isSystemTable(Identifier identifier) {
        return identifier.getObjectName().contains(SYSTEM_TABLE_SPLITTER);
    }

    private String[] tableAndSystemName(Identifier identifier) {
        String[] splits = StringUtils.split(identifier.getObjectName(), SYSTEM_TABLE_SPLITTER);
        if (splits.length != 2) {
            throw new IllegalArgumentException(
                    "System table can only contain one '$' separator, but this is: "
                            + identifier.getObjectName());
        }
        return splits;
    }
}
