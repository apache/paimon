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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.system.SystemTableLoader;

import org.apache.commons.lang3.StringUtils;

/** Common implementation of {@link Catalog}. */
public abstract class AbstractCatalog implements Catalog {

    protected static final String DB_SUFFIX = ".db";

    @Override
    public Path getTableLocation(ObjectPath tablePath) {
        if (tablePath.getObjectName().contains(SYSTEM_TABLE_SPLITTER)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Table name[%s] cannot contain '%s' separator",
                            tablePath.getObjectName(), SYSTEM_TABLE_SPLITTER));
        }
        return new Path(databasePath(tablePath.getDatabaseName()), tablePath.getObjectName());
    }

    @Override
    public Table getTable(ObjectPath tablePath) throws TableNotExistException {
        String inputTableName = tablePath.getObjectName();
        if (inputTableName.contains(SYSTEM_TABLE_SPLITTER)) {
            String[] splits = StringUtils.split(inputTableName, SYSTEM_TABLE_SPLITTER);
            if (splits.length != 2) {
                throw new IllegalArgumentException(
                        "System table can only contain one '$' separator, but this is: "
                                + inputTableName);
            }
            String table = splits[0];
            String type = splits[1];
            ObjectPath originTablePath = new ObjectPath(tablePath.getDatabaseName(), table);
            if (!tableExists(originTablePath)) {
                throw new TableNotExistException(tablePath);
            }
            Path location = getTableLocation(originTablePath);
            return SystemTableLoader.load(type, location);
        } else {
            TableSchema tableSchema = getTableSchema(tablePath);
            return FileStoreTableFactory.create(getTableLocation(tablePath), tableSchema);
        }
    }

    protected Path databasePath(String database) {
        return new Path(warehouse(), database + DB_SUFFIX);
    }

    protected abstract String warehouse();
}
