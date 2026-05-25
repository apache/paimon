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

package org.apache.paimon.cli.sql;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Maps a Paimon database to a Calcite schema. Tables are resolved lazily on first access. */
public class PaimonSchema extends AbstractSchema {

    private final Catalog catalog;
    private final String database;
    private final ConcurrentHashMap<String, Table> tableCache = new ConcurrentHashMap<>();
    private volatile List<String> tableNames;

    public PaimonSchema(Catalog catalog, String database) {
        this.catalog = catalog;
        this.database = database;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return new LazyTableMap();
    }

    private List<String> tableNameList() {
        if (tableNames == null) {
            try {
                tableNames = catalog.listTables(database);
            } catch (Exception e) {
                throw new RuntimeException("Failed to list tables in database: " + database, e);
            }
        }
        return tableNames;
    }

    private Table resolveTable(String name) {
        return tableCache.computeIfAbsent(
                name,
                k -> {
                    try {
                        org.apache.paimon.table.Table paimonTable =
                                catalog.getTable(Identifier.create(database, k));
                        return new PaimonCalciteTable(paimonTable);
                    } catch (Exception e) {
                        return null;
                    }
                });
    }

    private class LazyTableMap extends AbstractMap<String, Table> {

        @Override
        public Table get(Object key) {
            if (!(key instanceof String)) {
                return null;
            }
            return resolveTable((String) key);
        }

        @Override
        public boolean containsKey(Object key) {
            if (!(key instanceof String)) {
                return false;
            }
            return tableNameList().contains(key);
        }

        @Override
        public Set<String> keySet() {
            return new HashSet<>(tableNameList());
        }

        @Override
        public int size() {
            return tableNameList().size();
        }

        @Override
        public Set<Entry<String, Table>> entrySet() {
            Set<Entry<String, Table>> entries = new HashSet<>();
            for (String name : tableNameList()) {
                Table t = resolveTable(name);
                if (t != null) {
                    entries.add(new SimpleEntry<>(name, t));
                }
            }
            return entries;
        }
    }
}
