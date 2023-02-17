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

package org.apache.flink.table.store.file.catalog;

import org.apache.flink.table.store.annotation.Experimental;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.utils.StringUtils;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.table.store.file.catalog.AbstractCatalog.DB_SUFFIX;
import static org.apache.flink.table.store.utils.Preconditions.checkArgument;

/**
 * Identifies an object in a catalog.
 *
 * @since 0.4.0
 */
@Experimental
public class Identifier implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;

    public Identifier(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public String getDatabaseName() {
        return database;
    }

    public String getObjectName() {
        return table;
    }

    public String getFullName() {
        return String.format("%s.%s", database, table);
    }

    public String getEscapedFullName(char escapeChar) {
        return String.format(
                "%c%s%c.%c%s%c", escapeChar, database, escapeChar, escapeChar, table, escapeChar);
    }

    public static Identifier create(String db, String table) {
        return new Identifier(db, table);
    }

    public static Identifier fromString(String fullName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(fullName), "fullName cannot be null or empty");

        String[] paths = fullName.split("\\.");

        if (paths.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot get splits from '%s' to get database and table", fullName));
        }

        return new Identifier(paths[0], paths[1]);
    }

    public static Identifier fromPath(Path tablePath) {
        return fromPath(tablePath.getPath());
    }

    public static Identifier fromPath(String tablePath) {
        String[] paths = tablePath.split("/");
        if (paths.length < 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Path '%s' is not a legacy path, please use catalog table path instead: 'warehouse_path/your_database.db/your_table'.",
                            tablePath));
        }

        String database = paths[paths.length - 2];
        database = database.substring(0, database.length() - DB_SUFFIX.length());

        return new Identifier(database, paths[paths.length - 1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Identifier that = (Identifier) o;
        return Objects.equals(database, that.database) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table);
    }

    @Override
    public String toString() {
        return "Identifier{" + "database='" + database + '\'' + ", table='" + table + '\'' + '}';
    }
}
