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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Identifies an object in a catalog.
 *
 * @since 0.4.0
 */
@Public
@JsonIgnoreProperties(ignoreUnknown = true)
public class Identifier implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_DATABASE_NAME = "database";
    private static final String FIELD_TABLE_NAME = "table";
    private static final String FIELD_BRANCH_NAME = "branch";

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_DATABASE", DataTypes.STRING()),
                            new DataField(1, "_OBJECT", DataTypes.STRING())));

    public static final String UNKNOWN_DATABASE = "unknown";

    @JsonProperty(FIELD_DATABASE_NAME)
    private final String database;

    private final String object;

    @JsonProperty(FIELD_TABLE_NAME)
    private transient String table;

    @JsonProperty(FIELD_BRANCH_NAME)
    private transient String branch;

    private transient String systemTable;

    public Identifier(String database, String object) {
        this.database = database;
        this.object = object;
    }

    @JsonCreator
    public Identifier(
            @JsonProperty(FIELD_DATABASE_NAME) String database,
            @JsonProperty(FIELD_TABLE_NAME) String table,
            @JsonProperty(FIELD_BRANCH_NAME) @Nullable String branch) {
        this(database, table, branch, null);
    }

    public Identifier(
            String database, String table, @Nullable String branch, @Nullable String systemTable) {
        this.database = database;

        StringBuilder builder = new StringBuilder(table);
        if (branch != null) {
            builder.append(Catalog.SYSTEM_TABLE_SPLITTER)
                    .append(Catalog.SYSTEM_BRANCH_PREFIX)
                    .append(branch);
        }
        if (systemTable != null) {
            builder.append(Catalog.SYSTEM_TABLE_SPLITTER).append(systemTable);
        }
        this.object = builder.toString();

        this.table = table;
        this.branch = branch;
        this.systemTable = systemTable;
    }

    @JsonGetter(FIELD_DATABASE_NAME)
    public String getDatabaseName() {
        return database;
    }

    @JsonIgnore
    public String getObjectName() {
        return object;
    }

    @JsonIgnore
    public String getFullName() {
        return UNKNOWN_DATABASE.equals(this.database)
                ? object
                : String.format("%s.%s", database, object);
    }

    @JsonGetter(FIELD_TABLE_NAME)
    public String getTableName() {
        splitObjectName();
        return table;
    }

    @JsonGetter(FIELD_BRANCH_NAME)
    public @Nullable String getBranchName() {
        splitObjectName();
        return branch;
    }

    @JsonIgnore
    public String getBranchNameOrDefault() {
        String branch = getBranchName();
        return branch == null ? BranchManager.DEFAULT_MAIN_BRANCH : branch;
    }

    @JsonIgnore
    public @Nullable String getSystemTableName() {
        splitObjectName();
        return systemTable;
    }

    @JsonIgnore
    public boolean isSystemTable() {
        return getSystemTableName() != null;
    }

    private void splitObjectName() {
        if (table != null) {
            return;
        }

        String[] splits = StringUtils.split(object, Catalog.SYSTEM_TABLE_SPLITTER, -1, true);
        if (splits.length == 1) {
            table = object;
            branch = null;
            systemTable = null;
        } else if (splits.length == 2) {
            table = splits[0];
            if (splits[1].startsWith(Catalog.SYSTEM_BRANCH_PREFIX)) {
                branch = splits[1].substring(Catalog.SYSTEM_BRANCH_PREFIX.length());
                systemTable = null;
            } else {
                branch = null;
                systemTable = splits[1];
            }
        } else if (splits.length == 3) {
            Preconditions.checkArgument(
                    splits[1].startsWith(Catalog.SYSTEM_BRANCH_PREFIX),
                    "System table can only contain one '$' separator, but this is: " + object);
            table = splits[0];
            branch = splits[1].substring(Catalog.SYSTEM_BRANCH_PREFIX.length());
            systemTable = splits[2];
        } else {
            throw new IllegalArgumentException("Invalid object name: " + object);
        }
    }

    @JsonIgnore
    public String getEscapedFullName() {
        return getEscapedFullName('`');
    }

    @JsonIgnore
    public String getEscapedFullName(char escapeChar) {
        return String.format(
                "%c%s%c.%c%s%c", escapeChar, database, escapeChar, escapeChar, object, escapeChar);
    }

    public static Identifier create(String db, String object) {
        return new Identifier(db, object);
    }

    public static Identifier fromString(String fullName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(fullName), "fullName cannot be null or empty");

        String[] paths = fullName.split("\\.");

        if (paths.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot get splits from '%s' to get database and object", fullName));
        }

        return new Identifier(paths[0], paths[1]);
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
        return Objects.equals(database, that.database) && Objects.equals(object, that.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, object);
    }

    @Override
    public String toString() {
        return String.format("Identifier{database='%s', object='%s'}", database, object);
    }
}
