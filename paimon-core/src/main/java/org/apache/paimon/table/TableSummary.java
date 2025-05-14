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

package org.apache.paimon.table;

import org.apache.paimon.TableType;
import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * Summary info of a table, including full name(databaseName.tableName) and table type of this
 * table.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Public
public class TableSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FULL_NAME = "fullName";
    public static final String TABLE_TYPE = "tableType";

    @JsonProperty(FULL_NAME)
    private final String fullName;

    @JsonProperty(TABLE_TYPE)
    private final TableType tableType;

    @JsonCreator
    public TableSummary(
            @JsonProperty(FULL_NAME) String fullName,
            @JsonProperty(TABLE_TYPE) TableType tableType) {
        this.fullName = fullName;
        this.tableType = tableType;
    }

    @JsonGetter(FULL_NAME)
    public String fullName() {
        return fullName;
    }

    @JsonGetter(TABLE_TYPE)
    public TableType tableType() {
        return tableType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSummary that = (TableSummary) o;
        return fullName.equals(that.fullName) && tableType.equals(that.tableType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fullName, tableType);
    }

    @Override
    public String toString() {
        return "{" + "fullName=" + fullName + ", tableType=" + tableType + '}';
    }
}
