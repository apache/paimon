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

package org.apache.paimon.flink.action.cdc.mysql.schema;

import org.apache.paimon.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** Describe a MySQL table column. */
public class MySqlColumn {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlColumn.class);

    private final String name;
    private final DataType type;
    @Nullable private final String defaultValue;
    @Nullable private final String comment;

    public MySqlColumn(
            String name, DataType type, @Nullable String defaultValue, @Nullable String comment) {
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
        this.comment = comment;
    }

    public String name() {
        return name;
    }

    public DataType type() {
        return type;
    }

    @Nullable
    public String defaultValue() {
        return defaultValue;
    }

    @Nullable
    public String comment() {
        return comment;
    }

    public MySqlColumn copy() {
        return new MySqlColumn(name, type, defaultValue, comment);
    }

    public MySqlColumn toLowerCaseCopy() {
        return new MySqlColumn(name.toLowerCase(), type, defaultValue, comment);
    }

    public MySqlColumn merge(MySqlColumn other) {
        String defaultValue = null;
        if (this.defaultValue != null && other.defaultValue != null) {
            if (this.defaultValue.equals(other.defaultValue)) {
                defaultValue = this.defaultValue;
            } else {
                LOG.info(
                        "Found different default value definitions when merging column: '{}', '{}'. Default value will be ignored.",
                        this.defaultValue,
                        other.defaultValue);
            }
        } else if (this.defaultValue != null) {
            defaultValue = this.defaultValue;
        } else {
            defaultValue = other.defaultValue;
        }

        return new MySqlColumn(name, other.type, defaultValue, comment);
    }
}
