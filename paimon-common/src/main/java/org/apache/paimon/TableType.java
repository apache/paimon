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

package org.apache.paimon;

import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import static org.apache.paimon.options.description.TextElement.text;

/** Type of the table. */
public enum TableType implements DescribedEnum {
    TABLE("table", "Normal Paimon table."),
    FORMAT_TABLE(
            "format-table",
            "A file format table refers to a directory that contains multiple files of the same format."),
    FLINK_MATERIALIZED_TABLE("flink-materialized-table", "A Flink materialized table.");
    private final String value;
    private final String description;

    TableType(String value, String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public InlineElement getDescription() {
        return text(description);
    }

    public static TableType fromString(String name) {
        for (TableType type : TableType.values()) {
            if (type.value.equals(name)) {
                return type;
            }
        }
        throw new UnsupportedOperationException("Unknown table type: " + name);
    }
}
