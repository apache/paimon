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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A update schema. */
public class UpdateSchema {

    private final RowType rowType;

    private final List<String> partitionKeys;

    private final List<String> primaryKeys;

    private final Map<String, String> options;

    private final String comment;

    public UpdateSchema(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            String comment) {
        this.rowType = validateRowType(rowType, primaryKeys, partitionKeys);
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.options = new HashMap<>(options);
        this.comment = comment;
    }

    public RowType rowType() {
        return rowType;
    }

    public List<String> partitionKeys() {
        return partitionKeys;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public Map<String, String> options() {
        return options;
    }

    public String comment() {
        return comment;
    }

    private RowType validateRowType(
            RowType rowType, List<String> primaryKeys, List<String> partitionKeys) {
        List<String> fieldNames = rowType.getFieldNames();
        Set<String> allFields = new HashSet<>(fieldNames);
        Preconditions.checkState(
                allFields.containsAll(partitionKeys),
                "Table column %s should include all partition fields %s",
                fieldNames,
                partitionKeys);

        if (primaryKeys.isEmpty()) {
            return rowType;
        }
        Preconditions.checkState(
                allFields.containsAll(primaryKeys),
                "Table column %s should include all primary key constraint %s",
                fieldNames,
                primaryKeys);
        Set<String> pkSet = new HashSet<>(primaryKeys);
        Preconditions.checkState(
                pkSet.containsAll(partitionKeys),
                "Primary key constraint %s should include all partition fields %s",
                primaryKeys,
                partitionKeys);

        // primary key should not nullable
        List<DataField> fields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            if (pkSet.contains(field.name()) && field.type().isNullable()) {
                fields.add(
                        new DataField(
                                field.id(),
                                field.name(),
                                field.type().copy(false),
                                field.description()));
            } else {
                fields.add(field);
            }
        }
        return new RowType(false, fields);
    }

    @Override
    public String toString() {
        return "UpdateSchema{"
                + "rowType="
                + rowType
                + ", partitionKeys="
                + partitionKeys
                + ", primaryKeys="
                + primaryKeys
                + ", options="
                + options
                + ", comment="
                + comment
                + '}';
    }
}
