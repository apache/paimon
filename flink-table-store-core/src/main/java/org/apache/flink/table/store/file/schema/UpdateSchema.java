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

import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;

/** A update schema. */
public class UpdateSchema {

    private final RowType rowType;

    private final List<String> partitionKeys;

    private final List<String> primaryKeys;

    private final Map<String, String> options;

    public UpdateSchema(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options) {
        this.rowType = rowType;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.options = options;
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
                + '}';
    }
}
