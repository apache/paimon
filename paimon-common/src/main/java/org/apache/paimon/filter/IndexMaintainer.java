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

package org.apache.paimon.filter;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalRow;

import java.util.function.BiFunction;

/** One index maintainer for one column. */
public class IndexMaintainer {

    private final String columnName;
    private final int columnIndex;
    private final FilterInterface filter;
    private final BiFunction<DataGetters, Integer, byte[]> getter;

    public IndexMaintainer(
            String columnName,
            int columnIndex,
            FilterInterface filter,
            BiFunction<DataGetters, Integer, byte[]> getter) {
        this.columnName = columnName;
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.getter = getter;
    }

    public void write(InternalRow row) {
        filter.add(getter.apply(row, columnIndex));
    }

    public String getColumnName() {
        return columnName;
    }

    public byte[] serializedBytes() {
        return filter.serializedBytes();
    }
}
