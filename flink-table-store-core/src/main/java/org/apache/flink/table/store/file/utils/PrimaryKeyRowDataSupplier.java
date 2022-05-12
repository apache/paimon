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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.types.RowKind;

import java.util.function.Supplier;

/**
 * Reads in a {@link Supplier} of {@link KeyValue}, where the key represents the primary key and the
 * value represents the whole row, and gives out the corresponding {@link RowData}.
 *
 * <p>NOTE: The provided {@link Supplier} must return null when there is nothing to provide.
 */
public class PrimaryKeyRowDataSupplier implements Supplier<RowData> {

    private final Supplier<KeyValue> kvSupplier;

    public PrimaryKeyRowDataSupplier(Supplier<KeyValue> kvSupplier) {
        this.kvSupplier = kvSupplier;
    }

    @Override
    public RowData get() {
        KeyValue kv = kvSupplier.get();
        if (kv == null) {
            return null;
        }
        RowData row = kv.value();
        if (kv.valueKind() == ValueKind.DELETE) {
            row.setRowKind(RowKind.DELETE);
        }
        return row;
    }
}
