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
import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.codegen.GeneratedClass;
import org.apache.flink.table.store.codegen.RecordComparator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.function.SerializableSupplier;

import java.util.Comparator;
import java.util.function.Supplier;

/** A {@link Supplier} that returns the comparator for the file store key. */
public class KeyComparatorSupplier implements SerializableSupplier<Comparator<RowData>> {

    private static final long serialVersionUID = 1L;

    private final GeneratedClass<RecordComparator> genRecordComparator;

    public KeyComparatorSupplier(RowType keyType) {
        genRecordComparator =
                CodeGenUtils.generateRecordComparator(keyType.getChildren(), "KeyComparator");
    }

    @Override
    public RecordComparator get() {
        return genRecordComparator.newInstance(KeyComparatorSupplier.class.getClassLoader());
    }
}
