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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PrimaryKeyRowDataSupplier}. */
public class PrimaryKeyRowDataSupplierTest {

    @Test
    public void testSupplier() {
        List<KeyValue> kvs =
                Arrays.asList(
                        new KeyValue()
                                .replace(
                                        GenericRowData.of(1, 10L),
                                        1,
                                        ValueKind.ADD,
                                        GenericRowData.of(1, 10L, "Hi")),
                        new KeyValue()
                                .replace(
                                        GenericRowData.of(2, 20L),
                                        2,
                                        ValueKind.ADD,
                                        GenericRowData.of(2, 20L, "Hello")),
                        new KeyValue()
                                .replace(
                                        GenericRowData.of(3, 30L),
                                        3,
                                        ValueKind.DELETE,
                                        GenericRowData.of(3, 30L, "Test")));
        Iterator<KeyValue> iterator = kvs.iterator();
        PrimaryKeyRowDataSupplier supplier =
                new PrimaryKeyRowDataSupplier(() -> iterator.hasNext() ? iterator.next() : null);

        assertThat(supplier.get()).isEqualTo(GenericRowData.of(1, 10L, "Hi"));
        assertThat(supplier.get()).isEqualTo(GenericRowData.of(2, 20L, "Hello"));
        GenericRowData deleted = GenericRowData.of(3, 30L, "Test");
        deleted.setRowKind(RowKind.DELETE);
        assertThat(supplier.get()).isEqualTo(deleted);
        assertThat(supplier.get()).isNull();
    }

    @Test
    public void testEmpty() {
        PrimaryKeyRowDataSupplier supplier = new PrimaryKeyRowDataSupplier(() -> null);
        assertThat(supplier.get()).isNull();
    }
}
