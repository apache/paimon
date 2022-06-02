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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ValueContentRowDataIterator}. */
public class ValueContentRowDataIteratorTest {

    @Test
    public void testIterator() {
        RowData key = GenericRowData.of(1);
        RowData value = GenericRowData.of(10L, 100.0);

        ValueContentRowDataIterator iterator =
                new ValueContentRowDataIterator(
                        new KeyValue().replace(key, 1, ValueKind.ADD, value));
        assertThat(iterator).hasNext();
        assertThat(iterator.next()).isEqualTo(GenericRowData.ofKind(RowKind.INSERT, 10L, 100.0));
        assertThat(iterator).isExhausted();
        assertThat(iterator.next()).isNull();

        iterator =
                new ValueContentRowDataIterator(
                        new KeyValue().replace(key, 1, ValueKind.DELETE, value));
        assertThat(iterator).hasNext();
        assertThat(iterator.next()).isEqualTo(GenericRowData.ofKind(RowKind.DELETE, 10L, 100.0));
        assertThat(iterator).isExhausted();
        assertThat(iterator.next()).isNull();
    }
}
