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

package org.apache.flink.table.store.table;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ValueCountRowDataIterator}. */
public class ValueCountRowDataIteratorTest {

    @Test
    public void testWithoutProjection() {
        RowData key = GenericRowData.of(1, 10L);

        ValueCountRowDataIterator iterator =
                new ValueCountRowDataIterator(
                        new KeyValue().replace(key, 1, ValueKind.ADD, GenericRowData.of(3L)), null);
        for (int i = 0; i < 3; i++) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next()).isEqualTo(GenericRowData.ofKind(RowKind.INSERT, 1, 10L));
        }
        assertThat(iterator).isExhausted();
        assertThat(iterator.next()).isNull();

        iterator =
                new ValueCountRowDataIterator(
                        new KeyValue().replace(key, 1, ValueKind.ADD, GenericRowData.of(-3L)),
                        null);
        for (int i = 0; i < 3; i++) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next()).isEqualTo(GenericRowData.ofKind(RowKind.DELETE, 1, 10L));
        }
        assertThat(iterator).isExhausted();
        assertThat(iterator.next()).isNull();
    }

    @Test
    public void testWithProjection() {
        RowData key = GenericRowData.of(1, 10L, 100.0);

        ValueCountRowDataIterator iterator =
                new ValueCountRowDataIterator(
                        new KeyValue().replace(key, 1, ValueKind.ADD, GenericRowData.of(3L)),
                        new int[][] {new int[] {1}, new int[] {0}});
        for (int i = 0; i < 3; i++) {
            assertThat(iterator).hasNext();
            RowData row = iterator.next();
            assertThat(row.getArity()).isEqualTo(2);
            assertThat(row.getLong(0)).isEqualTo(10L);
            assertThat(row.getInt(1)).isEqualTo(1);
        }
        assertThat(iterator).isExhausted();
        assertThat(iterator.next()).isNull();
    }
}
