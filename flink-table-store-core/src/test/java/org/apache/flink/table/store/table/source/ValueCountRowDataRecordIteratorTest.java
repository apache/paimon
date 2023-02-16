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

import org.apache.flink.table.store.file.utils.ReusingTestData;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.utils.ProjectedRow;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ValueCountRowDataRecordIterator}. */
public class ValueCountRowDataRecordIteratorTest extends RowDataRecordIteratorTestBase {

    @Test
    public void testIteratorWithoutProjection() throws Exception {
        List<ReusingTestData> input =
                ReusingTestData.parse("1, 1, +, 3 | 2, 2, +, 1 | 1, 3, +, -2 | 2, 4, +, -1");
        List<Integer> expectedValues = Arrays.asList(1, 1, 1, 2, 1, 1, 2);
        List<RowKind> expectedRowKinds =
                Arrays.asList(
                        RowKind.INSERT,
                        RowKind.INSERT,
                        RowKind.INSERT,
                        RowKind.INSERT,
                        RowKind.DELETE,
                        RowKind.DELETE,
                        RowKind.DELETE);

        testIterator(
                input,
                ValueCountRowDataRecordIterator::new,
                (rowData, idx) -> {
                    assertThat(rowData.getFieldCount()).isEqualTo(1);
                    assertThat(rowData.getInt(0)).isEqualTo(expectedValues.get(idx));
                    assertThat(rowData.getRowKind()).isEqualTo(expectedRowKinds.get(idx));
                });
    }

    @Test
    public void testIteratorWithProjection() throws Exception {
        List<ReusingTestData> input =
                ReusingTestData.parse("1, 1, +, 3 | 2, 2, +, 1 | 1, 3, +, -2 | 2, 4, +, -1");
        List<Integer> expectedValues = Arrays.asList(1, 1, 1, 2, 1, 1, 2);
        List<RowKind> expectedRowKinds =
                Arrays.asList(
                        RowKind.INSERT,
                        RowKind.INSERT,
                        RowKind.INSERT,
                        RowKind.INSERT,
                        RowKind.DELETE,
                        RowKind.DELETE,
                        RowKind.DELETE);

        testIterator(
                input,
                kvIterator ->
                        new ValueCountRowDataRecordIterator(
                                kvIterator.transform(
                                        kv ->
                                                kv.replaceKey(
                                                        ProjectedRow.from(
                                                                        new int[][] {
                                                                            new int[] {0},
                                                                            new int[] {0}
                                                                        })
                                                                .replaceRow(kv.key())))),
                (rowData, idx) -> {
                    assertThat(rowData.getFieldCount()).isEqualTo(2);
                    assertThat(rowData.getInt(0)).isEqualTo(expectedValues.get(idx));
                    assertThat(rowData.getInt(1)).isEqualTo(expectedValues.get(idx));
                    assertThat(rowData.getRowKind()).isEqualTo(expectedRowKinds.get(idx));
                });
    }
}
