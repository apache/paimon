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

package org.apache.paimon.table.source;

import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.ReusingTestData;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ValueContentRowDataRecordIterator}. */
public class ValueContentRowDataRecordIteratorTest extends RowDataRecordIteratorTestBase {

    @Test
    public void testIterator() throws Exception {
        List<ReusingTestData> input =
                ReusingTestData.parse("1, 1, +, 100 | 2, 2, +, 200 | 1, 3, -, 100 | 2, 4, +, 300");
        List<Long> expectedValues = Arrays.asList(100L, 200L, 100L, 300L);
        List<RowKind> expectedRowKinds =
                Arrays.asList(RowKind.INSERT, RowKind.INSERT, RowKind.DELETE, RowKind.INSERT);

        testIterator(
                input,
                ValueContentRowDataRecordIterator::new,
                (rowData, idx) -> {
                    assertThat(rowData.getFieldCount()).isEqualTo(1);
                    assertThat(rowData.getLong(0)).isEqualTo(expectedValues.get(idx));
                    assertThat(rowData.getRowKind()).isEqualTo(expectedRowKinds.get(idx));
                });
    }
}
