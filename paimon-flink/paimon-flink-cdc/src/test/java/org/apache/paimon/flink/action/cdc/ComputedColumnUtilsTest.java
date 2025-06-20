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

package org.apache.paimon.flink.action.cdc;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.sortComputedColumns;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for ComputedColumnUtils. */
public class ComputedColumnUtilsTest {
    @Test
    public void test() {
        List<ComputedColumn> columns =
                Arrays.asList(
                        new ComputedColumn("A", Expression.substring("B", "1")),
                        new ComputedColumn("B", Expression.substring("ExistedColumn", "1")),
                        new ComputedColumn("C", Expression.cast("No Reference")),
                        new ComputedColumn("D", Expression.substring("A", "1")),
                        new ComputedColumn("E", Expression.substring("C", "1")));

        List<ComputedColumn> sortedColumns = sortComputedColumns(columns);
        assertEquals(
                Arrays.asList("B", "C", "E", "A", "D"),
                sortedColumns.stream()
                        .map(ComputedColumn::columnName)
                        .collect(Collectors.toList()));
    }

    @Test
    public void testCycleReference() {
        List<ComputedColumn> columns =
                Arrays.asList(
                        new ComputedColumn("A", Expression.substring("B", "1")),
                        new ComputedColumn("B", Expression.substring("C", "1")),
                        new ComputedColumn("C", Expression.substring("A", "1")));

        assertThrows(IllegalArgumentException.class, () -> sortComputedColumns(columns));
    }
}
