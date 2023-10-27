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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LookupTable}. */
public class LookupTableTest {

    @TempDir Path tempDir;

    private RocksDBStateFactory stateFactory;

    private RowType rowType;

    @BeforeEach
    public void before() throws IOException {
        this.stateFactory = new RocksDBStateFactory(tempDir.toString(), new Options(), null);
        this.rowType = RowType.of(new IntType(), new IntType(), new IntType());
    }

    @AfterEach
    public void after() throws IOException {
        if (stateFactory != null) {
            stateFactory.close();
        }
    }

    @Test
    public void testPkTable() throws IOException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        singletonList("f0"),
                        singletonList("f0"),
                        r -> r.getInt(0) < 3,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(row(1, 22, 222)).iterator());
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        table.refresh(singletonList(row(RowKind.DELETE, 1, 11, 111)).iterator());
        assertThat(table.get(row(1))).hasSize(0);

        table.refresh(singletonList(row(3, 33, 333)).iterator());
        assertThat(table.get(row(3))).hasSize(0);
    }

    @Test
    public void testPkTableFilter() throws IOException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        singletonList("f0"),
                        singletonList("f0"),
                        r -> r.getInt(1) < 22,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(row(1, 22, 222)).iterator());
        result = table.get(row(1));
        assertThat(result).hasSize(0);
    }

    @Test
    public void testSecKeyTable() throws IOException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        singletonList("f0"),
                        singletonList("f1"),
                        r -> r.getInt(0) < 3,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        List<InternalRow> result = table.get(row(11));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(row(1, 22, 222)).iterator());
        assertThat(table.get(row(11))).hasSize(0);
        result = table.get(row(22));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        table.refresh(singletonList(row(2, 22, 222)).iterator());
        result = table.get(row(22));
        assertThat(result).hasSize(2);
        assertRow(result.get(0), 1, 22, 222);
        assertRow(result.get(1), 2, 22, 222);

        table.refresh(singletonList(row(RowKind.DELETE, 2, 22, 222)).iterator());
        result = table.get(row(22));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        table.refresh(singletonList(row(3, 33, 333)).iterator());
        assertThat(table.get(row(33))).hasSize(0);
    }

    @Test
    public void testNoPrimaryKeyTable() throws IOException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        Collections.emptyList(),
                        singletonList("f1"),
                        r -> r.getInt(2) < 222,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        table.refresh(singletonList(row(1, 11, 333)).iterator());
        List<InternalRow> result = table.get(row(11));
        assertThat(result).hasSize(0);

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        result = table.get(row(11));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        result = table.get(row(11));
        assertThat(result).hasSize(2);
        assertRow(result.get(0), 1, 11, 111);
        assertRow(result.get(1), 1, 11, 111);
    }

    private static InternalRow row(Object... values) {
        return row(RowKind.INSERT, values);
    }

    private static InternalRow row(RowKind kind, Object... values) {
        GenericRow row = new GenericRow(kind, values.length);

        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }

        return row;
    }

    private static void assertRow(InternalRow resultRow, int... expected) {
        int[] results = new int[expected.length];
        for (int i = 0; i < results.length; i++) {
            results[i] = resultRow.getInt(i);
        }
        assertThat(results).containsExactly(expected);
    }
}
