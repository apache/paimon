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

package org.apache.paimon.sort;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.MutableObjectIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/** Tests for {@link ExternalRowSortBuffer}. */
public class ExternalRowSortBufferTest {

    private static final Random RANDOM = new Random();

    @TempDir private Path dir;

    @Test
    public void testFunctionRandom() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BIGINT())
                        .build();
        IOManager ioManager = new IOManagerImpl(dir.toString());
        CoreOptions coreOptions = new CoreOptions(new Options());

        ExternalRowSortBuffer externalRowSortBuffer =
                ExternalRowSortBuffer.create(
                        ioManager,
                        schema.rowType(),
                        new int[] {0, 2},
                        MemorySize.parse("10 mb").getBytes(),
                        coreOptions.pageSize(),
                        coreOptions.localSortMaxNumFileHandles());

        List<BinaryRow> datas = data(Math.abs(RANDOM.nextInt(1000)) + 1000);
        for (BinaryRow data : datas) {
            externalRowSortBuffer.write(data);
        }

        TreeSet<BinaryRow> treeSet =
                new TreeSet<>(
                        Comparator.comparingLong((BinaryRow o) -> o.getLong(0))
                                .thenComparingLong(o -> o.getLong(2)));
        treeSet.addAll(datas);
        List<BinaryRow> sorted = new ArrayList<>();
        MutableObjectIterator<BinaryRow> iterator = externalRowSortBuffer.sortedIterator();
        BinaryRow row;
        while ((row = iterator.next()) != null) {
            sorted.add(row.copy());
        }

        Iterator<BinaryRow> treeIterator = treeSet.iterator();
        for (BinaryRow sortRow : sorted) {
            long f0 = sortRow.getLong(0);
            BinaryString f1 = sortRow.getString(1);
            long f2 = sortRow.getLong(2);

            BinaryRow treeRow = treeIterator.next();
            long t0 = treeRow.getLong(0);
            BinaryString t1 = treeRow.getString(1);
            long t2 = treeRow.getLong(2);

            Assertions.assertThat(f0).isEqualTo(t0);
            Assertions.assertThat(f1).isEqualTo(t1);
            Assertions.assertThat(f2).isEqualTo(t2);
        }
    }

    private List<BinaryRow> data(int size) {
        List<BinaryRow> datas = new ArrayList<>();
        BinaryRow binaryRow = new BinaryRow(3);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        byte[] bytes = new byte[1024];

        for (int i = 0; i < size; i++) {
            writer.writeLong(0, RANDOM.nextLong());
            RANDOM.nextBytes(bytes);
            writer.writeString(1, BinaryString.fromBytes(bytes));
            writer.writeLong(2, RANDOM.nextLong());
            writer.complete();
            datas.add(binaryRow.copy());
        }

        return datas;
    }
}
