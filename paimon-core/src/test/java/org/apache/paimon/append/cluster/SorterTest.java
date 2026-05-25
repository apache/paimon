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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.append.cluster.IncrementalClusterManagerTest.writeOnce;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Sorter}. */
public class SorterTest {
    @TempDir java.nio.file.Path tableTempDir;

    @TempDir java.nio.file.Path ioTempDir;

    @ParameterizedTest
    @ValueSource(strings = {"hilbert", "zorder", "order"})
    public void testSorter(String curve) throws Exception {
        innerTest(curve);
    }

    private void innerTest(String curve) throws Exception {
        FileStoreTable table = createTable(new HashMap<>(), curve);
        writeOnce(
                table,
                GenericRow.of(2, 0, BinaryString.fromString("test")),
                GenericRow.of(2, 1, BinaryString.fromString("test")),
                GenericRow.of(2, 2, BinaryString.fromString("test")),
                GenericRow.of(0, 0, BinaryString.fromString("test")),
                GenericRow.of(0, 1, BinaryString.fromString("test")),
                GenericRow.of(0, 2, BinaryString.fromString("test")),
                GenericRow.of(1, 0, BinaryString.fromString("test")),
                GenericRow.of(1, 1, BinaryString.fromString("test")),
                GenericRow.of(1, 2, BinaryString.fromString("test")));
        IOManager ioManager = IOManager.create(ioTempDir.toString());
        ReadBuilder readBuilder = table.newReadBuilder();
        Sorter sorter =
                Sorter.getSorter(
                        new RecordReaderIterator<>(
                                readBuilder.newRead().createReader(readBuilder.newScan().plan())),
                        ioManager,
                        table.rowType(),
                        table.coreOptions());
        List<String> result = new ArrayList<>();
        MutableObjectIterator<BinaryRow> sorted = sorter.sort();
        BinaryRow binaryRow = new BinaryRow(sorter.arity());
        while ((binaryRow = sorted.next(binaryRow)) != null) {
            InternalRow rowRemovedKey = sorter.removeSortKey(binaryRow);
            result.add(String.format("%s,%s", rowRemovedKey.getInt(0), rowRemovedKey.getInt(1)));
        }
        verify(curve, result);
    }

    private void verify(String curve, List<String> result) {
        switch (curve) {
            case "hilbert":
                assertThat(result)
                        .containsExactly(
                                "0,0", "0,1", "1,1", "1,0", "2,0", "2,1", "2,2", "1,2", "0,2");
                break;
            case "zorder":
                assertThat(result)
                        .containsExactly(
                                "0,0", "0,1", "1,0", "1,1", "0,2", "1,2", "2,0", "2,1", "2,2");
                break;
            case "order":
                assertThat(result)
                        .containsExactly(
                                "0,0", "0,1", "0,2", "1,0", "1,1", "1,2", "2,0", "2,1", "2,2");
                break;
        }
    }

    protected FileStoreTable createTable(Map<String, String> customOptions, String clusterCurve)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CLUSTERING_COLUMNS.key(), "f0,f1");
        options.put(CoreOptions.CLUSTERING_STRATEGY.key(), clusterCurve);
        options.putAll(customOptions);

        Schema schema =
                new Schema(
                        RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.STRING())
                                .getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options,
                        "");

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tableTempDir.toString()));
        return FileStoreTableFactory.create(
                LocalFileIO.create(),
                new Path(tableTempDir.toString()),
                schemaManager.createTable(schema));
    }
}
