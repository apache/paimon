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

package org.apache.paimon.benchmark.compact;

import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Benchmark for compacting rows with array. */
public class ArrayCompactBenchmark {

    @TempDir java.nio.file.Path tempDir;

    private final int rowCount = 4_000_000;
    private final int numberArraySize = 100;
    private final int stringArraySize = 20;

    @Test
    public void testArrayInt() throws Exception {
        runBenchmark("array-int", DataTypes.INT(), this::intArray);
    }

    @Test
    public void testArrayBigInt() throws Exception {
        runBenchmark("array-bigint", DataTypes.BIGINT(), this::bigIntArray);
    }

    @Test
    public void testArrayString() throws Exception {
        runBenchmark("array-string", DataTypes.STRING(), this::stringArray);
    }

    private void runBenchmark(String name, DataType elementType, ArrayFactory arrayFactory)
            throws Exception {
        Benchmark benchmark =
                new Benchmark("avro-" + name + "-compact-benchmark", rowCount)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        FileStoreTable table = createTable(elementType);
        benchmark.addCase(
                "write-and-compact",
                1,
                () -> {
                    try {
                        writeAndCompact(table, arrayFactory);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        benchmark.run();
    }

    private FileStoreTable createTable(DataType elementType) throws Exception {
        FileSystemCatalog catalog =
                new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        catalog.createDatabase("default", false);
        Identifier identifier = Identifier.create("default", "T");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("k", DataTypes.STRING())
                        .column("v", DataTypes.ARRAY(elementType))
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("file.format", "avro")
                        .build(),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private void writeAndCompact(FileStoreTable table, ArrayFactory arrayFactory) {
        try (TableWriteImpl<?> write = table.newWrite("test")) {
            for (int i = 0; i < rowCount; i++) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(String.valueOf(i)),
                                arrayFactory.create(i)));
            }
            write.prepareCommit(true, 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private GenericArray intArray(int rowId) {
        Integer[] array = new Integer[numberArraySize];
        for (int i = 0; i < numberArraySize; i++) {
            array[i] = rowId * numberArraySize + i;
        }
        return new GenericArray(array);
    }

    private GenericArray bigIntArray(int rowId) {
        Long[] array = new Long[numberArraySize];
        for (int i = 0; i < numberArraySize; i++) {
            array[i] = (long) rowId * numberArraySize + i;
        }
        return new GenericArray(array);
    }

    private GenericArray stringArray(int rowId) {
        BinaryString[] array = new BinaryString[stringArraySize];
        for (int i = 0; i < stringArraySize; i++) {
            array[i] = BinaryString.fromString(String.valueOf(rowId * stringArraySize + i));
        }
        return new GenericArray(array);
    }

    private interface ArrayFactory {
        GenericArray create(int rowId);
    }
}
