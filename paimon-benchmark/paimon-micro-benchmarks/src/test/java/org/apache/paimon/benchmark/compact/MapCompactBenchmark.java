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
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.Map;

/** Benchmark for compacting rows with map. */
public class MapCompactBenchmark {

    @TempDir java.nio.file.Path tempDir;

    private final int rowCount = 4_000_000;

    @Test
    public void testWrite() throws Exception {
        Benchmark benchmark =
                new Benchmark("map-compact-benchmark", rowCount)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        FileSystemCatalog catalog =
                new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        catalog.createDatabase("default", false);
        Identifier identifier = Identifier.create("default", "T");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("k", DataTypes.STRING())
                        .column("v", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("file.format", "avro")
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        benchmark.addCase(
                "write-and-compact",
                1,
                () -> {
                    try (TableWriteImpl<?> write = table.newWrite("test")) {
                        for (int i = 0; i < rowCount; i++) {
                            Map<BinaryString, BinaryString> map = new HashMap<>();
                            for (int j = 0; j < 10; j++) {
                                map.put(
                                        BinaryString.fromString(String.valueOf(i * 10 + j)),
                                        BinaryString.fromString(String.valueOf(i * 100 + j)));
                            }
                            write.write(
                                    GenericRow.of(
                                            BinaryString.fromString(String.valueOf(i)),
                                            new GenericMap(map)));
                        }
                        write.prepareCommit(true, 1);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        benchmark.run();
    }
}
