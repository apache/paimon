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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;

/** Test for {@link SortCompactSinkWrite}. */
public class SortCompactSinkWriteTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testConstructorDoesNotThrow() throws Exception {
        FileStoreTable table = createSequenceOrderingTable();
        try (IOManager ioManager = new IOManagerAsync()) {
            assertThatCode(
                            () ->
                                    new SortCompactSinkWrite(
                                            table,
                                            "user",
                                            new NoopStoreSinkWriteState(0),
                                            ioManager,
                                            true,
                                            false,
                                            false,
                                            new MemoryPoolFactory(
                                                    new HeapMemorySegmentPool(
                                                            16 * 1024 * 1024, 4 * 1024)),
                                            null))
                    .doesNotThrowAnyException();
        }
    }

    private FileStoreTable createSequenceOrderingTable() throws Exception {
        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        FileIO fileIO = FileIOFinder.find(new Path(root));
        SchemaManager schemaManage = new SchemaManager(new LocalFileIO(), path);

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PATH.key(), root);
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.SEQUENCE_SNAPSHOT_ORDERING.key(), "true");
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        schemaManage,
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                TestKeyValueGenerator.getPrimaryKeys(
                                        TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED),
                                options,
                                null));
        return FileStoreTableFactory.create(fileIO, new CoreOptions(options).path(), tableSchema);
    }
}
