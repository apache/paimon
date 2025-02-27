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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.ForceUpLevel0Compaction;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValueFileStoreWrite}. */
public class KeyValueFileStoreWriteTest {

    private static final int NUM_BUCKETS = 10;

    @TempDir java.nio.file.Path tempDir;

    private IOManager ioManager;

    @BeforeEach
    public void before() throws IOException {
        this.ioManager = new IOManagerImpl(tempDir.toString());
    }

    @Test
    public void testRadicalLookupCompactStrategy() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.LOOKUP_COMPACT.key(), "radical");

        KeyValueFileStoreWrite write = createWriteWithOptions(options);
        write.withIOManager(ioManager);
        TestKeyValueGenerator gen = new TestKeyValueGenerator();

        KeyValue keyValue = gen.next();
        AbstractFileStoreWrite.WriterContainer<KeyValue> writerContainer =
                write.createWriterContainer(gen.getPartition(keyValue), 1, false);
        MergeTreeWriter writer = (MergeTreeWriter) writerContainer.writer;
        try (MergeTreeCompactManager compactManager =
                (MergeTreeCompactManager) writer.compactManager()) {
            CompactStrategy compactStrategy = compactManager.getStrategy();
            assertThat(compactStrategy).isInstanceOf(ForceUpLevel0Compaction.class);
        }
    }

    @Test
    public void testGentleLookupCompactStrategy() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.LOOKUP_COMPACT.key(), "gentle");

        KeyValueFileStoreWrite write = createWriteWithOptions(options);
        write.withIOManager(ioManager);
        TestKeyValueGenerator gen = new TestKeyValueGenerator();

        KeyValue keyValue = gen.next();
        AbstractFileStoreWrite.WriterContainer<KeyValue> writerContainer =
                write.createWriterContainer(gen.getPartition(keyValue), 1, false);
        MergeTreeWriter writer = (MergeTreeWriter) writerContainer.writer;
        try (MergeTreeCompactManager compactManager =
                (MergeTreeCompactManager) writer.compactManager()) {
            CompactStrategy compactStrategy = compactManager.getStrategy();
            assertThat(compactStrategy).isInstanceOf(UniversalCompaction.class);
        }
    }

    private KeyValueFileStoreWrite createWriteWithOptions(Map<String, String> options)
            throws Exception {
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));

        TableSchema schema =
                schemaManager.createTable(
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(
                                        TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                                options,
                                null));
        TestFileStore store =
                new TestFileStore.Builder(
                                "avro",
                                tempDir.toString(),
                                NUM_BUCKETS,
                                TestKeyValueGenerator.DEFAULT_PART_TYPE,
                                TestKeyValueGenerator.KEY_TYPE,
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                                TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                                DeduplicateMergeFunction.factory(),
                                schema)
                        .build();

        return (KeyValueFileStoreWrite) store.newWrite();
    }
}
