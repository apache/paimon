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
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.DelayedLookupCompactManager;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
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
    public void testDelayedLookupPartition() throws Exception {
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        Map<String, String> options = new HashMap<>();
        Duration lookupDelayPartitionThreshold = Duration.ofDays(1L);
        options.put(CoreOptions.LOOKUP_DELAY_PARTITION_THRESHOLD.key(), "1 d");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd H");
        options.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$dt $hr");

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

        KeyValueFileStoreWrite write = (KeyValueFileStoreWrite) store.newWrite();
        write.withIOManager(ioManager);
        TestKeyValueGenerator gen = new TestKeyValueGenerator();
        PartitionTimeExtractor partitionTimeExtractor = new PartitionTimeExtractor(store.options());

        for (int i = 0; i < 100; i++) {
            KeyValue keyValue = gen.next();
            BinaryRow partition = gen.getPartition(keyValue);
            LocalDateTime partitionTime =
                    partitionTimeExtractor.extract(
                            partition, TestKeyValueGenerator.DEFAULT_PART_TYPE);
            AbstractFileStoreWrite.WriterContainer<KeyValue> writerContainer =
                    write.createWriterContainer(gen.getPartition(keyValue), 1, false);
            MergeTreeWriter writer = (MergeTreeWriter) writerContainer.writer;

            CompactManager compactManager = writer.compactManager();
            assertThat(compactManager).isInstanceOf(DelayedLookupCompactManager.class);

            DelayedLookupCompactManager delayedLookupCompactManager =
                    (DelayedLookupCompactManager) compactManager;
            assertThat(delayedLookupCompactManager.isDelayedCompactPartition())
                    .isEqualTo(
                            !partitionTime.isAfter(
                                    LocalDateTime.now().minus(lookupDelayPartitionThreshold)));
        }
    }
}
