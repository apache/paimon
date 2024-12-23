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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.operation.FileStoreTestUtils;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.operation.FileStoreTestUtils.commitData;
import static org.apache.paimon.operation.FileStoreTestUtils.partitionedData;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for TagManager. */
public class TagManagerTest {

    @TempDir java.nio.file.Path tempDir;

    private final FileIO fileIO = new LocalFileIO();

    private long commitIdentifier;
    private String root;
    private TagManager tagManager;

    @BeforeEach
    public void setup() throws Exception {
        commitIdentifier = 0L;
        root = tempDir.toString();
        tagManager = null;
    }

    @Test
    public void testCreateTagWithoutTimeRetained() throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED, 4);
        tagManager = new TagManager(fileIO, store.options().schemaPath());
        SnapshotManager snapshotManager = store.snapshotManager();
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        BinaryRow partition = gen.getPartition(gen.next());

        // commit A to bucket 0 and B to bucket 1
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        for (int bucket : Arrays.asList(0, 1)) {
            List<KeyValue> kvs = partitionedData(5, gen);
            writeData(store, kvs, partition, bucket, writers);
        }
        commitData(store, commitIdentifier++, writers);

        tagManager.createTag(
                snapshotManager.snapshot(1),
                "tag",
                store.options().tagDefaultTimeRetained(),
                Collections.emptyList());
        assertThat(tagManager.tagExists("tag")).isTrue();
        Snapshot snapshot = tagManager.taggedSnapshot("tag");
        String snapshotJson = snapshot.toJson();
        Assertions.assertTrue(
                !snapshotJson.contains("tagCreateTime")
                        && !snapshotJson.contains("tagTimeRetained"));
        Assertions.assertEquals(snapshot, Snapshot.fromJson(snapshotJson));
    }

    @Test
    public void testCreateTagWithTimeRetained() throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED, 4);
        tagManager = new TagManager(fileIO, store.options().schemaPath());
        SnapshotManager snapshotManager = store.snapshotManager();
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        BinaryRow partition = gen.getPartition(gen.next());

        // commit A to bucket 0 and B to bucket 1
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        for (int bucket : Arrays.asList(0, 1)) {
            List<KeyValue> kvs = partitionedData(5, gen);
            writeData(store, kvs, partition, bucket, writers);
        }
        commitData(store, commitIdentifier++, writers);

        tagManager.createTag(
                snapshotManager.snapshot(1), "tag", Duration.ofDays(1), Collections.emptyList());
        assertThat(tagManager.tagExists("tag")).isTrue();
        List<Pair<Tag, String>> tags = tagManager.tagObjects();
        Assertions.assertEquals(1, tags.size());
        Tag tag = tags.get(0).getKey();
        String tagJson = tag.toJson();
        Assertions.assertTrue(
                tagJson.contains("tagCreateTime") && tagJson.contains("tagTimeRetained"));
        Assertions.assertEquals(tag, Tag.fromJson(tagJson));
        assertThat(tags.get(0).getValue()).contains("tag");
    }

    private TestFileStore createStore(TestKeyValueGenerator.GeneratorMode mode, int buckets)
            throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        CoreOptions.ChangelogProducer changelogProducer;
        if (random.nextBoolean()) {
            changelogProducer = CoreOptions.ChangelogProducer.INPUT;
        } else {
            changelogProducer = CoreOptions.ChangelogProducer.NONE;
        }

        RowType rowType, partitionType;
        switch (mode) {
            case NON_PARTITIONED:
                rowType = TestKeyValueGenerator.NON_PARTITIONED_ROW_TYPE;
                partitionType = TestKeyValueGenerator.NON_PARTITIONED_PART_TYPE;
                break;
            case SINGLE_PARTITIONED:
                rowType = TestKeyValueGenerator.SINGLE_PARTITIONED_ROW_TYPE;
                partitionType = TestKeyValueGenerator.SINGLE_PARTITIONED_PART_TYPE;
                break;
            case MULTI_PARTITIONED:
                rowType = TestKeyValueGenerator.DEFAULT_ROW_TYPE;
                partitionType = TestKeyValueGenerator.DEFAULT_PART_TYPE;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported generator mode: " + mode);
        }

        SchemaManager schemaManager = new SchemaManager(fileIO, new Path(root));
        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                rowType.getFields(),
                                partitionType.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(mode),
                                Collections.emptyMap(),
                                null));

        return new TestFileStore.Builder(
                        "avro",
                        root,
                        buckets,
                        partitionType,
                        TestKeyValueGenerator.KEY_TYPE,
                        rowType,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory(),
                        tableSchema)
                .changelogProducer(changelogProducer)
                .build();
    }

    private void writeData(
            TestFileStore store,
            List<KeyValue> kvs,
            BinaryRow partition,
            int bucket,
            Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers)
            throws Exception {
        writers.computeIfAbsent(partition, p -> new HashMap<>())
                .put(bucket, FileStoreTestUtils.writeData(store, kvs, partition, bucket));
    }
}
