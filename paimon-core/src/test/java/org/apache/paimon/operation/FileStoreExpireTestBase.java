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
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link FileStoreExpireImpl}. */
public class FileStoreExpireTestBase {

    protected final FileIO fileIO = new LocalFileIO();
    protected TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;
    protected TestFileStore store;
    protected SnapshotManager snapshotManager;

    @BeforeEach
    public void beforeEach() throws Exception {
        gen = new TestKeyValueGenerator();
        store = createStore();
        snapshotManager = store.snapshotManager();
        SchemaManager schemaManager = new SchemaManager(fileIO, new Path(tempDir.toUri()));
        schemaManager.createTable(
                new Schema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        Collections.emptyMap(),
                        null));
    }

    private TestFileStore createStore() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        CoreOptions.ChangelogProducer changelogProducer;
        if (random.nextBoolean()) {
            changelogProducer = CoreOptions.ChangelogProducer.INPUT;
        } else {
            changelogProducer = CoreOptions.ChangelogProducer.NONE;
        }

        return new TestFileStore.Builder(
                        "avro",
                        tempDir.toString(),
                        1,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory())
                .changelogProducer(changelogProducer)
                .build();
    }

    protected void commit(int numCommits, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        for (int i = 0; i < numCommits; i++) {
            int numRecords = ThreadLocalRandom.current().nextInt(100) + 1;
            List<KeyValue> data = new ArrayList<>();
            for (int j = 0; j < numRecords; j++) {
                data.add(gen.next());
            }
            allData.addAll(data);
            List<Snapshot> snapshots = store.commitData(data, gen::getPartition, kv -> 0);
            for (int j = 0; j < snapshots.size(); j++) {
                snapshotPositions.add(allData.size());
            }
        }
    }

    protected void assertSnapshot(
            int snapshotId, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        assertSnapshot(snapshotManager.snapshot(snapshotId), allData, snapshotPositions);
    }

    protected void assertSnapshot(
            Snapshot snapshot, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        int snapshotId = (int) snapshot.id();
        Map<BinaryRow, BinaryRow> expected =
                store.toKvMap(allData.subList(0, snapshotPositions.get(snapshotId - 1)));
        List<KeyValue> actualKvs =
                store.readKvsFromManifestEntries(
                        store.newScan().withSnapshot(snapshot).plan().files(), false);
        gen.sort(actualKvs);
        Map<BinaryRow, BinaryRow> actual = store.toKvMap(actualKvs);
        assertThat(actual).isEqualTo(expected);
    }
}
