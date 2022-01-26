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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.stats.FieldStats;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.mergetree.compact.CompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileStoreSourceSplitGenerator}. */
public class FileStoreSourceSplitGeneratorTest {

    @Test
    public void test() {
        FileStoreScan.Plan plan =
                new FileStoreScan.Plan() {
                    @Nullable
                    @Override
                    public Long snapshotId() {
                        return null;
                    }

                    @Override
                    public List<ManifestEntry> files() {
                        return Arrays.asList(
                                makeEntry(1, 0, "f0"),
                                makeEntry(1, 0, "f1"),
                                makeEntry(1, 1, "f2"),
                                makeEntry(2, 0, "f3"),
                                makeEntry(2, 0, "f4"),
                                makeEntry(2, 0, "f5"),
                                makeEntry(2, 1, "f6"));
                    }
                };
        List<FileStoreSourceSplit> splits = new FileStoreSourceSplitGenerator().createSplits(plan);
        assertThat(splits.size()).isEqualTo(4);
        assertSplit(splits.get(0), "0000000001", 2, 0, Arrays.asList("f3", "f4", "f5"));
        assertSplit(splits.get(1), "0000000002", 2, 1, Collections.singletonList("f6"));
        assertSplit(splits.get(2), "0000000003", 1, 0, Arrays.asList("f0", "f1"));
        assertSplit(splits.get(3), "0000000004", 1, 1, Collections.singletonList("f2"));
    }

    private void assertSplit(
            FileStoreSourceSplit split, String splitId, int part, int bucket, List<String> files) {
        assertThat(split.splitId()).isEqualTo(splitId);
        assertThat(split.partition().getInt(0)).isEqualTo(part);
        assertThat(split.bucket()).isEqualTo(bucket);
        assertThat(split.files().stream().map(SstFileMeta::fileName).collect(Collectors.toList()))
                .isEqualTo(files);
    }

    private ManifestEntry makeEntry(int partition, int bucket, String fileName) {
        return new ManifestEntry(
                ValueKind.ADD,
                row(partition), // not used
                bucket, // not used
                0, // not used
                new SstFileMeta(
                        fileName,
                        0, // not used
                        0, // not used
                        null, // not used
                        null, // not used
                        new FieldStats[] {new FieldStats(null, null, 0)}, // not used
                        0, // not used
                        0, // not used
                        0 // not used
                        ));
    }
}
