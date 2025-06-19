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

package org.apache.paimon.flink.sink.listener;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.sink.listener.ListenerTestUtils.createMockContext;
import static org.assertj.core.api.Assertions.assertThat;

/** Test custom {@link CommitListener}. */
public class CustomCommitListenerTest {

    @TempDir java.nio.file.Path tempDir;
    private static final Map<String, Set<String>> commitListenerResult = new ConcurrentHashMap<>();

    @Test
    public void testCustomCommitListener() throws Exception {
        Path tablePath = new Path(tempDir.toString());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        String testId = UUID.randomUUID().toString();
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("pt", DataTypes.STRING())
                        .partitionKeys("pt")
                        .option(
                                FlinkConnectorOptions.COMMIT_CUSTOM_LISTENERS.key(),
                                "partition-collector")
                        .option("test-listener-id", testId)
                        .build();
        schemaManager.createTable(schema);

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        String commitUser = UUID.randomUUID().toString();

        StreamTableWrite write = table.newWrite(commitUser);
        write.write(GenericRow.of(1, BinaryString.fromString("20250101")));
        write.write(GenericRow.of(1, BinaryString.fromString("20250102")));
        List<CommitMessage> commitMessages = write.prepareCommit(false, 1);
        write.close();

        StoreCommitter committer =
                new StoreCommitter(
                        table, table.newCommit(commitUser), createMockContext(true, false));
        ManifestCommittable committable = new ManifestCommittable(1L, null);
        commitMessages.forEach(committable::addFileCommittable);
        committer.commit(Collections.singletonList(committable));
        committer.close();

        assertThat(commitListenerResult.get(testId)).containsExactly("20250101", "20250102");
    }

    /** A mock {@link CommitListener}. */
    public static class TestPartitionCollector implements CommitListener {

        private final String testId;

        public TestPartitionCollector(String testId) {
            this.testId = testId;
        }

        @Override
        public void notifyCommittable(List<ManifestCommittable> committables) {
            commitListenerResult
                    .computeIfAbsent(testId, k -> new HashSet<>())
                    .addAll(
                            committables.stream()
                                    .flatMap(c -> c.fileCommittables().stream())
                                    .map(CommitMessage::partition)
                                    .map(p -> p.getString(0).toString())
                                    .collect(Collectors.toSet()));
        }

        @Override
        public void snapshotState() throws Exception {}

        @Override
        public void close() throws IOException {}

        /** A mock {@link CommitListenerFactory}. */
        public static class Factory implements CommitListenerFactory {

            @Override
            public String identifier() {
                return "partition-collector";
            }

            @Override
            public Optional<CommitListener> create(Committer.Context context, FileStoreTable table)
                    throws Exception {
                return Optional.of(
                        new TestPartitionCollector(table.options().get("test-listener-id")));
            }
        }
    }
}
