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

package org.apache.paimon.flink.query;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.lookup.LookupFileStoreTable;
import org.apache.paimon.flink.lookup.LookupStreamingReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.types.DataTypes.INT;
import static org.apache.paimon.types.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the materialized, partition/bucket-sharded query-service backend. */
class FullCacheTableQueryTest extends TableTestBase {

    private FileStoreTable createTable(String changelog) throws Exception {
        return createTable(changelog, Collections.emptyMap());
    }

    private FileStoreTable createTable(String changelog, Map<String, String> extraOptions)
            throws Exception {
        RowType rowType =
                RowType.builder().field("k", STRING()).field("pt", INT()).field("v", INT()).build();
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        options.put("changelog-producer", changelog);
        options.put("lookup.cache", "FULL");
        options.put("write-buffer-size", "1 mb");
        options.putAll(extraOptions);
        Identifier identifier = identifier("dim");
        catalog.createTable(
                identifier,
                new Schema(
                        rowType.getFields(),
                        Collections.singletonList("pt"),
                        Arrays.asList("k", "pt"),
                        options,
                        null),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private GenericRow value(String key, int partition, int value) {
        return GenericRow.of(BinaryString.fromString(key), partition, value);
    }

    private GenericRow key(String key) {
        return GenericRow.of(BinaryString.fromString(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {"input", "none"})
    void testShardedBootstrapUpdatesDeletesAndProjection(String changelog) throws Exception {
        FileStoreTable table = createTable(changelog);
        for (int pt = 1; pt <= 3; pt++) {
            for (int bucket = 0; bucket < 4; bucket++) {
                write(table, Pair.of(value("key-" + bucket, pt, pt * 100 + bucket), bucket));
            }
        }
        List<FullCacheTableQuery> queries = new ArrayList<>();
        try {
            for (int server = 0; server < 3; server++) {
                queries.add(new FullCacheTableQuery(table, tempPath.toFile(), server, 3));
            }
            for (int pt = 1; pt <= 3; pt++) {
                for (int bucket = 0; bucket < 4; bucket++) {
                    int owner = ChannelComputer.select(row(pt), bucket, 3);
                    for (int server = 0; server < 3; server++) {
                        InternalRow result =
                                queries.get(server).lookup(row(pt), bucket, key("key-" + bucket));
                        if (server == owner) {
                            assertThat(result).isNotNull();
                            assertThat(result.getInt(2)).isEqualTo(pt * 100 + bucket);
                        } else {
                            assertThat(result).isNull();
                        }
                    }
                }
            }

            // Updates are applied without a client request; the same trimmed key in another
            // partition must keep its own value.
            write(table, Pair.of(value("key-0", 1, 999), 0));
            GenericRow delete = value("key-1", 1, 101);
            delete.setRowKind(RowKind.DELETE);
            write(table, Pair.of(delete, 1));
            compact(table, row(1), 0);
            compact(table, row(1), 1);
            for (FullCacheTableQuery query : queries) {
                query.refresh();
            }
            FullCacheTableQuery owner = queries.get(ChannelComputer.select(row(1), 0, 3));
            assertThat(owner.lookup(row(1), 0, key("key-0")).getInt(2)).isEqualTo(999);
            assertThat(
                            queries.get(ChannelComputer.select(row(2), 0, 3))
                                    .lookup(row(2), 0, key("key-0"))
                                    .getInt(2))
                    .isEqualTo(200);
            assertThat(
                            queries.get(ChannelComputer.select(row(1), 1, 3))
                                    .lookup(row(1), 1, key("key-1")))
                    .isNull();
            assertThat(owner.lookup(row(1), 0, key("missing"))).isNull();

            owner.withValueProjection(new int[] {2, 0});
            InternalRow projected = owner.lookup(row(1), 0, key("key-0"));
            assertThat(projected.getFieldCount()).isEqualTo(2);
            assertThat(projected.getInt(0)).isEqualTo(999);
            assertThat(projected.getString(1).toString()).isEqualTo("key-0");
            assertThat(owner.createValueSerializer().toBinaryRow(projected).getInt(0))
                    .isEqualTo(999);
        } finally {
            for (FullCacheTableQuery query : queries) {
                query.close();
            }
        }
    }

    @Test
    void testEmptyBootstrapOverwriteAndReopen() throws Exception {
        FileStoreTable table = createTable("input");
        FullCacheTableQuery query = new FullCacheTableQuery(table, tempPath.toFile(), 0, 1);
        try {
            assertThat(query.lookup(row(1), 0, key("a"))).isNull();
            write(table, Pair.of(value("a", 1, 10), 0), Pair.of(value("a", 2, 20), 0));
            query.refresh();
            assertThat(query.lookup(row(1), 0, key("a")).getInt(2)).isEqualTo(10);

            BatchWriteBuilder builder =
                    table.newBatchWriteBuilder().withOverwrite(Collections.singletonMap("pt", "1"));
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.write(value("b", 1, 30), 0);
                commit.commit(write.prepareCommit());
            }
            query.refresh();
            assertThat(query.lookup(row(1), 0, key("a"))).isNull();
            assertThat(query.lookup(row(1), 0, key("b")).getInt(2)).isEqualTo(30);
            assertThat(query.lookup(row(2), 0, key("a")).getInt(2)).isEqualTo(20);
        } finally {
            query.close();
        }
        query.close();
        assertThatThrownBy(() -> query.lookup(row(1), 0, key("b")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
        try (java.util.stream.Stream<java.nio.file.Path> files =
                java.nio.file.Files.list(tempPath)) {
            assertThat(
                            files.noneMatch(
                                    p ->
                                            p.getFileName()
                                                    .toString()
                                                    .startsWith("query-full-cache-")))
                    .isTrue();
        }
        // A restarted executor bootstraps current data, without depending on local state.
        try (FullCacheTableQuery restarted =
                new FullCacheTableQuery(table, tempPath.toFile(), 0, 1)) {
            assertThat(restarted.lookup(row(1), 0, key("b")).getInt(2)).isEqualTo(30);
        }
    }

    @Test
    void testShardFilteringSkipsUnassignedSnapshots() throws Exception {
        FileStoreTable table = createTable("input");
        int owner = ChannelComputer.select(row(1), 0, 2);
        write(table, Pair.of(value("foreign", 1, 1), 1));
        LookupStreamingReader reader =
                new LookupStreamingReader(
                                LookupFileStoreTable.create(table, table.primaryKeys()),
                                new int[] {0, 1, 2},
                                null,
                                null,
                                null,
                                null)
                        .withPartitionBucketFilter(
                                (partition, bucket) ->
                                        ChannelComputer.select(partition, bucket, 2) == owner);
        assertThat(reader.nextSplits()).isEmpty();
        // Two snapshots are pending; the first contains only another executor's bucket.
        write(table, Pair.of(value("foreign-2", 1, 2), 1));
        write(table, Pair.of(value("owned", 1, 3), 0));
        List<Split> splits = reader.nextSplits();
        assertThat(splits).isNotEmpty();
        assertThat(splits)
                .allSatisfy(
                        split -> {
                            DataSplit data = (DataSplit) split;
                            assertThat(ChannelComputer.select(data.partition(), data.bucket(), 2))
                                    .isEqualTo(owner);
                        });
        assertThat(reader.nextSplits()).isEmpty();
    }

    @Test
    void testCompactionDiffRefreshUsesShardAssignment() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("merge-engine", "partial-update");
        options.put("deletion-vectors.enabled", "true");
        options.put("bucket", "1");
        FileStoreTable table = createTable("none", options);
        write(table, ioManager, value("a", 1, 10));
        compact(table, row(1), 0, ioManager, true);
        int owner = ChannelComputer.select(row(1), 0, 2);
        try (FullCacheTableQuery query =
                new FullCacheTableQuery(table, tempPath.toFile(), owner, 2)) {
            assertThat(query.lookup(row(1), 0, key("a")).getInt(2)).isEqualTo(10);
            write(table, ioManager, value("a", 1, 30));
            compact(table, row(1), 0, ioManager, true);
            query.refresh();
            assertThat(query.lookup(row(1), 0, key("a")).getInt(2)).isEqualTo(30);
        }
    }

    @Test
    void testExpiredSnapshotsRebuildCache() throws Exception {
        FileStoreTable table = createTable("input");
        write(table, Pair.of(value("a", 1, 10), 0));
        try (FullCacheTableQuery query = new FullCacheTableQuery(table, tempPath.toFile(), 0, 1)) {
            write(table, Pair.of(value("a", 1, 20), 0));
            write(table, Pair.of(value("a", 1, 30), 0));
            long latest = table.snapshotManager().latestSnapshotId();
            for (long snapshot = 1; snapshot < latest; snapshot++) {
                table.snapshotManager().deleteSnapshot(snapshot);
            }
            table.snapshotManager().commitEarliestHint(latest);
            query.refresh();
            assertThat(query.lookup(row(1), 0, key("a")).getInt(2)).isEqualTo(30);
        }
    }

    @Test
    void testRefreshFailureStopsServingStaleRows() throws Exception {
        FileStoreTable table = createTable("none");
        write(table, Pair.of(value("a", 1, 10), 0));
        try (FullCacheTableQuery query = new FullCacheTableQuery(table, tempPath.toFile(), 0, 1)) {
            write(table, Pair.of(value("a", 1, 20), 0));
            for (Split split : table.newReadBuilder().newScan().plan().splits()) {
                DataSplit data = (DataSplit) split;
                for (org.apache.paimon.io.DataFileMeta file : data.dataFiles()) {
                    table.fileIO()
                            .delete(
                                    new org.apache.paimon.fs.Path(
                                            data.bucketPath(), file.fileName()),
                                    false);
                }
            }
            assertThatThrownBy(query::refresh).isInstanceOf(Exception.class);
            assertThatThrownBy(() -> query.lookup(row(1), 0, key("a")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to refresh");
        }
    }

    @Test
    void testConcurrentRequestsAndRefreshReturnIndependentRows() throws Exception {
        FileStoreTable table = createTable("input");
        write(table, Pair.of(value("a", 1, 10), 0), Pair.of(value("b", 2, 20), 0));
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (FullCacheTableQuery query = new FullCacheTableQuery(table, tempPath.toFile(), 0, 1)) {
            InternalRow retained = query.lookup(row(1), 0, key("a"));
            List<Future<?>> requests = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                requests.add(
                        executor.submit(
                                () -> {
                                    for (int j = 0; j < 100; j++) {
                                        try {
                                            InternalRow result = query.lookup(row(2), 0, key("b"));
                                            assertThat(result.getString(0).toString())
                                                    .isEqualTo("b");
                                            assertThat(result.getInt(1)).isEqualTo(2);
                                            assertThat(result.getInt(2)).isEqualTo(20);
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                }));
            }
            write(table, Pair.of(value("a", 1, 30), 0));
            query.refresh();
            for (Future<?> request : requests) {
                request.get();
            }
            assertThat(query.lookup(row(1), 0, key("a")).getInt(2)).isEqualTo(30);
            assertThat(retained.getInt(2)).isEqualTo(10);
        } finally {
            executor.shutdownNow();
        }
    }
}
