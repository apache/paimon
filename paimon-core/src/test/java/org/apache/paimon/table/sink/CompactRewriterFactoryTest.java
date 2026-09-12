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

package org.apache.paimon.table.sink;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.FullChangelogMergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.MergeTreeCompactRewriter;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests compaction rewriter installation through the table write API. */
public class CompactRewriterFactoryTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({
        "none,false",
        "input,false",
        "lookup,false",
        "full-compaction,false",
        "none,true",
        "input,true",
        "lookup,true"
    })
    public void testRewriteAndUpgradeAcrossReopenedWriters(String producer, boolean deletionVectors)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("changelog-producer", producer);
        options.put("deletion-vectors.enabled", Boolean.toString(deletionVectors));
        options.put("num-sorted-run.compaction-trigger", "100");
        FileStoreTable table = createTable(options, true);
        List<TrackingRewriter> rewriters = new ArrayList<>();
        Class<?> expected =
                producer.equals("full-compaction")
                        ? FullChangelogMergeTreeCompactRewriter.class
                        : producer.equals("lookup") || deletionVectors
                                ? LookupMergeTreeCompactRewriter.class
                                : MergeTreeCompactRewriter.class;

        for (int run = 0; run < 2; run++) {
            try (IOManagerImpl io = new IOManagerImpl(tempDir.toString());
                    StreamTableWrite write = table.newWrite("test").withIOManager(io);
                    StreamTableCommit commit = table.newCommit("test")) {
                write.withCompactRewriterFactory(
                        (partition, bucket, delegate) -> {
                            assertThat(partition.getInt(0)).isBetween(1, 2);
                            assertThat(bucket).isZero();
                            assertThat(delegate).isExactlyInstanceOf(expected);
                            TrackingRewriter rewriter = new TrackingRewriter(delegate);
                            rewriters.add(rewriter);
                            return rewriter;
                        });
                if (run == 0) {
                    write.write(GenericRow.of(1, 1, 10));
                    write.write(GenericRow.of(1, 2, 20));
                    write.write(GenericRow.of(2, 1, 30));
                    commit.commit(0, write.prepareCommit(true, 0));
                } else {
                    // Restore and compact existing buckets before receiving any new records.
                    write.compact(partition(1), 0, true);
                    write.compact(partition(2), 0, true);
                    commit.commit(1, write.prepareCommit(true, 1));
                    write.write(GenericRow.of(1, 1, 11));
                    write.write(GenericRow.ofKind(RowKind.DELETE, 1, 2, 20));
                    write.write(GenericRow.of(2, 1, 31));
                    write.compact(partition(1), 0, true);
                    write.compact(partition(2), 0, true);
                    commit.commit(2, write.prepareCommit(true, 2));
                }
            }
        }

        assertThat(rewriters.size()).isGreaterThanOrEqualTo(4);
        assertThat(rewriters.stream().mapToInt(r -> r.rewrites.get()).sum()).isPositive();
        if (producer.equals("none") && !deletionVectors) {
            assertThat(rewriters.stream().mapToInt(r -> r.upgrades.get()).sum()).isPositive();
        }
        rewriters.forEach(r -> assertThat(r.closes.get()).isEqualTo(1));
        List<String> rows = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> reader =
                new RecordReaderIterator<>(table.newRead().createReader(table.newScan().plan()))) {
            while (reader.hasNext()) {
                InternalRow row = reader.next();
                rows.add(row.getInt(0) + "/" + row.getInt(1) + "/" + row.getInt(2));
            }
        }
        assertThat(rows).containsExactlyInAnyOrder("1/1/11", "2/1/31");
    }

    @Test
    public void testWriteOnlyDoesNotCreateRewritersAndLateInstallationIsRejected()
            throws Exception {
        FileStoreTable table = createTable(Collections.singletonMap("write-only", "true"), true);
        try (StreamTableWrite write = table.newWrite("test");
                StreamTableCommit commit = table.newCommit("test")) {
            write.withCompactRewriterFactory(
                    (partition, bucket, delegate) -> {
                        throw new AssertionError("write-only must not create a rewriter");
                    });
            write.write(GenericRow.of(1, 1, 10));
            commit.commit(0, write.prepareCommit(true, 0));
            assertThatThrownBy(() -> write.withCompactRewriterFactory((p, b, delegate) -> delegate))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("before creating bucket writers");
        }
    }

    @Test
    public void testAppendWriterRejectsFactory() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap(), false);
        try (StreamTableWrite write = table.newWrite("test")) {
            assertThatThrownBy(() -> write.withCompactRewriterFactory((p, b, delegate) -> delegate))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    private FileStoreTable createTable(Map<String, String> extraOptions, boolean primaryKey)
            throws Exception {
        Map<String, String> options = new HashMap<>(extraOptions);
        options.put("bucket", primaryKey ? "1" : "-1");
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT()},
                        new String[] {"pt", "k", "v"});
        Path path = new Path(tempDir.toUri());
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), path),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                primaryKey ? Arrays.asList("pt", "k") : Collections.emptyList(),
                                options,
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), path, schema);
    }

    private static BinaryRow partition(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }

    private static class TrackingRewriter implements CompactRewriter {
        private final CompactRewriter delegate;
        private final AtomicInteger rewrites = new AtomicInteger();
        private final AtomicInteger upgrades = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();

        private TrackingRewriter(CompactRewriter delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompactResult rewrite(
                int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
                throws Exception {
            rewrites.incrementAndGet();
            return delegate.rewrite(outputLevel, dropDelete, sections);
        }

        @Override
        public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
            upgrades.incrementAndGet();
            return delegate.upgrade(outputLevel, file);
        }

        @Override
        public void close() throws IOException {
            closes.incrementAndGet();
            delegate.close();
        }
    }
}
