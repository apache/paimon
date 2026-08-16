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

package org.apache.paimon.flink.compact.changelog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ChangelogCompactTask}. */
public class ChangelogCompactTaskTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testExceptionWhenRead() throws Exception {
        FileSystemCatalog catalog =
                new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        catalog.createDatabase("default", false);
        catalog.createTable(
                Identifier.create("default", "T"),
                new Schema(
                        Arrays.asList(
                                new DataField(0, "k", DataTypes.INT()),
                                new DataField(1, "v", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        new HashMap<>(),
                        ""),
                false);

        Map<Integer, List<DataFileMeta>> files = new HashMap<>();
        files.put(
                0,
                Collections.singletonList(
                        DataFileMeta.forAppend(
                                "unexisting-file",
                                128,
                                0,
                                SimpleStats.EMPTY_STATS,
                                0,
                                0,
                                1,
                                Collections.emptyList(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)));
        ChangelogCompactTask task =
                new ChangelogCompactTask(1, BinaryRow.EMPTY_ROW, 1, files, new HashMap<>());
        assertThatThrownBy(
                        () ->
                                task.doCompact(
                                        (FileStoreTable)
                                                catalog.getTable(Identifier.create("default", "T")),
                                        Executors.newFixedThreadPool(1),
                                        MemorySize.ofMebiBytes(64)))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("unexisting-file");
    }

    @Test
    public void testManyExternalPaths() throws Exception {
        String warehouse = tempDir.toString() + "/warehouse";
        FileSystemCatalog catalog =
                new FileSystemCatalog(LocalFileIO.create(), new Path(warehouse));
        catalog.createDatabase("default", false);

        List<String> externalPaths = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String path = "file://" + tempDir.toString() + "/external" + i;
            externalPaths.add(path);
        }

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "2");
        options.put(
                CoreOptions.CHANGELOG_PRODUCER.key(),
                CoreOptions.ChangelogProducer.INPUT.toString());
        options.put(
                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                String.valueOf(System.currentTimeMillis()));
        options.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(), String.join(",", externalPaths));
        options.put(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(),
                CoreOptions.ExternalPathStrategy.ROUND_ROBIN.toString());
        catalog.createTable(
                Identifier.create("default", "T"),
                new Schema(
                        Arrays.asList(
                                new DataField(0, "k", DataTypes.INT()),
                                new DataField(1, "v", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        options,
                        ""),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("default", "T"));
        int numRecords = 10;

        Map<Integer, List<DataFileMeta>> files = new HashMap<>();
        TableWriteImpl<?> write = table.newWrite("test");
        for (int i = 0; i < numRecords; i++) {
            write.write(GenericRow.of(i, i * 10));
            for (CommitMessage message : write.prepareCommit(false, 1)) {
                CommitMessageImpl casted = (CommitMessageImpl) message;
                files.computeIfAbsent(message.bucket(), k -> new ArrayList<>())
                        .addAll(casted.newFilesIncrement().changelogFiles());
            }
        }
        write.close();

        ChangelogCompactTask task =
                new ChangelogCompactTask(1, BinaryRow.EMPTY_ROW, 2, files, new HashMap<>());

        List<CommitMessage> messages =
                task.doCompact(table, Executors.newFixedThreadPool(1), MemorySize.ofMebiBytes(64))
                        .stream()
                        .map(c -> (CommitMessageImpl) c.commitMessage())
                        .collect(Collectors.toList());
        TableCommitImpl commit = table.newCommit("test");
        commit.commit(messages);
        commit.close();

        StreamDataTableScan scan = table.newStreamScan();
        List<String> actual = new ArrayList<>();
        while (actual.size() < numRecords) {
            TableScan.Plan plan = scan.plan();
            CloseableIterator<InternalRow> it =
                    new RecordReaderIterator<>(table.newRead().createReader(plan));
            while (it.hasNext()) {
                InternalRow row = it.next();
                actual.add(String.format("(%d, %d)", row.getInt(0), row.getInt(1)));
            }
        }

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            expected.add(String.format("(%d, %d)", i, i * 10));
        }
        assertThat(actual).hasSameElementsAs(expected);
    }
}
