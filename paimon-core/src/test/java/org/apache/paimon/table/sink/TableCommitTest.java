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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.WriteMode;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FailingFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableCommit}. */
public class TableCommitTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCommitCallbackWithFailure() throws Exception {
        int numIdentifiers = 30;

        String failingName = UUID.randomUUID().toString();
        // no failure when creating table and writing data
        FailingFileIO.reset(failingName, 0, 1);

        String path = FailingFileIO.getFailingPath(failingName, tempDir.toString());

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});

        Options conf = new Options();
        conf.set(CoreOptions.PATH, path);
        conf.set(CoreOptions.WRITE_MODE, WriteMode.CHANGE_LOG);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), new Path(path)),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                conf.toMap(),
                                ""));

        Set<Long> identifiers = new HashSet<>();
        FileStoreTable table =
                FileStoreTableFactory.create(
                        new FailingFileIO(),
                        new Path(path),
                        tableSchema,
                        Lock.emptyFactory(),
                        Collections.singletonList(t -> new TestCommitCallback(identifiers)));

        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = table.newWrite(commitUser);
        Map<Long, List<CommitMessage>> commitMessages = new HashMap<>();
        for (int i = 0; i < numIdentifiers; i++) {
            write.write(GenericRow.of(i, i * 1000L));
            commitMessages.put((long) i, write.prepareCommit(true, i));
        }

        StreamTableCommit commit = table.newCommit(commitUser);
        // enable failure when committing
        FailingFileIO.reset(failingName, 3, 1000);
        FailingFileIO.retryArtificialException(() -> commit.filterAndCommit(commitMessages));

        assertThat(identifiers)
                .isEqualTo(LongStream.range(0, numIdentifiers).boxed().collect(Collectors.toSet()));
    }

    private static class TestCommitCallback implements CommitCallback {

        private final Set<Long> identifiers;

        private TestCommitCallback(Set<Long> identifiers) {
            this.identifiers = identifiers;
        }

        @Override
        public void call(List<ManifestCommittable> committables) {
            committables.forEach(c -> identifiers.add(c.identifier()));
        }

        @Override
        public void close() throws Exception {}
    }
}
