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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.PartitionPathUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test for {@link HmsReporter}. */
public class HmsReporterTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testReportAction() throws Exception {
        Path tablePath = new Path(tempDir.toString(), "table");
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "c1", DataTypes.STRING()),
                                new DataField(1, "c2", DataTypes.STRING()),
                                new DataField(2, "c3", DataTypes.STRING())),
                        Collections.singletonList("c1"),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        "");
        schemaManager.createTable(schema);

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        BatchTableWrite writer = table.newBatchWriteBuilder().newWrite();
        writer.write(
                GenericRow.of(
                        BinaryString.fromString("a"),
                        BinaryString.fromString("a"),
                        BinaryString.fromString("a")));
        writer.write(
                GenericRow.of(
                        BinaryString.fromString("b"),
                        BinaryString.fromString("a"),
                        BinaryString.fromString("a")));
        List<CommitMessage> messages = writer.prepareCommit();
        BatchTableCommit committer = table.newBatchWriteBuilder().newCommit();
        committer.commit(messages);
        AtomicBoolean closed = new AtomicBoolean(false);
        Map<String, Map<String, String>> partitionParams = Maps.newHashMap();

        MetastoreClient client =
                new MetastoreClient() {
                    @Override
                    public void addPartition(BinaryRow partition) throws Exception {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void addPartition(LinkedHashMap<String, String> partitionSpec)
                            throws Exception {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void deletePartition(LinkedHashMap<String, String> partitionSpec)
                            throws Exception {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void markDone(LinkedHashMap<String, String> partitionSpec)
                            throws Exception {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void alterPartition(
                            LinkedHashMap<String, String> partitionSpec,
                            Map<String, String> parameters,
                            long modifyTime,
                            boolean ignoreIfNotExist)
                            throws Exception {
                        partitionParams.put(
                                PartitionPathUtils.generatePartitionPath(partitionSpec),
                                parameters);
                    }

                    @Override
                    public void close() throws Exception {
                        closed.set(true);
                    }
                };

        HmsReporter action = new HmsReporter(table, client);
        long time = 1729598544974L;
        action.report("c1=a/", time);
        Assertions.assertThat(partitionParams).containsKey("c1=a/");
        Assertions.assertThat(partitionParams.get("c1=a/"))
                .isEqualTo(
                        ImmutableMap.of(
                                "numFiles",
                                "1",
                                "totalSize",
                                "591",
                                "numRows",
                                "1",
                                "transient_lastDdlTime",
                                String.valueOf(time / 1000)));
        action.close();
        Assertions.assertThat(closed).isTrue();
    }
}
