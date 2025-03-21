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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.partition.file.SuccessFile;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkConnectorOptions.PartitionMarkDoneActionMode}. */
public class WatermarkPartitionMarkDoneTest extends TableTestBase {
    @Test
    public void testWaterMarkPartitionMarkDone() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.STRING())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .partitionKeys("a")
                        .primaryKey("a", "b")
                        .option(PARTITION_MARK_DONE_ACTION.key(), "success-file")
                        .option(FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_AVRO)
                        .option(FlinkConnectorOptions.PARTITION_MARK_DONE_MODE.key(), "watermark")
                        .option(FlinkConnectorOptions.PARTITION_TIME_INTERVAL.key(), "1 h")
                        .option(FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_DONE.key(), "15 min")
                        .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyy-MM-dd HH")
                        .option(BUCKET.key(), "1")
                        .build();
        catalog.createTable(identifier, schema, true);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

        TableWriteImpl<?> write = table.newWrite("user1");
        TableCommitImpl commit = table.newCommit("user1");
        OperatorMetricGroup metricGroup = UnregisteredMetricsGroup.createOperatorMetricGroup();
        StoreCommitter committer =
                new StoreCommitter(
                        table,
                        commit,
                        Committer.createContext(
                                "user1",
                                metricGroup,
                                true,
                                false,
                                new PartitionMarkDoneTest.MockOperatorStateStore(),
                                1,
                                1));

        write.write(GenericRow.of(BinaryString.fromString("2025-03-01 12"), 1, 1));
        write.write(GenericRow.of(BinaryString.fromString("2025-03-01 13"), 1, 1));
        write.write(GenericRow.of(BinaryString.fromString("2025-03-01 14"), 1, 1));
        ManifestCommittable committable1 =
                new ManifestCommittable(
                        1L,
                        LocalDateTime.parse("2025-03-01T12:50:30.00")
                                .atZone(ZoneId.of("UTC"))
                                .toInstant()
                                .toEpochMilli());

        write.prepareCommit(true, 1L).forEach(committable1::addFileCommittable);
        committer.commit(Collections.singletonList(committable1));

        // No partition will be marked done.
        validatePartitions(
                table,
                emptyList(),
                Arrays.asList("2025-03-01 12", "2025-03-01 13", "2025-03-01 14"));

        write.write(GenericRow.of(BinaryString.fromString("2025-03-01 15"), 1, 1));
        ManifestCommittable committable2 =
                new ManifestCommittable(
                        2L,
                        LocalDateTime.parse("2025-03-01T14:30:30.00")
                                .atZone(ZoneId.of("UTC"))
                                .toInstant()
                                .toEpochMilli());
        write.prepareCommit(true, 2L).forEach(committable2::addFileCommittable);
        committer.commit(Collections.singletonList(committable2));

        // Partitions before 2025-03-01T13:15 will be marked done.
        validatePartitions(
                table,
                Arrays.asList("2025-03-01 12", "2025-03-01 13"),
                Arrays.asList("2025-03-01 14", "2025-03-01 15"));

        write.write(GenericRow.of(BinaryString.fromString("2025-03-01 16"), 2, 1));
        ManifestCommittable committable3 =
                new ManifestCommittable(
                        3L,
                        LocalDateTime.parse("2025-03-01T16:20:30.00")
                                .atZone(ZoneId.of("UTC"))
                                .toInstant()
                                .toEpochMilli());
        write.prepareCommit(true, 3L).forEach(committable3::addFileCommittable);
        committer.commit(Collections.singletonList(committable3));

        // Partitions before 2025-03-01T15:15 will be marked done.
        validatePartitions(
                table,
                Arrays.asList("2025-03-01 12", "2025-03-01 13", "2025-03-01 14", "2025-03-01 15"),
                Collections.singletonList("2025-03-01 16"));

        committer.close();
    }

    private void validatePartitions(
            FileStoreTable table, List<String> donePartitions, List<String> pendingPartitions)
            throws Exception {
        for (String partition : donePartitions) {
            LocalFileIO fileIO = new LocalFileIO();
            Path successPath =
                    new Path(table.location(), String.format("a=%s/_SUCCESS", partition));
            SuccessFile successFile = SuccessFile.safelyFromPath(fileIO, successPath);
            assertThat(successFile).isNotNull();
        }

        for (String partition : pendingPartitions) {
            LocalFileIO fileIO = new LocalFileIO();
            Path dir = new Path(table.location(), String.format("a=%s", partition));
            assertThat(fileIO.exists(dir)).isTrue();
            Path successPath =
                    new Path(table.location(), String.format("a=%s/_SUCCESS", partition));
            SuccessFile successFile = SuccessFile.safelyFromPath(fileIO, successPath);
            assertThat(successFile).isNull();
        }
    }
}
