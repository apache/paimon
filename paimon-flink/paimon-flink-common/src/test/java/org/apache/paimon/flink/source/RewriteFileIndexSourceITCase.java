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

package org.apache.paimon.flink.source;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.sink.NoneCopyVersionedSerializerTypeSerializerProxy;
import org.apache.paimon.flink.sink.RewriteFileIndexSink;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link RewriteFileIndexSource}. */
public class RewriteFileIndexSourceITCase extends CatalogITCaseBase {

    @Test
    @Timeout(120)
    public void testRestoreFromSavepoint() throws Exception {
        sql("CREATE TABLE T (k INT, v STRING) WITH ('bucket' = '-1', 'write-only' = 'true')");
        for (int i = 0; i < 10; i++) {
            sql("INSERT INTO T VALUES (%d, 'v%d')", i, i);
        }
        sql("ALTER TABLE T SET ('file-index.bloom-filter.columns' = 'k')");
        FileStoreTable table = paimonTable("T");
        long snapshotBeforeRewrite = table.snapshotManager().latestSnapshotId();

        // stop the rewrite after its first commit, while files are still left to the enumerator
        JobClient jobClient = rewriteFileIndex(table, null);
        while (table.snapshotManager().latestSnapshotId() == snapshotBeforeRewrite) {
            Thread.sleep(100);
        }
        String savepoint =
                jobClient
                        .stopWithSavepoint(false, path + "/savepoint", SavepointFormatType.DEFAULT)
                        .get();
        assertThat(countFilesWithIndex()).isBetween(1, 9);

        // the restored enumerator hands out the rest of the files
        rewriteFileIndex(table, savepoint).getJobExecutionResult().get(60, TimeUnit.SECONDS);

        assertThat(countFilesWithIndex()).isEqualTo(10);
        assertThat(sql("SELECT COUNT(*) FROM T")).containsExactly(Row.of(10L));
    }

    private int countFilesWithIndex() throws Exception {
        flinkCatalog().catalog().invalidateTable(Identifier.create(tEnv.getCurrentDatabase(), "T"));
        List<ManifestEntry> files = paimonTable("T").store().newScan().plan().files();
        assertThat(files).hasSize(10);
        int indexed = 0;
        for (ManifestEntry entry : files) {
            if (entry.file().embeddedIndex() != null
                    || entry.file().extraFiles().stream()
                            .anyMatch(f -> f.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))) {
                indexed++;
            }
        }
        return indexed;
    }

    private JobClient rewriteFileIndex(FileStoreTable table, @Nullable String savepoint)
            throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + path + "/checkpoint");
        if (savepoint != null) {
            SavepointRestoreSettings.toConfiguration(
                    SavepointRestoreSettings.forPath(savepoint, false), conf);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(500);

        DataStream<ManifestEntry> source =
                env.fromSource(
                                new RewriteFileIndexSource(table, null),
                                WatermarkStrategy.noWatermarks(),
                                "index source",
                                new ManifestEntryTypeInfo())
                        .map(new SlowIdentity(), new ManifestEntryTypeInfo());
        new RewriteFileIndexSink(table).sinkFrom(source);
        return env.executeAsync();
    }

    /** Slows the rewrite down so that the job is still running when it is stopped. */
    private static class SlowIdentity implements MapFunction<ManifestEntry, ManifestEntry> {

        @Override
        public ManifestEntry map(ManifestEntry entry) throws Exception {
            Thread.sleep(500);
            return entry;
        }
    }

    private static class ManifestEntryTypeInfo extends GenericTypeInfo<ManifestEntry> {

        public ManifestEntryTypeInfo() {
            super(ManifestEntry.class);
        }

        @Override
        public TypeSerializer<ManifestEntry> createSerializer(SerializerConfig config) {
            return new NoneCopyVersionedSerializerTypeSerializerProxy<>(
                    () ->
                            new SimpleVersionedSerializer<ManifestEntry>() {
                                private final ManifestEntrySerializer serializer =
                                        new ManifestEntrySerializer();

                                @Override
                                public int getVersion() {
                                    return 0;
                                }

                                @Override
                                public byte[] serialize(ManifestEntry entry) throws IOException {
                                    return serializer.serializeToBytes(entry);
                                }

                                @Override
                                public ManifestEntry deserialize(int version, byte[] bytes)
                                        throws IOException {
                                    return serializer.deserializeFromBytes(bytes);
                                }
                            });
        }
    }
}
