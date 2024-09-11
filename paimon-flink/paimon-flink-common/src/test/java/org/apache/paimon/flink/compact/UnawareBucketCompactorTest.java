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

package org.apache.paimon.flink.compact;

import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.source.FileStoreSourceReaderTest;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.ExecutorThreadFactory;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.paimon.operation.metrics.CompactionMetrics.AVG_COMPACTION_TIME;
import static org.apache.paimon.operation.metrics.CompactionMetrics.COMPACTION_THREAD_BUSY;

/** Test for {@link UnawareBucketCompactor}. */
public class UnawareBucketCompactorTest {

    @TempDir private Path dir;
    private String tableName = "Orders1";
    private String dataBaseName = "my_db";
    private Catalog catalog;

    @Test
    public void testGaugeCollection() throws Exception {
        createTable();
        ExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "-append-only-compact-worker"));
        Map<String, Gauge> map = new HashMap<>();
        UnawareBucketCompactor unawareBucketCompactor =
                new UnawareBucketCompactor(
                        (FileStoreTable) catalog.getTable(identifier()),
                        "10086",
                        () -> executorService,
                        new FileStoreSourceReaderTest.DummyMetricGroup() {
                            @Override
                            public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                                map.put(name, gauge);
                                return null;
                            }

                            @Override
                            public MetricGroup addGroup(String name) {
                                return this;
                            }

                            @Override
                            public MetricGroup addGroup(String key, String value) {
                                return this;
                            }
                        });

        for (int i = 0; i < 320; i++) {
            unawareBucketCompactor.processElement(new MockCompactionTask());
            Thread.sleep(250);
            System.out.println(map.get(COMPACTION_THREAD_BUSY).getValue());
            System.out.println(map.get(AVG_COMPACTION_TIME).getValue());
        }

        double compactionThreadBusy = (double) map.get(COMPACTION_THREAD_BUSY).getValue();
        double compactionAvrgTime = (double) map.get(AVG_COMPACTION_TIME).getValue();

        Assertions.assertThat(compactionThreadBusy).isGreaterThan(45).isLessThan(55);
        Assertions.assertThat(compactionAvrgTime).isGreaterThan(120).isLessThan(140);
    }

    protected Catalog getCatalog() {
        if (catalog == null) {
            Options options = new Options();
            options.set(
                    CatalogOptions.WAREHOUSE,
                    new org.apache.paimon.fs.Path(dir.toString()).toUri().toString());
            catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        }
        return catalog;
    }

    protected void createTable() throws Exception {
        getCatalog().createDatabase(dataBaseName, true);
        getCatalog().createTable(identifier(), schema(), true);
    }

    protected Identifier identifier() {
        return Identifier.create(dataBaseName, tableName);
    }

    protected static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.column("f2", DataTypes.SMALLINT());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.column("f4", DataTypes.DOUBLE());
        schemaBuilder.column("f5", DataTypes.CHAR(100));
        schemaBuilder.column("f6", DataTypes.VARCHAR(100));
        schemaBuilder.column("f7", DataTypes.BOOLEAN());
        schemaBuilder.column("f8", DataTypes.DATE());
        schemaBuilder.column("f10", DataTypes.TIMESTAMP(9));
        schemaBuilder.column("f11", DataTypes.DECIMAL(10, 2));
        schemaBuilder.column("f12", DataTypes.BYTES());
        schemaBuilder.column("f13", DataTypes.FLOAT());
        schemaBuilder.column("f14", DataTypes.BINARY(10));
        schemaBuilder.column("f15", DataTypes.VARBINARY(10));
        return schemaBuilder.build();
    }

    /** Mock compaction task for test. */
    private static class MockCompactionTask extends UnawareAppendCompactionTask {

        public MockCompactionTask() {
            super(BinaryRow.EMPTY_ROW, Collections.emptyList());
        }

        @Override
        public CommitMessage doCompact(FileStoreTable table, AppendOnlyFileStoreWrite write)
                throws Exception {
            Thread.sleep(125);
            return null;
        }
    }
}
