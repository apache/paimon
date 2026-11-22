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

import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.ScheduledFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AppendTableCompactSource}. */
public class AppendTableCompactSourceTest {

    private static final long SCAN_INTERVAL_MS = 3_000L;

    @TempDir Path tempDir;

    private FileStoreTable table;

    @BeforeEach
    public void before() throws Exception {
        Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new org.apache.paimon.fs.Path(tempDir.toUri())));
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .option("bucket", "-1")
                        .build();
        Identifier identifier = Identifier.create("default", "t");
        catalog.createDatabase("default", false);
        catalog.createTable(identifier, schema, false);
        this.table = (FileStoreTable) catalog.getTable(identifier);
    }

    @Test
    public void testPollWithoutCompactionTaskReportsNothingAvailable() throws Exception {
        ManuallyTriggeredScheduledExecutorService timer =
                new ManuallyTriggeredScheduledExecutorService();
        try (AppendTableCompactSource.CompactSourceReader reader =
                new AppendTableCompactSource.CompactSourceReader(
                        table, true, null, SCAN_INTERVAL_MS, new DelayedAvailability(timer))) {
            TestingReaderOutput<AppendCompactTask> output = new TestingReaderOutput<>();

            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(output.getEmittedRecords()).isEmpty();
            // the poll must not block the mailbox thread for the scan interval
            assertThat(reader.isAvailable()).isNotDone();

            timer.triggerNonPeriodicScheduledTasks();

            assertThat(reader.isAvailable()).isDone();
        }
    }

    @Test
    public void testCloseCancelsThePendingWait() throws Exception {
        ManuallyTriggeredScheduledExecutorService timer =
                new ManuallyTriggeredScheduledExecutorService();
        try (AppendTableCompactSource.CompactSourceReader reader =
                new AppendTableCompactSource.CompactSourceReader(
                        table, true, null, SCAN_INTERVAL_MS, new DelayedAvailability(timer))) {
            assertThat(reader.pollNext(new TestingReaderOutput<>()))
                    .isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(timer.getAllNonPeriodicScheduledTask()).hasSize(1);
            ScheduledFuture<?> wakeUp = timer.getAllNonPeriodicScheduledTask().get(0);

            reader.close();

            assertThat(wakeUp.isCancelled()).isTrue();
            assertThat(reader.isAvailable()).isDone();
        }
    }
}
