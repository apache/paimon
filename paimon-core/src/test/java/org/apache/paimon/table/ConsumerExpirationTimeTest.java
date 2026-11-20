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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/** Tests that consumer lifetime protects snapshots for the configured elapsed duration. */
@ResourceLock(Resources.TIME_ZONE)
class ConsumerExpirationTimeTest extends TableTestBase {

    @ParameterizedTest
    @CsvSource({
        "America/New_York,2026-03-08T07:30:00Z,45,false",
        "America/New_York,2026-03-08T07:30:00Z,75,true",
        "America/New_York,2026-11-01T06:30:00Z,45,false",
        "America/New_York,2026-11-01T06:30:00Z,75,true",
        "America/New_York,2026-11-02T07:30:00Z,45,false",
        "America/New_York,2026-11-02T07:30:00Z,75,true",
        "UTC,2026-03-08T07:30:00Z,60,false",
        "UTC,2026-03-08T07:30:00Z,61,true"
    })
    void testConsumerLifetimeUsesElapsedTime(
            String zoneName, String nowText, int ageMinutes, boolean expired) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 h")
                        .option(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1")
                        .option(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1")
                        .option(CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE.key(), "sync")
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        write(table, GenericRow.of(1));
        long protectedSnapshot = table.latestSnapshot().get().id();
        ConsumerManager consumers = new ConsumerManager(table.fileIO(), table.location());
        consumers.resetConsumer("reader", new Consumer(protectedSnapshot));

        ZoneId zone = ZoneId.of(zoneName);
        Instant now = Instant.parse(nowText);
        Instant lastActive = now.minus(ageMinutes, ChronoUnit.MINUTES);
        Files.setLastModifiedTime(
                Paths.get(table.location().toUri().getPath(), "consumer", "consumer-reader"),
                FileTime.from(lastActive));
        LocalDateTime localNow = LocalDateTime.ofInstant(now, zone);
        TimeZone originalZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone(zone));
        try (MockedStatic<LocalDateTime> localTimes =
                        mockStatic(LocalDateTime.class, CALLS_REAL_METHODS);
                MockedStatic<Instant> instants = mockStatic(Instant.class, CALLS_REAL_METHODS)) {
            localTimes.when(LocalDateTime::now).thenReturn(localNow);
            instants.when(Instant::now).thenReturn(now);
            write(table, GenericRow.of(2));
        } finally {
            TimeZone.setDefault(originalZone);
        }

        assertThat(consumers.consumer("reader").isPresent()).isEqualTo(!expired);
        assertThat(table.snapshotManager().snapshotExists(protectedSnapshot)).isEqualTo(!expired);
        assertThat(table.latestSnapshot().get().id()).isGreaterThan(protectedSnapshot);
    }
}
