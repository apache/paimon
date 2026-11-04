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

package org.apache.paimon.operation;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that diagnostics remain available during blocked work and release scheduled tasks. */
class MaintenanceProgressTest {
    private final ScheduledThreadPoolExecutor reporter = new ScheduledThreadPoolExecutor(1);
    private final List<String> messages = new CopyOnWriteArrayList<>();
    private final CountDownLatch activeHeartbeat = new CountDownLatch(1);
    private Level previousLevel;
    private boolean previousAdditive;
    private final Logger logger = (Logger) LogManager.getLogger(MaintenanceProgress.class);
    private final AbstractAppender appender =
            new AbstractAppender("maintenance-test", null, null, false, Property.EMPTY_ARRAY) {
                @Override
                public void append(LogEvent event) {
                    String message = event.getMessage().getFormattedMessage();
                    messages.add(message);
                    if (message.contains("event=PROGRESS")
                            && message.contains("oldest_active=[blocked-manifest]")) {
                        activeHeartbeat.countDown();
                    }
                }
            };

    @BeforeAll
    static void initializeLogging() throws ClassNotFoundException {
        // Initialize the SLF4J binding before changing the Log4j test logger configuration.
        Class.forName(MaintenanceProgress.class.getName());
    }

    @BeforeEach
    void setup() {
        reporter.setRemoveOnCancelPolicy(true);
        previousLevel = logger.getLevel();
        previousAdditive = logger.isAdditive();
        Configurator.setLevel(logger.getName(), Level.INFO);
        logger.setAdditive(false);
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void cleanup() {
        reporter.shutdownNow();
        logger.removeAppender(appender);
        appender.stop();
        Configurator.setLevel(logger.getName(), previousLevel);
        logger.setAdditive(previousAdditive);
    }

    @Test
    void reportsBlockedWorkAndStopsOnCompletion() throws Exception {
        MaintenanceProgress progress =
                new MaintenanceProgress("test-table", "read-manifests", reporter, 10);
        try (MaintenanceProgress ignored = progress) {
            progress.add("manifests_total", 2);
            long item = progress.startItem("blocked-manifest");
            // The work thread is blocked here; the independent reporter must still run.
            assertThat(activeHeartbeat.await(10, TimeUnit.SECONDS))
                    .as(
                            "Captured logs: %s; scheduled executions: %s",
                            messages, reporter.getCompletedTaskCount())
                    .isTrue();
            progress.finishItem(item);
            progress.add("manifests_read", 1);
            progress.phase("delete-files");
            progress.complete();
        }
        progress.close();
        assertThat(reporter.getQueue()).isEmpty();
        assertThat(messages)
                .anySatisfy(
                        message -> {
                            assertThat(message)
                                    .contains(
                                            "event=PROGRESS",
                                            "active_calls=1",
                                            "manifests_total=2",
                                            "oldest_active_ms=");
                        });
        assertThat(messages.stream().filter(message -> message.contains("event=END"))).hasSize(1);
        assertThat(messages)
                .anySatisfy(
                        message -> {
                            assertThat(message)
                                    .contains(
                                            "event=END",
                                            "phase=[delete-files]",
                                            "manifests_read=1",
                                            "active_calls=0");
                        });
    }

    @Test
    void releasesHeartbeatOnFailureWithoutReplacingOriginalException() {
        IOException failure = new IOException("read failed");
        assertThatThrownBy(
                        () -> {
                            try (MaintenanceProgress progress =
                                    new MaintenanceProgress(
                                            "test-table", "read-manifests", reporter, 60_000)) {
                                progress.startItem("failed-manifest");
                                throw failure;
                            }
                        })
                .isSameAs(failure);
        assertThat(reporter.getQueue()).isEmpty();
        assertThat(messages)
                .anySatisfy(
                        message -> {
                            assertThat(message)
                                    .contains("event=ABORTED", "oldest_active=[failed-manifest]");
                        });
    }

    @Test
    void tracksConcurrentDeletionCalls() {
        try (MaintenanceProgress progress =
                new MaintenanceProgress("test-table", "delete-files", reporter, 60_000)) {
            IntStream.range(0, 1000)
                    .parallel()
                    .forEach(
                            index -> {
                                long item = progress.startItem("file-" + index);
                                progress.add("delete_calls_completed", 1);
                                progress.finishItem(item);
                            });
            progress.complete();
        }
        assertThat(messages)
                .anySatisfy(
                        message -> {
                            assertThat(message)
                                    .contains(
                                            "event=END",
                                            "delete_calls_completed=1000",
                                            "active_calls=0");
                        });
    }
}
