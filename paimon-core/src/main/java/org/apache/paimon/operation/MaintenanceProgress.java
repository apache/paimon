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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Independent, bounded-frequency diagnostics for blocking maintenance operations. */
public final class MaintenanceProgress implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceProgress.class);
    private static ScheduledThreadPoolExecutor sharedReporter;
    private static int reporterUsers;

    private final String operationId = UUID.randomUUID().toString();
    private final String context;
    private final long started = System.nanoTime();
    private final AtomicLong lastProgress = new AtomicLong(started);
    private final AtomicLong sequence = new AtomicLong();
    private final Map<String, AtomicLong> counters = new ConcurrentSkipListMap<>();
    private final Map<Long, Item> active = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> heartbeat;
    private final boolean shared;
    private volatile String phase;
    private volatile long phaseStarted = started;
    private boolean completed;
    private final AtomicBoolean closed = new AtomicBoolean();

    public MaintenanceProgress(String context, String phase) {
        this(context, phase, acquireReporter(), TimeUnit.SECONDS.toMillis(60), true);
    }

    MaintenanceProgress(
            String context,
            String phase,
            ScheduledThreadPoolExecutor reporter,
            long intervalMillis) {
        this(context, phase, reporter, intervalMillis, false);
    }

    private MaintenanceProgress(
            String context,
            String phase,
            ScheduledThreadPoolExecutor reporter,
            long intervalMillis,
            boolean shared) {
        this.shared = shared;
        this.context = context;
        this.phase = phase;
        report("START");
        heartbeat =
                reporter.scheduleAtFixedRate(
                        () -> report("PROGRESS"),
                        intervalMillis,
                        intervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    private static synchronized ScheduledThreadPoolExecutor acquireReporter() {
        if (sharedReporter == null) {
            sharedReporter = reporter();
        }
        reporterUsers++;
        return sharedReporter;
    }

    private static synchronized void releaseReporter() {
        if (--reporterUsers == 0) {
            // Do not leave a job classloader or reporter thread alive after maintenance finishes.
            sharedReporter.shutdown();
            sharedReporter = null;
        }
    }

    private static ScheduledThreadPoolExecutor reporter() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(
                        1,
                        runnable -> {
                            Thread thread = new Thread(runnable, "paimon-maintenance-progress");
                            thread.setDaemon(true);
                            return thread;
                        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    public void phase(String phase) {
        report("PHASE_END");
        this.phase = phase;
        phaseStarted = System.nanoTime();
        report("PHASE_START");
    }

    public void add(String counter, long value) {
        counters.computeIfAbsent(counter, key -> new AtomicLong()).addAndGet(value);
        if (value != 0) {
            lastProgress.set(System.nanoTime());
        }
    }

    /** Only active calls are retained; queued tasks do not consume tracking entries. */
    public long startItem(String description) {
        long id = sequence.incrementAndGet();
        active.put(id, new Item(description, System.nanoTime()));
        return id;
    }

    public void finishItem(long id) {
        active.remove(id);
        lastProgress.set(System.nanoTime());
    }

    public void complete() {
        completed = true;
    }

    void report(String event) {
        long now = System.nanoTime();
        Item oldest = null;
        for (Item item : active.values()) {
            if (oldest == null || item.started < oldest.started) {
                oldest = item;
            }
        }
        LOG.info(
                "Maintenance event={} operation_id={} context=[{}] phase=[{}] elapsed_ms={} phase_elapsed_ms={} "
                        + "since_last_progress_ms={} counters={} active_calls={} oldest_active=[{}] oldest_active_ms={}",
                event,
                operationId,
                context,
                phase,
                millis(now - started),
                millis(now - phaseStarted),
                millis(now - lastProgress.get()),
                counters,
                active.size(),
                oldest == null ? "none" : oldest.description,
                oldest == null ? 0 : millis(now - oldest.started));
    }

    private static long millis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            heartbeat.cancel(false);
            try {
                report(completed ? "END" : "ABORTED");
            } finally {
                if (shared) {
                    releaseReporter();
                }
            }
        }
    }

    private static final class Item {
        private final String description;
        private final long started;

        private Item(String description, long started) {
            this.description = description;
            this.started = started;
        }
    }
}
