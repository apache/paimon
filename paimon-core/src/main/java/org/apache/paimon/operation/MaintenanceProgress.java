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
    private static final AtomicLong REPORTER_THREADS_CREATED = new AtomicLong();

    private final String operationId = UUID.randomUUID().toString();
    private final String context;
    private final long started = System.nanoTime();
    private final AtomicLong lastProgress = new AtomicLong(started);
    private final AtomicLong sequence = new AtomicLong();
    private final Map<String, AtomicLong> counters = new ConcurrentSkipListMap<>();
    private final Map<Long, Item> active = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> heartbeat;
    private final AtomicBoolean reported = new AtomicBoolean();
    private volatile String phase;
    private volatile long phaseStarted = started;
    private boolean completed;
    private final AtomicBoolean closed = new AtomicBoolean();

    public MaintenanceProgress(String context, String phase) {
        this(context, phase, sharedReporter(), TimeUnit.SECONDS.toMillis(60), true);
    }

    /** Creates progress diagnostics which stay quiet unless the operation reaches a heartbeat. */
    public static MaintenanceProgress slowOperation(String context, String phase) {
        return new MaintenanceProgress(
                context, phase, sharedReporter(), TimeUnit.SECONDS.toMillis(60), false);
    }

    static MaintenanceProgress slowOperation(
            String context,
            String phase,
            ScheduledThreadPoolExecutor reporter,
            long intervalMillis) {
        return new MaintenanceProgress(context, phase, reporter, intervalMillis, false);
    }

    MaintenanceProgress(
            String context,
            String phase,
            ScheduledThreadPoolExecutor reporter,
            long intervalMillis) {
        this(context, phase, reporter, intervalMillis, true);
    }

    private MaintenanceProgress(
            String context,
            String phase,
            ScheduledThreadPoolExecutor reporter,
            long intervalMillis,
            boolean reportImmediately) {
        this.context = context;
        this.phase = phase;
        if (reportImmediately) {
            report("START");
        }
        heartbeat =
                LOG.isInfoEnabled()
                        ? reporter.scheduleAtFixedRate(
                                () -> {
                                    if (!closed.get()) {
                                        report("PROGRESS");
                                    }
                                },
                                intervalMillis,
                                intervalMillis,
                                TimeUnit.MILLISECONDS)
                        : null;
    }

    private static synchronized ScheduledThreadPoolExecutor sharedReporter() {
        if (sharedReporter == null) {
            sharedReporter = reporter();
        }
        return sharedReporter;
    }

    private static ScheduledThreadPoolExecutor reporter() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(
                        1,
                        runnable -> {
                            REPORTER_THREADS_CREATED.incrementAndGet();
                            Thread thread = new Thread(runnable, "paimon-maintenance-progress");
                            thread.setDaemon(true);
                            return thread;
                        });
        executor.setRemoveOnCancelPolicy(true);
        // Stay alive across bursts of maintenance calls, but release the classloader root when
        // there are no scheduled heartbeats. Keep-alive exceeds the heartbeat interval so a slow
        // operation always has a worker available when its first report becomes due.
        executor.setKeepAliveTime(2, TimeUnit.MINUTES);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    static long reporterThreadsCreated() {
        return REPORTER_THREADS_CREATED.get();
    }

    public void phase(String phase) {
        synchronized (this) {
            if (closed.get()) {
                return;
            }
            if (reported.get()) {
                reportInternal("PHASE_END");
            }
            this.phase = phase;
            phaseStarted = System.nanoTime();
            if (reported.get()) {
                reportInternal("PHASE_START");
            }
        }
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
        synchronized (this) {
            if (closed.get()) {
                return;
            }
            reportInternal(event);
        }
    }

    private void reportInternal(String event) {
        reported.set(true);
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
        if (heartbeat != null) {
            heartbeat.cancel(false);
        }
        synchronized (this) {
            if (closed.compareAndSet(false, true) && reported.get()) {
                reportInternal(completed ? "END" : "ABORTED");
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
