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

package org.apache.paimon.jindo;

import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import com.aliyun.jindodata.common.JindoConstant;
import com.aliyun.jindodata.common.JindoHadoopSystem;
import com.aliyun.jindodata.store.JindoMetricsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JVM-wide counters of the Jindo streams of FileIOs that enable them, logged periodically with
 * Jindo native, process and container memory; a WARN is logged when the container working set nears
 * its limit.
 */
public final class JindoIOStats {

    private static final Logger LOG = LoggerFactory.getLogger(JindoIOStats.class);

    static final StreamCounter INPUT = new StreamCounter();
    static final StreamCounter OUTPUT = new StreamCounter();

    private static final AtomicLong INTERVAL_MILLIS = new AtomicLong();
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static volatile Pair<JindoHadoopSystem, org.apache.hadoop.fs.Path> nativeSource;

    private JindoIOStats() {}

    /**
     * Counts a stream just opened by a FileIO with this interval and returns its counter, or null
     * when the interval is null, zero or negative. The first positive interval in the JVM is used.
     */
    @Nullable
    static StreamCounter track(StreamCounter counter, @Nullable Duration interval) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            return null;
        }
        INTERVAL_MILLIS.compareAndSet(0, Math.max(1, interval.toMillis()));
        counter.opened();
        startReporter();
        return counter;
    }

    private static void startReporter() {
        if (RUNNING.get() || !RUNNING.compareAndSet(false, true)) {
            return;
        }
        Reporter reporter =
                new Reporter(
                        INTERVAL_MILLIS.get(),
                        Cgroup.detect(Paths.get("/proc/self/cgroup"), Paths.get("/sys/fs/cgroup")),
                        Paths.get("/proc/self"),
                        RUNNING);
        Thread thread = new Thread(reporter::run, "paimon-jindo-io-stats");
        thread.setDaemon(true);
        // the inherited loader may belong to the job that opened the stream, keep it collectable
        thread.setContextClassLoader(JindoIOStats.class.getClassLoader());
        thread.start();
    }

    /** Registers a file system that is never closed, used to dump the Jindo native metrics. */
    static void registerNativeMetrics(JindoHadoopSystem fs, org.apache.hadoop.fs.Path path) {
        if (nativeSource == null) {
            nativeSource = Pair.of(fs, path);
        }
    }

    /** Open, close and byte counters of one stream direction. */
    static final class StreamCounter {

        private final AtomicLong opened = new AtomicLong();
        private final AtomicLong closed = new AtomicLong();
        private final AtomicLong active = new AtomicLong();
        private final AtomicLong peak = new AtomicLong();
        private final LongAdder bytes = new LongAdder();

        void opened() {
            opened.incrementAndGet();
            peak.accumulateAndGet(active.incrementAndGet(), Math::max);
        }

        void closed() {
            closed.incrementAndGet();
            active.decrementAndGet();
        }

        void bytes(long n) {
            if (n > 0) {
                bytes.add(n);
            }
        }

        long open() {
            return active.get();
        }

        long openedTotal() {
            return opened.get();
        }

        long closedTotal() {
            return closed.get();
        }

        long bytesTotal() {
            return bytes.sum();
        }

        /**
         * Returns the peak since the previous call, at least {@code open}, and restarts it from the
         * current count without losing a concurrent update.
         */
        long takePeak(long open) {
            while (true) {
                long previous = peak.get();
                if (peak.compareAndSet(previous, active.get())) {
                    return Math.max(previous, open);
                }
            }
        }
    }

    /** Samples memory every second and logs the statistics until the streams go idle. */
    static final class Reporter {

        private static final long SAMPLE_MILLIS = 1000;
        private static final double[] ALARM_RATIOS = {0.8, 0.9, 0.95};
        private static final double ALARM_RESET_RATIO = 0.7;

        private final long intervalMillis;
        private final List<Cgroup> cgroups;
        private final Path procSelf;
        private final AtomicBoolean running;

        @Nullable private JindoMetricsStore nativeStore;
        private boolean nativeDisabled;
        @Nullable private String jemalloc;
        private int alarmLevel;
        private long lastEvents;

        Reporter(long intervalMillis, List<Cgroup> cgroups, Path procSelf, AtomicBoolean running) {
            this.intervalMillis = intervalMillis;
            this.cgroups = cgroups;
            this.procSelf = procSelf;
            this.running = running;
        }

        /** Runs until an interval passes without stream activity; the next open starts another. */
        void run() {
            long sampleMillis = Math.min(SAMPLE_MILLIS, intervalMillis);
            long nextReport = System.currentTimeMillis() + intervalMillis;
            while (true) {
                try {
                    Thread.sleep(sampleMillis);
                } catch (InterruptedException e) {
                    running.set(false);
                    return;
                }
                try {
                    String alarm = checkAlarm();
                    if (alarm != null) {
                        LOG.warn(alarm);
                    }
                    long now = System.currentTimeMillis();
                    if (now >= nextReport) {
                        nextReport = now + intervalMillis;
                        if (hasActivity()) {
                            LOG.info(snapshot());
                        } else if (stop()) {
                            return;
                        }
                    }
                } catch (Throwable t) {
                    LOG.debug("Failed to collect Jindo IO stats.", t);
                }
            }
        }

        /** Returns a WARN message when the working set enters a higher alarm level. */
        @Nullable
        String checkAlarm() {
            Cgroup cgroup = Cgroup.closestToLimit(cgroups);
            if (cgroup == null) {
                return null;
            }
            long limit = cgroup.limit();
            long workingSet = cgroup.workingSet();
            if (limit <= 0 || workingSet < 0) {
                return null;
            }
            double ratio = (double) workingSet / limit;
            if (ratio < ALARM_RESET_RATIO) {
                alarmLevel = 0;
            }
            int level = 0;
            while (level < ALARM_RATIOS.length && ratio >= ALARM_RATIOS[level]) {
                level++;
            }
            if (level <= alarmLevel) {
                return null;
            }
            alarmLevel = level;
            return String.format(
                    "Container memory working set is at %d%% of the limit. %s",
                    Math.round(ratio * 100), snapshot());
        }

        /** True while streams are open or when any stream opened or closed since last call. */
        boolean hasActivity() {
            long events = events();
            boolean active = events != lastEvents || INPUT.open() > 0 || OUTPUT.open() > 0;
            lastEvents = events;
            return active;
        }

        /**
         * Called after an idle {@link #hasActivity()}; returns true when stopped. Opens since then
         * saw it running and started no other reporter, so their events keep it running.
         */
        boolean stop() {
            running.set(false);
            boolean missed = events() != lastEvents || INPUT.open() > 0 || OUTPUT.open() > 0;
            return !missed || !running.compareAndSet(false, true);
        }

        private static long events() {
            return INPUT.openedTotal()
                    + INPUT.closedTotal()
                    + OUTPUT.openedTotal()
                    + OUTPUT.closedTotal();
        }

        String snapshot() {
            StringBuilder sb = new StringBuilder("Jindo IO stats:");
            appendStreams(sb, "input", INPUT, "read");
            appendStreams(sb, "output", OUTPUT, "written");
            appendNative(sb);
            appendProcess(sb);
            appendCgroup(sb);
            return sb.toString();
        }

        private static void appendStreams(
                StringBuilder sb, String name, StreamCounter counter, String bytesName) {
            long open = counter.open();
            sb.append(' ')
                    .append(name)
                    .append(" open=")
                    .append(open)
                    .append(" peak=")
                    .append(counter.takePeak(open))
                    .append(" opened=")
                    .append(counter.openedTotal())
                    .append(" closed=")
                    .append(counter.closedTotal())
                    .append(' ')
                    .append(bytesName)
                    .append('=')
                    .append(mib(counter.bytesTotal()))
                    .append(';');
        }

        private void appendNative(StringBuilder sb) {
            Map<String, Long> metrics = nativeMetrics();
            if (metrics == null) {
                sb.append(" jindo unavailable;");
                return;
            }
            long memInsufficient = 0;
            for (Map.Entry<String, Long> e : metrics.entrySet()) {
                if (e.getKey().startsWith("jindosdk_prefetch_fallback_mem_insufficient")) {
                    memInsufficient += e.getValue();
                }
            }
            sb.append(" jindo prefetchCache=")
                    .append(mib(get(metrics, "jindosdk_prefetch_cache_size", 0)))
                    .append('/')
                    .append(mib(get(metrics, "jindosdk_prefetch_cache_capacity", -1)))
                    .append(" readStreams=")
                    .append(
                            get(metrics, "jindosdk_read_stream_open_count", 0)
                                    - get(metrics, "jindosdk_read_stream_close_count", 0))
                    .append(" writeStreams=")
                    .append(
                            get(metrics, "jindosdk_write_stream_open_count", 0)
                                    - get(metrics, "jindosdk_write_stream_close_count", 0))
                    .append(" memInsufficient=")
                    .append(memInsufficient)
                    .append(';');
        }

        @Nullable
        private Map<String, Long> nativeMetrics() {
            Pair<JindoHadoopSystem, org.apache.hadoop.fs.Path> source = nativeSource;
            if (nativeDisabled || source == null) {
                return null;
            }
            try {
                if (nativeStore == null) {
                    nativeStore = source.getLeft().getMetricsStore(source.getRight());
                    if (nativeStore == null) {
                        nativeDisabled = true;
                        return null;
                    }
                }
                return parseNativeMetrics(
                        nativeStore.dumpMetrics(
                                JindoConstant.MetricType.STATIC_ONLY,
                                JindoConstant.MetricFormat.JSON));
            } catch (Throwable t) {
                nativeDisabled = true;
                LOG.debug("Jindo native metrics are unavailable.", t);
                return null;
            }
        }

        private void appendProcess(StringBuilder sb) {
            Map<String, Long> status = readStatus(procSelf.resolve("status"));
            MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            sb.append(" process rss=")
                    .append(mib(status.getOrDefault("VmRSS", -1L)))
                    .append(" anon=")
                    .append(mib(status.getOrDefault("RssAnon", -1L)))
                    .append(" file=")
                    .append(mib(status.getOrDefault("RssFile", -1L)))
                    .append(" shmem=")
                    .append(mib(status.getOrDefault("RssShmem", -1L)))
                    .append(" threads=")
                    .append(status.getOrDefault("Threads", -1L))
                    .append(" heap=")
                    .append(mib(heap.getUsed()))
                    .append('/')
                    .append(mib(heap.getCommitted()))
                    .append('/')
                    .append(mib(heap.getMax()))
                    .append(" direct=")
                    .append(mib(directUsed()))
                    .append(" metaspace=")
                    .append(mib(metaspaceUsed()))
                    .append(" jemalloc=")
                    .append(jemalloc())
                    .append(';');
        }

        private void appendCgroup(StringBuilder sb) {
            Cgroup cgroup = Cgroup.closestToLimit(cgroups);
            if (cgroup == null) {
                sb.append(" cgroup unavailable");
                return;
            }
            sb.append(" cgroup usage=")
                    .append(mib(cgroup.usage()))
                    .append('/')
                    .append(mib(cgroup.limit()))
                    .append(" workingSet=")
                    .append(mib(cgroup.workingSet()))
                    .append(" peak=")
                    .append(mib(cgroup.peak()))
                    .append(" limitHits=")
                    .append(cgroup.limitHits())
                    .append(" oomKills=")
                    .append(cgroup.oomKills());
        }

        private String jemalloc() {
            if (jemalloc == null) {
                try {
                    jemalloc = "false";
                    for (String line : Files.readAllLines(procSelf.resolve("maps"))) {
                        if (line.contains("/libjemalloc")) {
                            jemalloc = "true";
                            break;
                        }
                    }
                } catch (IOException e) {
                    jemalloc = "unknown";
                }
            }
            return jemalloc;
        }

        private static long directUsed() {
            for (BufferPoolMXBean pool :
                    ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
                if ("direct".equals(pool.getName())) {
                    return pool.getMemoryUsed();
                }
            }
            return -1;
        }

        private static long metaspaceUsed() {
            for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
                if ("Metaspace".equals(pool.getName())) {
                    return pool.getUsage().getUsed();
                }
            }
            return -1;
        }

        /** Reads /proc/self/status; sizes in bytes, counts as is. */
        private static Map<String, Long> readStatus(Path file) {
            Map<String, Long> values = new HashMap<>();
            try {
                for (String line : Files.readAllLines(file)) {
                    int colon = line.indexOf(':');
                    if (colon < 0) {
                        continue;
                    }
                    String[] parts = line.substring(colon + 1).trim().split("\\s+");
                    try {
                        long value = Long.parseLong(parts[0]);
                        boolean kb = parts.length > 1 && "kB".equals(parts[1]);
                        values.put(line.substring(0, colon), kb ? value * 1024 : value);
                    } catch (NumberFormatException ignored) {
                    }
                }
            } catch (IOException ignored) {
            }
            return values;
        }

        private static long get(Map<String, Long> metrics, String name, long defaultValue) {
            return metrics.getOrDefault(name, defaultValue);
        }

        private static String mib(long bytes) {
            return bytes < 0 ? "?" : (bytes >> 20) + "MiB";
        }
    }

    /** Sums the values of each metric in a Jindo JSON metrics dump. */
    static Map<String, Long> parseNativeMetrics(String json) throws IOException {
        Map<String, Long> values = new HashMap<>();
        for (JsonNode metric : JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(json)) {
            long sum = 0;
            for (JsonNode m : metric.path("metrics")) {
                sum += m.path("value").asLong();
            }
            values.merge(metric.path("name").asText(), sum, Long::sum);
        }
        return values;
    }

    /** One memory cgroup (v1 or v2) level of this process; unreadable values are -1. */
    static final class Cgroup {

        private static final Pattern V1_MEMORY =
                Pattern.compile("^\\d+:(?:[^:]*,)?memory(?:,[^:]*)?:(.*)$");

        private final Path dir;
        private final boolean v2;

        private Cgroup(Path dir, boolean v2) {
            this.dir = dir;
            this.v2 = v2;
        }

        /**
         * Returns the levels from the process's own cgroup up to the mount root when it is visible,
         * else the mount root alone (container view); empty when there is no memory cgroup.
         */
        static List<Cgroup> detect(Path procSelfCgroup, Path root) {
            List<String> lines;
            try {
                lines = Files.readAllLines(procSelfCgroup);
            } catch (IOException e) {
                return Collections.emptyList();
            }
            boolean v2 = Files.exists(root.resolve("cgroup.controllers"));
            Path mount = (v2 ? root : root.resolve("memory")).normalize();
            for (String line : lines) {
                String relative = null;
                if (v2 && line.startsWith("0::")) {
                    relative = line.substring(3);
                } else if (!v2) {
                    Matcher matcher = V1_MEMORY.matcher(line);
                    if (matcher.matches()) {
                        relative = matcher.group(1);
                    }
                }
                if (relative != null) {
                    Path own = resolve(mount, relative).normalize();
                    if (own.startsWith(mount) && Files.exists(own.resolve(usageFile(v2)))) {
                        return levels(mount, own, v2);
                    }
                }
            }
            return levels(mount, mount, v2);
        }

        /**
         * Limits are hierarchical: the level whose working set is closest to its limit, or the
         * leaf.
         */
        @Nullable
        static Cgroup closestToLimit(List<Cgroup> levels) {
            Cgroup closest = null;
            double closestRatio = -1;
            for (Cgroup level : levels) {
                long limit = level.limit();
                long workingSet = level.workingSet();
                if (limit > 0 && workingSet >= 0 && (double) workingSet / limit > closestRatio) {
                    closest = level;
                    closestRatio = (double) workingSet / limit;
                }
            }
            if (closest == null && !levels.isEmpty()) {
                return levels.get(0);
            }
            return closest;
        }

        private static List<Cgroup> levels(Path mount, Path leaf, boolean v2) {
            List<Cgroup> levels = new ArrayList<>();
            for (Path dir = leaf; dir != null && dir.startsWith(mount); dir = dir.getParent()) {
                if (Files.exists(dir.resolve(usageFile(v2)))) {
                    levels.add(new Cgroup(dir, v2));
                }
            }
            return levels;
        }

        private static String usageFile(boolean v2) {
            return v2 ? "memory.current" : "memory.usage_in_bytes";
        }

        private static Path resolve(Path root, String relative) {
            String trimmed = relative.startsWith("/") ? relative.substring(1) : relative;
            return trimmed.isEmpty() ? root : root.resolve(trimmed);
        }

        long limit() {
            if (v2) {
                return readLong("memory.max");
            }
            long limit = readLong("memory.limit_in_bytes");
            long hierarchical = readKey("memory.stat", "hierarchical_memory_limit");
            if (hierarchical > 0 && (limit < 0 || hierarchical < limit)) {
                limit = hierarchical;
            }
            return limit >= Long.MAX_VALUE / 2 ? -1 : limit;
        }

        long usage() {
            return readLong(usageFile(v2));
        }

        /** Usage minus inactive file pages, the part the kernel cannot reclaim cheaply. */
        long workingSet() {
            long usage = usage();
            long inactiveFile =
                    readKey("memory.stat", v2 ? "inactive_file" : "total_inactive_file");
            return usage < 0 ? -1 : Math.max(0, usage - Math.max(0, inactiveFile));
        }

        long peak() {
            return readLong(v2 ? "memory.peak" : "memory.max_usage_in_bytes");
        }

        long limitHits() {
            return v2 ? readKey("memory.events", "max") : readLong("memory.failcnt");
        }

        long oomKills() {
            return readKey(v2 ? "memory.events" : "memory.oom_control", "oom_kill");
        }

        private long readLong(String file) {
            try {
                String value = new String(Files.readAllBytes(dir.resolve(file))).trim();
                return "max".equals(value) ? -1 : Long.parseLong(value);
            } catch (IOException | NumberFormatException e) {
                return -1;
            }
        }

        private long readKey(String file, String key) {
            try {
                for (String line : Files.readAllLines(dir.resolve(file))) {
                    int space = line.indexOf(' ');
                    if (space > 0 && line.substring(0, space).equals(key)) {
                        return Long.parseLong(line.substring(space + 1).trim());
                    }
                }
            } catch (IOException | NumberFormatException ignored) {
            }
            return -1;
        }
    }
}
