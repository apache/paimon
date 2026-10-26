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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.Pair;

import com.aliyun.jindodata.common.JindoHadoopSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link JindoIOStats}. */
public class JindoIOStatsTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testStreamCounterPeak() {
        JindoIOStats.StreamCounter counter = new JindoIOStats.StreamCounter();
        counter.opened();
        counter.opened();
        counter.opened();
        counter.closed();
        counter.bytes(10);
        counter.bytes(-1);

        assertThat(counter.open()).isEqualTo(2);
        assertThat(counter.bytesTotal()).isEqualTo(10);
        assertThat(counter.takePeak()).isEqualTo(3);
        assertThat(counter.takePeak()).isEqualTo(2);
    }

    @Test
    public void testFileIOStreamsAreCounted() throws Exception {
        JindoHadoopSystem fs = mock(JindoHadoopSystem.class);
        when(fs.open(any(org.apache.hadoop.fs.Path.class)))
                .thenAnswer(i -> new FSDataInputStream(new SeekableBytes(new byte[100])));
        when(fs.create(any(org.apache.hadoop.fs.Path.class), anyBoolean()))
                .thenAnswer(i -> new FSDataOutputStream(new ByteArrayOutputStream(), null));
        StubFileIO fileIO = new StubFileIO(fs);
        Path path = new Path("oss://bucket/db/t/data.parquet");
        long inOpened = JindoIOStats.INPUT.openedTotal();
        long inClosed = JindoIOStats.INPUT.closedTotal();
        long read = JindoIOStats.INPUT.bytesTotal();
        long outOpened = JindoIOStats.OUTPUT.openedTotal();
        long outClosed = JindoIOStats.OUTPUT.closedTotal();
        long written = JindoIOStats.OUTPUT.bytesTotal();

        SeekableInputStream in = fileIO.newInputStream(path);
        assertThat(in.read(new byte[64], 0, 64)).isEqualTo(64);
        in.read();
        in.close();
        in.close();
        PositionOutputStream out = fileIO.newOutputStream(path, true);
        out.write(new byte[30]);
        out.write(new byte[30], 5, 20);
        out.write(1);
        out.close();
        out.close();

        assertThat(JindoIOStats.INPUT.openedTotal() - inOpened).isEqualTo(1);
        assertThat(JindoIOStats.INPUT.closedTotal() - inClosed).isEqualTo(1);
        assertThat(JindoIOStats.INPUT.bytesTotal() - read).isEqualTo(65);
        assertThat(JindoIOStats.OUTPUT.openedTotal() - outOpened).isEqualTo(1);
        assertThat(JindoIOStats.OUTPUT.closedTotal() - outClosed).isEqualTo(1);
        assertThat(JindoIOStats.OUTPUT.bytesTotal() - written).isEqualTo(51);
    }

    @Test
    public void testActivityIncludesCloses() {
        JindoIOStats.Reporter reporter =
                new JindoIOStats.Reporter(60_000, null, java.nio.file.Paths.get("/nonexistent"));
        reporter.hasActivity();
        assertThat(reporter.hasActivity()).isFalse();

        JindoIOStats.INPUT.opened();
        assertThat(reporter.hasActivity()).isTrue();
        assertThat(reporter.hasActivity()).isTrue();
        JindoIOStats.INPUT.closed();
        assertThat(reporter.hasActivity()).isTrue();
        assertThat(reporter.hasActivity()).isFalse();
    }

    @Test
    public void testParseNativeMetrics() throws IOException {
        String json =
                "[{\"name\":\"jindosdk_prefetch_cache_size\",\"help\":\"prefetch cache size\","
                        + "\"type\":\"gauge\",\"metrics\":[{\"labels\":{},\"value\":157286400}]},"
                        + "{\"name\":\"jindosdk_read_stream_open_count\",\"help\":\"\","
                        + "\"type\":\"counter\",\"metrics\":[{\"labels\":{},\"value\":8}]},"
                        + "{\"name\":\"jindosdk_getObject_latency\",\"help\":\"\","
                        + "\"type\":\"summary\",\"quantiles\":[0.5],\"metrics\":"
                        + "[{\"labels\":{},\"quantiles\":[1.0],\"count\":3,\"sum\":3.0}]}]";

        Map<String, Long> metrics = JindoIOStats.parseNativeMetrics(json);

        assertThat(metrics)
                .containsEntry("jindosdk_prefetch_cache_size", 157286400L)
                .containsEntry("jindosdk_read_stream_open_count", 8L)
                .containsEntry("jindosdk_getObject_latency", 0L);
    }

    @Test
    public void testCgroupV2() throws IOException {
        java.nio.file.Path root = tempDir.resolve("cgroup");
        java.nio.file.Path own = root.resolve("kubepods/pod1");
        Files.createDirectories(own);
        write(root.resolve("cgroup.controllers"), "memory");
        write(own.resolve("memory.current"), "2000000000");
        write(own.resolve("memory.max"), "2442133504");
        write(own.resolve("memory.peak"), "2100000000");
        write(own.resolve("memory.stat"), "anon 1700000000\ninactive_file 100000000\n");
        write(own.resolve("memory.events"), "low 0\nhigh 0\nmax 12\noom 1\noom_kill 1\n");
        java.nio.file.Path procCgroup =
                write(tempDir.resolve("cgroup-file"), "0::/kubepods/pod1\n");

        JindoIOStats.Cgroup cgroup = JindoIOStats.Cgroup.detect(procCgroup, root);

        assertThat(cgroup).isNotNull();
        assertThat(cgroup.limit()).isEqualTo(2442133504L);
        assertThat(cgroup.usage()).isEqualTo(2000000000L);
        assertThat(cgroup.workingSet()).isEqualTo(1900000000L);
        assertThat(cgroup.peak()).isEqualTo(2100000000L);
        assertThat(cgroup.limitHits()).isEqualTo(12);
        assertThat(cgroup.oomKills()).isEqualTo(1);
    }

    @Test
    public void testCgroupV1FallsBackToMountRoot() throws IOException {
        java.nio.file.Path root = tempDir.resolve("cgroup");
        java.nio.file.Path memory = root.resolve("memory");
        Files.createDirectories(memory);
        write(memory.resolve("memory.usage_in_bytes"), "1500000000");
        write(memory.resolve("memory.limit_in_bytes"), "9223372036854771712");
        write(memory.resolve("memory.max_usage_in_bytes"), "1600000000");
        write(memory.resolve("memory.failcnt"), "3");
        write(
                memory.resolve("memory.stat"),
                "hierarchical_memory_limit 2442133504\ntotal_inactive_file 500000000\n");
        write(
                memory.resolve("memory.oom_control"),
                "oom_kill_disable 0\nunder_oom 0\noom_kill 0\n");
        java.nio.file.Path procCgroup =
                write(tempDir.resolve("cgroup-file"), "4:cpu,cpuacct:/a\n9:memory:/kubepods/x\n");

        JindoIOStats.Cgroup cgroup = JindoIOStats.Cgroup.detect(procCgroup, root);

        assertThat(cgroup).isNotNull();
        assertThat(cgroup.limit()).isEqualTo(2442133504L);
        assertThat(cgroup.workingSet()).isEqualTo(1000000000L);
        assertThat(cgroup.peak()).isEqualTo(1600000000L);
        assertThat(cgroup.limitHits()).isEqualTo(3);
        assertThat(cgroup.oomKills()).isEqualTo(0);
    }

    @Test
    public void testNoCgroup() {
        assertThat(JindoIOStats.Cgroup.detect(tempDir.resolve("missing"), tempDir)).isNull();
    }

    @Test
    public void testAlarmLevels() throws IOException {
        java.nio.file.Path root = tempDir.resolve("cgroup");
        Files.createDirectories(root);
        write(root.resolve("cgroup.controllers"), "memory");
        write(root.resolve("memory.max"), "1000");
        write(root.resolve("memory.stat"), "inactive_file 0\n");
        write(root.resolve("memory.current"), "500");
        java.nio.file.Path procCgroup = write(tempDir.resolve("cgroup-file"), "0::/\n");
        JindoIOStats.Reporter reporter =
                new JindoIOStats.Reporter(
                        60_000, JindoIOStats.Cgroup.detect(procCgroup, root), tempDir);

        assertThat(reporter.checkAlarm()).isNull();
        write(root.resolve("memory.current"), "810");
        assertThat(reporter.checkAlarm())
                .startsWith("Container memory working set is at 81% of the limit.")
                .contains("Jindo IO stats: input open=")
                .contains("cgroup usage=0MiB/0MiB");
        write(root.resolve("memory.current"), "850");
        assertThat(reporter.checkAlarm()).isNull();
        write(root.resolve("memory.current"), "960");
        assertThat(reporter.checkAlarm()).contains("at 96% of the limit");
        write(root.resolve("memory.current"), "600");
        assertThat(reporter.checkAlarm()).isNull();
        write(root.resolve("memory.current"), "800");
        assertThat(reporter.checkAlarm()).contains("at 80% of the limit");
    }

    private static java.nio.file.Path write(java.nio.file.Path file, String content)
            throws IOException {
        return Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    }

    private static class StubFileIO extends HadoopCompliantFileIO {

        private final JindoHadoopSystem fs;

        private StubFileIO(JindoHadoopSystem fs) {
            this.fs = fs;
        }

        @Override
        public void configure(CatalogContext context) {}

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        protected Pair<JindoHadoopSystem, String> createFileSystem(
                org.apache.hadoop.fs.Path path, boolean enableCache) {
            return Pair.of(fs, "oss");
        }
    }

    private static class SeekableBytes extends ByteArrayInputStream
            implements Seekable, PositionedReadable {

        private SeekableBytes(byte[] bytes) {
            super(bytes);
        }

        @Override
        public void seek(long pos) {
            this.pos = (int) pos;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) {
            int n = Math.min(length, count - (int) position);
            System.arraycopy(buf, (int) position, buffer, offset, n);
            return n;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) {
            read(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer) {
            read(position, buffer, 0, buffer.length);
        }
    }
}
