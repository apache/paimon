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
import org.apache.paimon.jindo.IoCacheRouting.CacheTarget;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Pair;

import com.aliyun.jindodata.api.spec.JdoException;
import com.aliyun.jindodata.common.JindoHadoopSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the cache endpoints of {@link JindoFileIO}. */
public class JindoIoCacheRoutingTest {

    private static final String TABLE = "oss://bkt/db1.db/t1";
    private static final String UUID = "8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b";
    private static final Path DATA = new Path(TABLE + "/dt=1/bucket-0/data-" + UUID + "-1.orc");
    private static final Path MANIFEST = new Path(TABLE + "/manifest/manifest-1");
    private static final Path SNAPSHOT = new Path(TABLE + "/snapshot/snapshot-2");
    private static final Path LATEST = new Path(TABLE + "/snapshot/LATEST");
    private static final String ACCEL_HOST = "cn-hangzhou-j-internal.oss-data-acc.aliyuncs.com";
    private static final String CLUSTER_HOST = "10.0.0.1:8080";
    private static final String OSS_HOST = "oss-cn-hangzhou-internal.aliyuncs.com";
    private static final String DLF_CACHE_KEY = "fs.oss.dlf-cache.consistent-hash.enabled";

    private JindoHadoopSystem accelFs;
    private JindoHadoopSystem clusterFs;
    private JindoHadoopSystem ossFs;

    @BeforeEach
    public void before() throws IOException {
        accelFs = mockFileSystem();
        clusterFs = mockFileSystem();
        ossFs = mockFileSystem();
    }

    @Test
    public void testOptionsOfEachCacheTarget() {
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(cacheTargetOptions()));

        Options cluster = fileIO.hadoopOptions(DATA, "read");
        assertThat(cluster.get("fs.oss.endpoint")).isEqualTo(CLUSTER_HOST);
        assertThat(cluster.get("fs.oss.https.enable")).isEqualTo("false");
        assertThat(cluster.get("fs.oss.second.level.domain.enable")).isEqualTo("true");
        assertThat(cluster.get("fs.oss.region")).isEqualTo("cn-hangzhou");
        assertThat(cluster.get(DLF_CACHE_KEY)).isEqualTo("true");
        assertThat(cluster.containsKey("fs.xengine")).isFalse();
        assertThat(fileIO.hadoopOptions(DATA, "meta")).isSameAs(cluster);

        Options accel = fileIO.hadoopOptions(MANIFEST, "meta");
        assertThat(accel.get("fs.oss.endpoint")).isEqualTo(ACCEL_HOST);
        assertThat(accel.get("fs.oss.https.enable")).isEqualTo("true");
        assertThat(accel.get("fs.oss.second.level.domain.enable")).isEqualTo("false");
        assertThat(accel.get("fs.oss.region")).isEqualTo("cn-shanghai");

        Options oss = fileIO.hadoopOptions(DATA, "write");
        assertThat(oss.get("fs.oss.endpoint")).isEqualTo(OSS_HOST);
        assertThat(oss.get("fs.oss.https.enable")).isEqualTo("true");
        assertThat(oss.get("fs.oss.region")).isEqualTo("cn-hangzhou");
        assertThat(oss.containsKey(DLF_CACHE_KEY)).isFalse();
        for (Path path :
                new Path[] {SNAPSHOT, LATEST, new Path(TABLE + "/dt=1/bucket-0/000000_0")}) {
            assertThat(fileIO.hadoopOptions(path, "read")).as(path.toString()).isSameAs(oss);
        }
        assertThat(fileIO.hadoopOptions(DATA, "list")).isSameAs(oss);
    }

    @Test
    public void testOnlyReadsAndStatusUseIoCacheRouting() throws IOException {
        JindoFileIO fileIO = configuredFileIO(cacheTargetOptions());
        Path copy = new Path(TABLE + "/dt=1/bucket-0/data-" + UUID + "-2.orc");

        fileIO.newInputStream(DATA).close();
        fileIO.getFileSize(DATA);
        fileIO.getFileStatus(MANIFEST);
        fileIO.exists(DATA);
        fileIO.exists(MANIFEST);
        verify(clusterFs).open(any(org.apache.hadoop.fs.Path.class));
        verify(clusterFs).getFileStatus(any());
        verify(clusterFs).exists(any());
        verify(accelFs).getFileStatus(any());
        verify(accelFs).exists(any());

        fileIO.exists(SNAPSHOT);
        fileIO.newInputStream(SNAPSHOT).close();
        fileIO.getFileStatus(SNAPSHOT);
        fileIO.newOutputStream(DATA, false);
        fileIO.listStatus(DATA.getParent());
        fileIO.delete(DATA, false);
        fileIO.mkdirs(DATA.getParent());
        fileIO.rename(DATA, copy);
        fileIO.copyFile(DATA, copy, true);
        verify(ossFs).exists(any());
        verify(ossFs, times(2)).open(any(org.apache.hadoop.fs.Path.class));
        verify(ossFs).getFileStatus(any());
        verify(ossFs, times(2)).create(any(), anyBoolean());
        verify(ossFs).listStatus(any(org.apache.hadoop.fs.Path.class));
        verify(ossFs).delete(any(), anyBoolean());
        verify(ossFs).mkdirs(any());
        verify(ossFs).rename(any(), any());

        // the two-phase write checks a routable data file on OSS, not on its cache target
        fileIO.tryToWriteAtomic(new Path(TABLE + "/snapshot/snapshot-3"), "{}");
        fileIO.newTwoPhaseOutputStream(copy, false);
        verify(ossFs, times(4)).create(any(), anyBoolean());
        verify(ossFs, times(2)).exists(any());
        verify(ossFs, times(2)).rename(any(), any());
        verify(ossFs).getMpuStore(any());

        verify(clusterFs).exists(any());
        for (JindoHadoopSystem fs : new JindoHadoopSystem[] {accelFs, clusterFs}) {
            verify(fs, never()).create(any(), anyBoolean());
            verify(fs, never()).listStatus(any(org.apache.hadoop.fs.Path.class));
            verify(fs, never()).rename(any(), any());
        }
    }

    @Test
    public void testCacheTargetErrorsAreThrown() throws IOException {
        JindoFileIO fileIO = configuredFileIO(cacheTargetOptions());
        IOException unavailable = jindoError(6503, "open failed: 503 Service Unavailable");
        FileNotFoundException notFound = new FileNotFoundException("404");
        when(clusterFs.open(any(org.apache.hadoop.fs.Path.class))).thenThrow(unavailable);
        when(accelFs.getFileStatus(any())).thenThrow(notFound);

        assertThatThrownBy(() -> fileIO.newInputStream(DATA)).isSameAs(unavailable);
        assertThatThrownBy(() -> fileIO.getFileStatus(MANIFEST)).isSameAs(notFound);
        verify(ossFs, never()).open(any(org.apache.hadoop.fs.Path.class));
        verify(ossFs, never()).getFileStatus(any());
    }

    @Test
    public void testCacheTargetInitializationErrorsStayChecked() throws IOException {
        SocketTimeoutException timeout = new SocketTimeoutException("connect timed out");
        JindoFileIO fileIO =
                new TestingJindoFileIO(accelFs, clusterFs, ossFs) {
                    @Override
                    Pair<JindoHadoopSystem, String> createFileSystem(
                            org.apache.hadoop.fs.Path path, CacheTarget target) {
                        // JindoFileIO wraps an initialization IOException the same way
                        throw new UncheckedIOException(timeout);
                    }
                };
        fileIO.configure(CatalogContext.create(cacheTargetOptions()));

        assertThatThrownBy(() -> fileIO.newInputStream(DATA)).isSameAs(timeout);
        assertThatThrownBy(() -> fileIO.getFileStatus(MANIFEST)).isSameAs(timeout);
        assertThatThrownBy(() -> fileIO.exists(MANIFEST)).isSameAs(timeout);
        verify(ossFs, never()).open(any(org.apache.hadoop.fs.Path.class));
    }

    @Test
    public void testSingleCacheTarget() {
        Options options = baseOptions();
        options.set("io-cache.endpoint", "http://" + CLUSTER_HOST);
        options.set("io-cache.target.default.path-style-access", "true");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));

        Options read = fileIO.hadoopOptions(DATA, "read");
        assertThat(read.get("fs.oss.endpoint")).isEqualTo(CLUSTER_HOST);
        assertThat(read.get("fs.oss.second.level.domain.enable")).isEqualTo("true");
        assertThat(fileIO.hadoopOptions(MANIFEST, "meta")).isSameAs(read);
        assertThat(fileIO.hadoopOptions(DATA, "write").get("fs.oss.endpoint")).isEqualTo(OSS_HOST);
    }

    @Test
    public void testJindoCacheTakesPrecedence() {
        Options options = cacheTargetOptions();
        options.set("fs.jindocache.namespace.rpc.address", "rpc:8101");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));

        assertThat(fileIO.cacheRouting).isNull();
        Options read = fileIO.hadoopOptions(DATA, "read");
        assertThat(read.get("fs.xengine")).isEqualTo("jindocache");
        assertThat(read.get("fs.oss.endpoint")).isEqualTo("http://" + CLUSTER_HOST);
        Options write = fileIO.hadoopOptions(DATA, "write");
        assertThat(write.containsKey("fs.xengine")).isFalse();
        // legacy whitelist-path matching: "manifest" is in the default whitelist
        assertThat(fileIO.hadoopOptions(MANIFEST, "meta")).isSameAs(read);
        assertThat(fileIO.hadoopOptions(LATEST, "read")).isSameAs(write);
    }

    @Test
    public void testWithoutIoCacheRouting() {
        Options override = cacheTargetOptions();
        override.set("dlf.oss-endpoint", "oss-cn-hangzhou.aliyuncs.com");
        override.set("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        Options notEnabled = cacheTargetOptions();
        notEnabled.remove("io-cache.enabled");
        Options noPolicy = cacheTargetOptions();
        noPolicy.set("io-cache.policy", "write");

        for (Options options : new Options[] {override, notEnabled, noPolicy}) {
            JindoFileIO fileIO = new JindoFileIO();
            fileIO.configure(CatalogContext.create(options));
            assertThat(fileIO.cacheRouting).isNull();
            for (String op : new String[] {"read", "meta", "write"}) {
                Options hadoopOptions = fileIO.hadoopOptions(DATA, op);
                assertThat(hadoopOptions.get("fs.oss.endpoint"))
                        .isEqualTo(options.get("fs.oss.endpoint"));
                assertThat(hadoopOptions.containsKey("fs.oss.https.enable")).isFalse();
            }
        }
    }

    @Test
    public void testWithEndpoint() {
        Options base = new Options();
        base.set("fs.oss.https.enable", "false");

        Options bare = JindoFileIO.withEndpoint(base, "oss.example.com");
        assertThat(bare.get("fs.oss.endpoint")).isEqualTo("oss.example.com");
        assertThat(bare.get("fs.oss.https.enable")).isEqualTo("false");

        Options https = JindoFileIO.withEndpoint(base, "HTTPS://cache.example.com:443/");
        assertThat(https.get("fs.oss.endpoint")).isEqualTo("cache.example.com:443");
        assertThat(https.get("fs.oss.https.enable")).isEqualTo("true");

        Options http = JindoFileIO.withEndpoint(base, "http://10.0.0.1:8080");
        assertThat(http.get("fs.oss.endpoint")).isEqualTo("10.0.0.1:8080");
        assertThat(http.get("fs.oss.https.enable")).isEqualTo("false");

        assertThat(JindoFileIO.withEndpoint(base, null).toMap()).isEqualTo(base.toMap());
    }

    @Test
    public void testFileSystemPerCacheTargetAndBucket() throws IOException {
        List<String> created = new ArrayList<>();
        JindoFileIO fileIO =
                new JindoFileIO() {
                    @Override
                    Pair<JindoHadoopSystem, String> createFileSystem(
                            org.apache.hadoop.fs.Path path, CacheTarget target) {
                        created.add(target.name + "/" + path.toUri().getAuthority());
                        return Pair.of(mock(JindoHadoopSystem.class), "oss");
                    }
                };
        fileIO.configure(CatalogContext.create(cacheTargetOptions()));

        Pair<JindoHadoopSystem, String> first =
                fileIO.getFileSystemPair(hadoopPath(TABLE + "/manifest/manifest-a"), true);
        assertThat(fileIO.getFileSystemPair(hadoopPath(TABLE + "/manifest/manifest-b"), true))
                .isSameAs(first);
        fileIO.getFileSystemPair(hadoopPath("oss://other/t/manifest/manifest-c"), true);
        fileIO.getFileSystemPair(
                hadoopPath(TABLE + "/dt=1/bucket-0/data-" + UUID + "-3.orc"), true);
        assertThat(created).containsExactly("accel/bkt", "accel/other", "cluster/bkt");
    }

    @Test
    public void testCloseAfterOnlyCacheReads() throws IOException {
        Options options = cacheTargetOptions();
        options.set(FILE_IO_ALLOW_CACHE, false);
        JindoFileIO fileIO = new TestingJindoFileIO(accelFs, clusterFs, ossFs);
        fileIO.configure(CatalogContext.create(options));
        fileIO.newInputStream(DATA).close();

        fileIO.close();

        verify(clusterFs).close();
    }

    @Test
    public void testCloseClosesCacheTargetFileSystems() throws IOException {
        Options options = cacheTargetOptions();
        options.set(FILE_IO_ALLOW_CACHE, false);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));
        fileIO.fsMap = new ConcurrentHashMap<>();
        fileIO.fsMap.put("bkt", Pair.of(ossFs, "oss"));
        fileIO.cacheTargetFsMap = new ConcurrentHashMap<>();
        fileIO.cacheTargetFsMap.put("accel/bkt", Pair.of(accelFs, "oss"));
        fileIO.cacheTargetFsMap.put("cluster/bkt", Pair.of(clusterFs, "oss"));

        fileIO.close();

        for (JindoHadoopSystem fs : new JindoHadoopSystem[] {ossFs, accelFs, clusterFs}) {
            verify(fs).close();
        }
        assertThat(fileIO.cacheTargetFsMap).isEmpty();
    }

    private static Options baseOptions() {
        Options options = new Options();
        options.set("fs.oss.endpoint", "http://" + CLUSTER_HOST);
        options.set("fs.oss.accessKeyId", "ak");
        options.set("fs.oss.accessKeySecret", "sk");
        options.set("fs.oss.region", "cn-hangzhou");
        options.set("io-cache.enabled", "true");
        options.set("io-cache.origin.endpoint", "https://" + OSS_HOST);
        options.set("io-cache.policy", "meta,read");
        options.set(DLF_CACHE_KEY, "true");
        return options;
    }

    private static Options cacheTargetOptions() {
        Options options = baseOptions();
        options.set("io-cache.targets", "accel,cluster");
        options.set("io-cache.target.accel.endpoint", "https://" + ACCEL_HOST);
        options.set("io-cache.target.accel.region", "cn-shanghai");
        options.set("io-cache.target.cluster.endpoint", "http://" + CLUSTER_HOST);
        options.set("io-cache.target.cluster.path-style-access", "true");
        options.set("io-cache.routes", "meta=accel;data,bucket-index=cluster");
        return options;
    }

    private JindoFileIO configuredFileIO(Options options) {
        JindoFileIO fileIO = new TestingJindoFileIO(accelFs, clusterFs, ossFs);
        fileIO.configure(CatalogContext.create(options));
        return fileIO;
    }

    private static IOException jindoError(int code, String message) {
        return new IOException(new JdoException(code, message));
    }

    // Hadoop's Path(String) needs commons-lang, which is not on this module's test classpath.
    private static org.apache.hadoop.fs.Path hadoopPath(String path) {
        return new org.apache.hadoop.fs.Path(URI.create(path));
    }

    private static JindoHadoopSystem mockFileSystem() throws IOException {
        JindoHadoopSystem fs = mock(JindoHadoopSystem.class);
        when(fs.open(any(org.apache.hadoop.fs.Path.class)))
                .thenAnswer(invocation -> new FSDataInputStream(new BytesInput()));
        when(fs.create(any(), anyBoolean())).thenReturn(mock(FSDataOutputStream.class));
        when(fs.getFileStatus(any())).thenReturn(mock(FileStatus.class));
        when(fs.rename(any(), any())).thenReturn(true);
        return fs;
    }

    private static class BytesInput extends ByteArrayInputStream
            implements Seekable, PositionedReadable {

        private BytesInput() {
            super(new byte[] {1, 2, 3});
        }

        @Override
        public void seek(long position) {
            pos = (int) position;
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
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer) {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestingJindoFileIO extends JindoFileIO {

        private final JindoHadoopSystem accelFs;
        private final JindoHadoopSystem clusterFs;
        private final JindoHadoopSystem ossFs;

        private TestingJindoFileIO(
                JindoHadoopSystem accelFs, JindoHadoopSystem clusterFs, JindoHadoopSystem ossFs) {
            this.accelFs = accelFs;
            this.clusterFs = clusterFs;
            this.ossFs = ossFs;
        }

        @Override
        protected Pair<JindoHadoopSystem, String> createFileSystem(
                org.apache.hadoop.fs.Path path, boolean enableCache) {
            assertThat(enableCache).isFalse();
            return Pair.of(ossFs, "oss");
        }

        @Override
        Pair<JindoHadoopSystem, String> createFileSystem(
                org.apache.hadoop.fs.Path path, CacheTarget target) {
            return Pair.of("accel".equals(target.name) ? accelFs : clusterFs, "oss");
        }
    }
}
