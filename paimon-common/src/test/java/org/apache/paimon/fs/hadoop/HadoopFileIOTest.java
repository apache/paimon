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

package org.apache.paimon.fs.hadoop;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HadoopFileIO}, mostly around releasing the file systems it owns. */
public class HadoopFileIOTest {

    @Test
    public void testUncachedFileSystemIsClosed() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://owned/warehouse");

        RecordingFileSystem fs = fileSystem(fileIO, "testfs://owned/a");
        assertThat(fs.closeCount()).isZero();

        fileIO.close();

        assertThat(fs.closeCount()).isEqualTo(1);
        assertThat(fileIO.fsMap).isEmpty();
    }

    @Test
    public void testCachedFileSystemIsNotClosed() throws Exception {
        // without disable.cache the instance comes from Hadoop's global cache and is shared with
        // every other user in this JVM, so closing it here would break unrelated readers
        Configuration conf = conf("testfs");
        HadoopFileIO fileIO = fileIO(conf, "testfs://shared/warehouse");

        RecordingFileSystem fs = fileSystem(fileIO, "testfs://shared/a");
        try {
            fileIO.close();

            assertThat(fs.closeCount()).isZero();

            HadoopFileIO other = fileIO(conf, "testfs://shared/warehouse");
            assertThat(fileSystem(other, "testfs://shared/a")).isSameAs(fs);
        } finally {
            fs.close();
        }
    }

    @Test
    public void testOnlyTheOwnedSchemeIsClosed() throws Exception {
        Configuration conf = conf("testfs", "otherfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://mixed/warehouse");

        RecordingFileSystem owned = fileSystem(fileIO, "testfs://mixed/a");
        RecordingFileSystem shared = fileSystem(fileIO, "otherfs://mixed/a");

        try {
            fileIO.close();

            assertThat(owned.closeCount()).isEqualTo(1);
            assertThat(shared.closeCount()).isZero();
        } finally {
            shared.close();
        }
    }

    @Test
    public void testFailingCloseDoesNotSkipTheOtherFileSystems() throws Exception {
        Configuration conf = conf("testfs", "badfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        conf.setBoolean("fs.badfs.impl.disable.cache", true);
        conf.setBoolean("fs.badfs.test.fail-on-close", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://failing/warehouse");

        RecordingFileSystem bad = fileSystem(fileIO, "badfs://failing/a");
        RecordingFileSystem good = fileSystem(fileIO, "testfs://failing/a");

        assertThatThrownBy(fileIO::close).isInstanceOf(IOException.class);

        assertThat(bad.closeCount()).isEqualTo(1);
        assertThat(good.closeCount()).isEqualTo(1);
        assertThat(fileIO.fsMap).isEmpty();
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://idempotent/warehouse");

        RecordingFileSystem fs = fileSystem(fileIO, "testfs://idempotent/a");
        fileIO.close();
        fileIO.close();

        assertThat(fs.closeCount()).isEqualTo(1);
    }

    @Test
    public void testCloseBeforeAnyUseIsSafe() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://untouched/warehouse");

        fileIO.close();
        fileIO.close();
    }

    @Test
    public void testInjectedFileSystemIsNotClosed() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://injected/warehouse");
        RecordingFileSystem fs = new RecordingFileSystem();
        fileIO.setFileSystem(fs);

        fileIO.close();

        assertThat(fs.closeCount()).isZero();
    }

    @Test
    public void testFileSystemLosingTheCreationRaceIsReleased() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        RecordingFileSystem winner = new RecordingFileSystem();
        RacingHadoopFileIO fileIO =
                new RacingHadoopFileIO(new Path("testfs://race/warehouse"), winner);
        fileIO.configure(CatalogContext.create(new Options(), conf));

        FileSystem returned =
                fileIO.getFileSystem(new org.apache.hadoop.fs.Path("testfs://race/a"));

        assertThat(returned).isSameAs(winner);
        assertThat(fileIO.loser.closeCount()).isEqualTo(1);
        assertThat(winner.closeCount()).isZero();
    }

    @Test
    public void testOwnershipUsesTheSchemeAsWritten() {
        Configuration conf = new Configuration();
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://spelling/warehouse");

        assertThat(fileIO.isOwnedScheme("testfs")).isTrue();
        assertThat(fileIO.isOwnedScheme("TESTFS")).isFalse();
        assertThat(fileIO.isOwnedScheme("otherfs")).isFalse();
    }

    @Test
    public void testSchemelessPathFallsBackToTheDefaultFileSystem() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "testfs://default");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://default/warehouse");

        assertThat(fileIO.isOwnedScheme(null)).isTrue();

        conf.setBoolean("fs.testfs.impl.disable.cache", false);
        assertThat(fileIO.isOwnedScheme(null)).isFalse();
    }

    @Test
    public void testUseAfterCloseIsRejectedInsteadOfLeakingAgain() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://reuse/warehouse");

        RecordingFileSystem fs = fileSystem(fileIO, "testfs://reuse/a");
        fileIO.close();
        assertThat(fs.closeCount()).isEqualTo(1);

        RecordingFileSystem.resetCounters();
        assertThatThrownBy(() -> fileSystem(fileIO, "testfs://reuse/a"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
        assertThat(RecordingFileSystem.created()).isZero();
    }

    @Test
    public void testFileSystemCreatedWhileClosingIsReleased() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        ClosingWhileCreatingFileIO fileIO =
                new ClosingWhileCreatingFileIO(new Path("testfs://late/warehouse"));
        fileIO.configure(CatalogContext.create(new Options(), conf));

        assertThatThrownBy(
                        () ->
                                fileIO.getFileSystem(
                                        new org.apache.hadoop.fs.Path("testfs://late/a")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");

        assertThat(fileIO.created.closeCount()).isEqualTo(1);
        assertThat(fileIO.fsMap).isEmpty();
    }

    @Test
    public void testCachedFileSystemLosingTheRaceIsNotClosed() throws Exception {
        Configuration conf = conf("testfs");
        RacingHadoopFileIO fileIO =
                new RacingHadoopFileIO(new Path("testfs://cachedrace/warehouse"), null);
        fileIO.configure(CatalogContext.create(new Options(), conf));

        FileSystem returned =
                fileIO.getFileSystem(new org.apache.hadoop.fs.Path("testfs://cachedrace/a"));

        try {
            assertThat(((RecordingFileSystem) returned).closeCount()).isZero();
        } finally {
            ((RecordingFileSystem) returned).close();
        }
    }

    @Test
    public void testADeserializedCopyOutlivesTheInstanceItCameFrom() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://serialized/warehouse");
        RecordingFileSystem fs = fileSystem(fileIO, "testfs://serialized/a");

        byte[] shipped = InstantiationUtil.serializeObject(fileIO);
        fileIO.close();
        assertThat(fs.closeCount()).isEqualTo(1);

        HadoopFileIO copy =
                InstantiationUtil.deserializeObject(shipped, HadoopFileIO.class.getClassLoader());

        RecordingFileSystem copyFs = fileSystem(copy, "testfs://serialized/a");
        assertThat(copyFs).isNotSameAs(fs);
        assertThat(copyFs.closeCount()).isZero();

        copy.close();
        assertThat(copyFs.closeCount()).isEqualTo(1);
    }

    @Test
    public void testOwnershipFollowsThePathSchemeNotTheFileSystemUri() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        conf.set("fs.testfs.test.reported-scheme", "otherfs");
        HadoopFileIO fileIO = fileIO(conf, "testfs://reported/warehouse");

        RecordingFileSystem fs = fileSystem(fileIO, "testfs://reported/a");
        assertThat(fs.getUri().getScheme()).isEqualTo("otherfs");

        fileIO.close();

        assertThat(fs.closeCount()).isEqualTo(1);
    }

    @Test
    public void testOwnedFileSystemIsStillClosedAfterTheFlagIsFlippedOff() throws Exception {
        Configuration conf = conf("testfs");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);
        HadoopFileIO fileIO = fileIO(conf, "testfs://flippedoff/warehouse");

        RecordingFileSystem fs = fileSystem(fileIO, "testfs://flippedoff/a");
        conf.setBoolean("fs.testfs.impl.disable.cache", false);

        fileIO.close();

        assertThat(fs.closeCount()).isEqualTo(1);
    }

    @Test
    public void testSharedFileSystemIsStillSparedAfterTheFlagIsFlippedOn() throws Exception {
        Configuration conf = conf("testfs");
        HadoopFileIO fileIO = fileIO(conf, "testfs://flippedon/warehouse");

        RecordingFileSystem fs = fileSystem(fileIO, "testfs://flippedon/a");
        conf.setBoolean("fs.testfs.impl.disable.cache", true);

        try {
            fileIO.close();

            assertThat(fs.closeCount()).isZero();
        } finally {
            fs.close();
        }
    }

    @Test
    public void testMalformedDefaultFileSystemDoesNotBreakClose() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "no-scheme-here");
        HadoopFileIO fileIO = fileIO(conf, "testfs://malformed/warehouse");

        assertThat(fileIO.isOwnedScheme(null)).isFalse();
        fileIO.close();
    }

    private static Configuration conf(String... schemes) {
        Configuration conf = new Configuration();
        for (String scheme : schemes) {
            conf.set("fs." + scheme + ".impl", RecordingFileSystem.class.getName());
        }
        return conf;
    }

    private static HadoopFileIO fileIO(Configuration conf, String warehouse) {
        HadoopFileIO fileIO = new HadoopFileIO(new Path(warehouse));
        fileIO.configure(CatalogContext.create(new Options(), conf));
        return fileIO;
    }

    private static RecordingFileSystem fileSystem(HadoopFileIO fileIO, String path)
            throws IOException {
        return (RecordingFileSystem) fileIO.getFileSystem(new org.apache.hadoop.fs.Path(path));
    }

    /** A {@link HadoopFileIO} that always loses the race for publishing a new file system. */
    private static class RacingHadoopFileIO extends HadoopFileIO {

        private static final long serialVersionUID = 1L;

        private final FileSystem winner;
        private RecordingFileSystem loser;

        private RacingHadoopFileIO(Path path, FileSystem winner) {
            super(path);
            this.winner = winner;
        }

        @Override
        protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
            loser = (RecordingFileSystem) super.createFileSystem(path);
            URI uri = path.toUri();
            fsMap.put(
                    Pair.of(uri.getScheme(), uri.getAuthority()),
                    Pair.of(winner == null ? loser : winner, isOwnedScheme(uri.getScheme())));
            return loser;
        }
    }

    /** A {@link HadoopFileIO} that is closed in the window between creating and publishing. */
    private static class ClosingWhileCreatingFileIO extends HadoopFileIO {

        private static final long serialVersionUID = 1L;

        private RecordingFileSystem created;

        private ClosingWhileCreatingFileIO(Path path) {
            super(path);
        }

        @Override
        protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
            created = (RecordingFileSystem) super.createFileSystem(path);
            close();
            return created;
        }
    }

    /** A {@link FileSystem} that records how often it was closed and does nothing else. */
    public static class RecordingFileSystem extends FileSystem {

        private static final AtomicInteger CREATED = new AtomicInteger();

        private URI uri;
        private boolean failOnClose;
        private int closeCount;

        static void resetCounters() {
            CREATED.set(0);
        }

        static int created() {
            return CREATED.get();
        }

        @Override
        public void initialize(URI name, Configuration conf) throws IOException {
            super.initialize(name, conf);
            String reportedScheme =
                    conf.get("fs." + name.getScheme() + ".test.reported-scheme", null);
            this.uri =
                    reportedScheme == null
                            ? name
                            : URI.create(reportedScheme + "://" + name.getAuthority());
            this.failOnClose =
                    conf.getBoolean("fs." + name.getScheme() + ".test.fail-on-close", false);
            CREATED.incrementAndGet();
        }

        @Override
        public URI getUri() {
            return uri;
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            super.close();
            if (failOnClose) {
                throw new IOException("close fails on purpose for " + uri);
            }
        }

        int closeCount() {
            return closeCount;
        }

        @Override
        public FSDataInputStream open(org.apache.hadoop.fs.Path f, int bufferSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream create(
                org.apache.hadoop.fs.Path f,
                FsPermission permission,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                Progressable progress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(
                org.apache.hadoop.fs.Path f, int bufferSize, Progressable progress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean delete(org.apache.hadoop.fs.Path f, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(org.apache.hadoop.fs.Path f) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(org.apache.hadoop.fs.Path dir) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.hadoop.fs.Path getWorkingDirectory() {
            return new org.apache.hadoop.fs.Path("/");
        }

        @Override
        public boolean mkdirs(org.apache.hadoop.fs.Path f, FsPermission permission) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(org.apache.hadoop.fs.Path f) throws IOException {
            throw new FileNotFoundException(f.toString());
        }
    }
}
