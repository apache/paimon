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

package org.apache.paimon.spark;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the bounded transport before handing split metadata to Spark. */
public class SplitMetadataExternalizerTest {

    @TempDir private java.nio.file.Path directory;

    @Test
    public void testSmallValueStaysInlineWithoutCreatingAFile() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path sharedDirectory = new Path(directory.resolve("inline").toUri());
        SharedSplitMetadataExternalizer.Encoded encoded;
        try (SharedSplitMetadataExternalizer.ScanWriter writer =
                new SharedSplitMetadataExternalizer.ScanWriter(fileIO, sharedDirectory, 1024)) {
            encoded = writer.encode(scalaSplits(new TestingSplit(1)));
            assertThat(writer.externalEntryCount()).isZero();
            assertThat(writer.externalBytes()).isZero();
        }

        assertThat(encoded.external()).isFalse();
        assertThat(encoded.splitCount()).isOne();
        assertThat(encoded.rowCount()).isOne();
        assertThat(encoded.estimatedDataBytes()).isEqualTo(-1L);
        assertThat(fileIO.exists(sharedDirectory)).isFalse();
    }

    @Test
    public void testMultipleEntriesShareOneContainerAndUseRangeReads() throws Exception {
        CountingLocalFileIO fileIO = new CountingLocalFileIO();
        Path sharedDirectory = new Path(directory.resolve("ranges").toUri());
        SharedSplitMetadataExternalizer.Encoded first;
        SharedSplitMetadataExternalizer.Encoded second;
        SharedSplitMetadataExternalizer.Encoded empty;
        TestingSplit repeated = new TestingSplit(20);
        try (SharedSplitMetadataExternalizer.ScanWriter writer =
                new SharedSplitMetadataExternalizer.ScanWriter(fileIO, sharedDirectory, 1)) {
            first = writer.encode(scalaSplits(new TestingSplit(10)));
            second = writer.encode(scalaSplits(repeated, repeated, new TestingSplit(30)));
            empty = writer.encode(scalaSplits());
            assertThat(writer.externalEntryCount()).isEqualTo(3);
            assertThat(writer.externalBytes())
                    .isEqualTo(first.length() + second.length() + empty.length());
        }

        assertThat(first.path()).isEqualTo(second.path());
        assertThat(second.offset()).isEqualTo(first.length());
        assertThat(empty.offset()).isEqualTo(first.length() + second.length());
        CountingLocalFileIO.resetBytesRead();
        assertThat(
                        SharedSplitMetadataExternalizer.decode(
                                fileIO, second.path(), second.offset(), second.length()))
                .extracting(Split::rowCount)
                .containsExactly(20L, 20L, 30L);
        assertThat(CountingLocalFileIO.bytesRead()).isEqualTo(second.length());
        assertThat(CountingLocalFileIO.bytesRead()).isLessThan(first.length() + second.length());
        assertThat(
                        SharedSplitMetadataExternalizer.decode(
                                fileIO, empty.path(), empty.offset(), empty.length()))
                .isEmpty();

        SharedSplitMetadataExternalizer.cleanup(fileIO, sharedDirectory);
        assertThat(fileIO.exists(sharedDirectory)).isFalse();
    }

    @Test
    public void testSerializationFailureCanBeCleanedUp() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path sharedDirectory = new Path(directory.resolve("failure").toUri());
        try (SharedSplitMetadataExternalizer.ScanWriter writer =
                new SharedSplitMetadataExternalizer.ScanWriter(fileIO, sharedDirectory, 1)) {
            assertThatThrownBy(() -> writer.encode(scalaSplits(new NonSerializableSplit())))
                    .isInstanceOf(NotSerializableException.class);
        }

        SharedSplitMetadataExternalizer.cleanup(fileIO, sharedDirectory);
        assertThat(fileIO.exists(sharedDirectory)).isFalse();
    }

    @Test
    public void testSinglePassIteratorProducesDescriptorStatistics() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path sharedDirectory = new Path(directory.resolve("iterator").toUri());
        AtomicLong nextCalls = new AtomicLong();
        Iterator<Split> delegate =
                Arrays.<Split>asList(new TestingSplit(10), new TestingSplit(20)).iterator();
        Iterator<Split> singlePass =
                new Iterator<Split>() {
                    @Override
                    public boolean hasNext() {
                        return delegate.hasNext();
                    }

                    @Override
                    public Split next() {
                        nextCalls.incrementAndGet();
                        return delegate.next();
                    }
                };

        SharedSplitMetadataExternalizer.Encoded encoded;
        try (SharedSplitMetadataExternalizer.ScanWriter writer =
                new SharedSplitMetadataExternalizer.ScanWriter(fileIO, sharedDirectory, 1)) {
            encoded = writer.encode(singlePass, split -> split.rowCount() * 2);
        }

        assertThat(nextCalls).hasValue(2);
        assertThat(encoded.splitCount()).isEqualTo(2);
        assertThat(encoded.rowCount()).isEqualTo(30);
        assertThat(encoded.estimatedDataBytes()).isEqualTo(60);
        assertThat(
                        SharedSplitMetadataExternalizer.decode(
                                fileIO,
                                encoded.path(),
                                encoded.offset(),
                                encoded.length(),
                                encoded.formatVersion(),
                                encoded.splitCount(),
                                encoded.rowCount(),
                                encoded.estimatedDataBytes()))
                .extracting(Split::rowCount)
                .containsExactly(10L, 20L);
        SharedSplitMetadataExternalizer.cleanup(fileIO, sharedDirectory);
    }

    @Test
    public void testRejectsUnknownVersionTruncationAndChecksumMismatch() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();

        SharedSplitMetadataExternalizer.Encoded version =
                writeExternalValue(fileIO, "unknown-version", new TestingSplit(10));
        overwriteInt(version.path(), version.offset() + Integer.BYTES, 99);
        assertThatThrownBy(() -> decode(fileIO, version))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported split metadata format version");

        SharedSplitMetadataExternalizer.Encoded truncated =
                writeExternalValue(fileIO, "truncated", new TestingSplit(10));
        assertThatThrownBy(
                        () ->
                                SharedSplitMetadataExternalizer.decode(
                                        fileIO,
                                        truncated.path(),
                                        truncated.offset(),
                                        truncated.length() - 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Truncated split metadata range");

        SharedSplitMetadataExternalizer.Encoded checksum =
                writeExternalValue(fileIO, "checksum", new TestingSplit(10));
        flipInt(checksum.path(), checksum.offset() + checksum.length() - 28);
        assertThatThrownBy(() -> decode(fileIO, checksum))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("checksum mismatch");

        SharedSplitMetadataExternalizer.cleanup(
                fileIO, new Path(directory.resolve("unknown-version").toUri()));
        SharedSplitMetadataExternalizer.cleanup(
                fileIO, new Path(directory.resolve("truncated").toUri()));
        SharedSplitMetadataExternalizer.cleanup(
                fileIO, new Path(directory.resolve("checksum").toUri()));
    }

    @Test
    public void testRejectsDescriptorMismatch() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        SharedSplitMetadataExternalizer.Encoded encoded =
                writeExternalValue(fileIO, "descriptor", new TestingSplit(10));

        assertThatThrownBy(
                        () ->
                                SharedSplitMetadataExternalizer.decode(
                                        fileIO,
                                        encoded.path(),
                                        encoded.offset(),
                                        encoded.length(),
                                        encoded.formatVersion(),
                                        encoded.splitCount() + 1,
                                        encoded.rowCount(),
                                        encoded.estimatedDataBytes()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("descriptor mismatch");
        SharedSplitMetadataExternalizer.cleanup(
                fileIO, new Path(directory.resolve("descriptor").toUri()));
    }

    @Test
    public void testSeekFailureClosesInputStream() {
        AtomicBoolean closed = new AtomicBoolean();
        LocalFileIO fileIO =
                new LocalFileIO() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public SeekableInputStream newInputStream(Path path) {
                        return new SeekableInputStream() {
                            @Override
                            public void seek(long desired) throws IOException {
                                throw new IOException("seek failed");
                            }

                            @Override
                            public long getPos() {
                                return 0;
                            }

                            @Override
                            public int read() {
                                return -1;
                            }

                            @Override
                            public int read(byte[] bytes, int offset, int length) {
                                return -1;
                            }

                            @Override
                            public void close() {
                                closed.set(true);
                            }
                        };
                    }
                };

        assertThatThrownBy(
                        () ->
                                SharedSplitMetadataExternalizer.decode(
                                        fileIO, new Path(directory.toUri()).toString(), 1, 1))
                .isInstanceOf(IOException.class)
                .hasMessage("seek failed");
        assertThat(closed).isTrue();
    }

    @Test
    public void testCleanupManagerCleansAtApplicationEndAndAfterStop() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path applicationDirectory = writeExternalValue(fileIO, "application-cleanup");
        Path lateDirectory = writeExternalValue(fileIO, "late-cleanup");
        SparkSession spark = SparkSession.builder().master("local[2]").getOrCreate();
        Broadcast<FileIO> applicationBroadcast =
                JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(fileIO);
        Broadcast<FileIO> lateBroadcast =
                JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(fileIO);

        SplitMetadataCleanupManager$.MODULE$.register(
                spark, fileIO, applicationDirectory, applicationBroadcast);
        assertThat(fileIO.exists(applicationDirectory)).isTrue();
        spark.stop();
        assertThat(fileIO.exists(applicationDirectory)).isFalse();

        SplitMetadataCleanupManager$.MODULE$.register(spark, fileIO, lateDirectory, lateBroadcast);
        assertThat(fileIO.exists(lateDirectory)).isFalse();
    }

    @Test
    public void testCleanupHandleIsIdempotent() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path manualDirectory = writeExternalValue(fileIO, "manual-cleanup");
        SparkSession spark = SparkSession.builder().master("local[2]").getOrCreate();
        Broadcast<FileIO> broadcast =
                JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(fileIO);
        try {
            SplitMetadataResourceHandle handle =
                    SplitMetadataCleanupManager$.MODULE$.register(
                            spark, fileIO, manualDirectory, broadcast);
            handle.close();
            handle.close();
            assertThat(fileIO.exists(manualDirectory)).isFalse();
        } finally {
            spark.stop();
        }
    }

    @Test
    public void testSparkExecutorReadsSharedMetadataAndPayloadStaysSmall() throws Exception {
        CountingLocalFileIO fileIO = new CountingLocalFileIO();
        Path sharedDirectory = new Path(directory.resolve("executor").toUri());
        List<Split> splits =
                IntStream.range(0, 10_000).mapToObj(TestingSplit::new).collect(Collectors.toList());
        Seq<Split> scalaSplits = JavaConverters.asScalaBuffer(splits).toList();
        SharedSplitMetadataExternalizer.Encoded encoded;
        try (SharedSplitMetadataExternalizer.ScanWriter writer =
                new SharedSplitMetadataExternalizer.ScanWriter(fileIO, sharedDirectory, 1)) {
            encoded = writer.encode(splits.iterator(), Split::rowCount);
        }

        SparkSession spark = SparkSession.builder().master("local[2]").getOrCreate();
        try {
            SharedExternalizedPaimonInputPartition partition =
                    new SharedExternalizedPaimonInputPartition(
                            encoded.path(),
                            encoded.offset(),
                            encoded.length(),
                            encoded.formatVersion(),
                            encoded.splitCount(),
                            encoded.rowCount(),
                            encoded.estimatedDataBytes(),
                            JavaSparkContext.fromSparkContext(spark.sparkContext())
                                    .broadcast(fileIO),
                            scalaSplits);
            byte[] original =
                    InstantiationUtil.serializeObject(new SimplePaimonInputPartition(scalaSplits));
            byte[] externalized = InstantiationUtil.serializeObject(partition);
            assertThat(externalized.length).isLessThan(original.length / 10);

            CountingLocalFileIO.resetBytesRead();
            SharedExternalizedPaimonInputPartition metricsOnly =
                    InstantiationUtil.deserializeObject(
                            externalized, Thread.currentThread().getContextClassLoader());
            assertThat(metricsOnly.splitCount()).isEqualTo(10_000);
            assertThat(metricsOnly.rowCount()).isEqualTo(49_995_000L);
            assertThat(metricsOnly.estimatedDataBytes()).isEqualTo(49_995_000L);
            assertThat(CountingLocalFileIO.bytesRead()).isZero();
            assertThat(metricsOnly.splits().size()).isEqualTo(10_000);
            assertThat(CountingLocalFileIO.bytesRead()).isGreaterThan(0);

            List<Long> result =
                    JavaSparkContext.fromSparkContext(spark.sparkContext())
                            .parallelize(Arrays.asList(externalized), 1)
                            .map(
                                    bytes -> {
                                        SharedExternalizedPaimonInputPartition decoded =
                                                InstantiationUtil.deserializeObject(
                                                        bytes,
                                                        Thread.currentThread()
                                                                .getContextClassLoader());
                                        return JavaConverters.seqAsJavaList(decoded.splits())
                                                .stream()
                                                .mapToLong(Split::rowCount)
                                                .sum();
                                    })
                            .collect();
            assertThat(result).containsExactly(49_995_000L);
        } finally {
            spark.stop();
            SharedSplitMetadataExternalizer.cleanup(fileIO, sharedDirectory);
        }
    }

    @Test
    public void testExternalizedBucketedPartitionPreservesBucket() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path sharedDirectory = new Path(directory.resolve("bucketed").toUri());
        Seq<Split> splits =
                JavaConverters.asScalaBuffer(
                                Arrays.<Split>asList(new TestingSplit(10), new TestingSplit(20)))
                        .toList();
        SharedSplitMetadataExternalizer.Encoded encoded;
        try (SharedSplitMetadataExternalizer.ScanWriter writer =
                new SharedSplitMetadataExternalizer.ScanWriter(fileIO, sharedDirectory, 1)) {
            encoded = writer.encode(splits);
        }

        SparkSession spark = SparkSession.builder().master("local[2]").getOrCreate();
        try {
            SharedExternalizedPaimonBucketedInputPartition partition =
                    new SharedExternalizedPaimonBucketedInputPartition(
                            encoded.path(),
                            encoded.offset(),
                            encoded.length(),
                            encoded.formatVersion(),
                            encoded.splitCount(),
                            encoded.rowCount(),
                            encoded.estimatedDataBytes(),
                            JavaSparkContext.fromSparkContext(spark.sparkContext())
                                    .broadcast(fileIO),
                            splits,
                            7);
            SharedExternalizedPaimonBucketedInputPartition decoded =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(partition),
                            Thread.currentThread().getContextClassLoader());

            assertThat(decoded.bucketed()).isTrue();
            assertThat(decoded.partitionKey().getInt(0)).isEqualTo(7);
            assertThat(JavaConverters.seqAsJavaList(decoded.splits()))
                    .extracting(Split::rowCount)
                    .containsExactly(10L, 20L);
        } finally {
            spark.stop();
            SharedSplitMetadataExternalizer.cleanup(fileIO, sharedDirectory);
        }
    }

    @SafeVarargs
    private static Seq<Split> scalaSplits(Split... splits) {
        return JavaConverters.asScalaBuffer(Arrays.asList(splits)).toList();
    }

    private Path writeExternalValue(LocalFileIO fileIO, String name) throws Exception {
        SharedSplitMetadataExternalizer.Encoded encoded =
                writeExternalValue(fileIO, name, new TestingSplit(1));
        assertThat(encoded.external()).isTrue();
        return new Path(directory.resolve(name).toUri());
    }

    private SharedSplitMetadataExternalizer.Encoded writeExternalValue(
            LocalFileIO fileIO, String name, Split split) throws Exception {
        Path sharedDirectory = new Path(directory.resolve(name).toUri());
        try (SharedSplitMetadataExternalizer.ScanWriter writer =
                new SharedSplitMetadataExternalizer.ScanWriter(fileIO, sharedDirectory, 1)) {
            SharedSplitMetadataExternalizer.Encoded encoded =
                    writer.encode(Arrays.asList(split).iterator(), ignored -> 0L);
            assertThat(encoded.external()).isTrue();
            return encoded;
        }
    }

    private static List<Split> decode(
            LocalFileIO fileIO, SharedSplitMetadataExternalizer.Encoded encoded) throws Exception {
        return SharedSplitMetadataExternalizer.decode(
                fileIO,
                encoded.path(),
                encoded.offset(),
                encoded.length(),
                encoded.formatVersion(),
                encoded.splitCount(),
                encoded.rowCount(),
                encoded.estimatedDataBytes());
    }

    private static void overwriteInt(String path, long offset, int value) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(new java.io.File(new URI(path)), "rw")) {
            file.seek(offset);
            file.writeInt(value);
        }
    }

    private static void flipInt(String path, long offset) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(new java.io.File(new URI(path)), "rw")) {
            file.seek(offset);
            int value = file.readInt();
            file.seek(offset);
            file.writeInt(value ^ 1);
        }
    }

    private static class CountingLocalFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;
        private static final AtomicLong BYTES_READ = new AtomicLong();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            SeekableInputStream delegate = super.newInputStream(path);
            return new SeekableInputStream() {
                @Override
                public void seek(long desired) throws IOException {
                    delegate.seek(desired);
                }

                @Override
                public long getPos() throws IOException {
                    return delegate.getPos();
                }

                @Override
                public int read(byte[] bytes, int offset, int length) throws IOException {
                    int read = delegate.read(bytes, offset, length);
                    if (read > 0) {
                        BYTES_READ.addAndGet(read);
                    }
                    return read;
                }

                @Override
                public int read() throws IOException {
                    int value = delegate.read();
                    if (value >= 0) {
                        BYTES_READ.incrementAndGet();
                    }
                    return value;
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }

        private static void resetBytesRead() {
            BYTES_READ.set(0);
        }

        private static long bytesRead() {
            return BYTES_READ.get();
        }
    }

    private static class TestingSplit implements Split {

        private static final long serialVersionUID = 1L;
        private final long rowCount;

        private TestingSplit(long rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public long rowCount() {
            return rowCount;
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }

    private static class NonSerializableSplit implements Split {

        private static final long serialVersionUID = 1L;
        private final Object nonSerializable = new Object();

        @Override
        public long rowCount() {
            return nonSerializable.hashCode();
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }
}
