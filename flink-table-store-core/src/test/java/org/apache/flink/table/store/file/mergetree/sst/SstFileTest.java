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

package org.apache.flink.table.store.file.mergetree.sst;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializerTest;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SstFile}. */
public class SstFileTest {

    private final SstTestDataGenerator gen =
            SstTestDataGenerator.builder().memTableCapacity(20).build();
    private final FileFormat flushingAvro = new FlushingAvroFormat();

    @TempDir java.nio.file.Path tempDir;

    @RepeatedTest(10)
    public void testWriteAndReadSstFile() throws Exception {
        SstTestDataGenerator.SstFile data = gen.next();
        SstFile sstFile = createSstFile(tempDir.toString());
        SstFileMetaSerializer serializer =
                new SstFileMetaSerializer(
                        TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.ROW_TYPE);

        List<SstFileMeta> actualMetas =
                sstFile.write(CloseableIterator.fromList(data.content, kv -> {}), 0);

        checkRollingFiles(data.meta, actualMetas, sstFile.suggestedFileSize());

        Iterator<KeyValue> expectedIterator = data.content.iterator();
        for (SstFileMeta meta : actualMetas) {
            // check the contents of sst file
            CloseableIterator<KeyValue> actualKvsIterator =
                    new RecordReaderIterator(sstFile.read(meta.fileName()));
            while (actualKvsIterator.hasNext()) {
                assertThat(expectedIterator.hasNext()).isTrue();
                KeyValue actualKv = actualKvsIterator.next();
                assertThat(
                                KeyValueSerializerTest.equals(
                                        expectedIterator.next(),
                                        actualKv,
                                        TestKeyValueGenerator.KEY_SERIALIZER,
                                        TestKeyValueGenerator.ROW_SERIALIZER))
                        .isTrue();
            }
            actualKvsIterator.close();

            // check that each sst file meta is serializable
            assertThat(serializer.fromRow(serializer.toRow(meta))).isEqualTo(meta);
        }
        assertThat(expectedIterator.hasNext()).isFalse();
    }

    @RepeatedTest(10)
    public void testCleanUpForException() throws IOException {
        FailingAtomicRenameFileSystem.resetFailCounter(1);
        FailingAtomicRenameFileSystem.setFailPossibility(10);
        SstTestDataGenerator.SstFile data = gen.next();
        SstFile sstFile =
                createSstFile(FailingAtomicRenameFileSystem.SCHEME + "://" + tempDir.toString());

        try {
            sstFile.write(CloseableIterator.fromList(data.content, kv -> {}), 0);
        } catch (Throwable e) {
            assertThat(e)
                    .isExactlyInstanceOf(FailingAtomicRenameFileSystem.ArtificialException.class);
            Path root = new Path(tempDir.toString());
            FileSystem fs = root.getFileSystem();
            for (FileStatus bucketStatus : fs.listStatus(root)) {
                assertThat(bucketStatus.isDir()).isTrue();
                assertThat(fs.listStatus(bucketStatus.getPath())).isEmpty();
            }
        }
    }

    private SstFile createSstFile(String path) {
        FileStorePathFactory fileStorePathFactory = new FileStorePathFactory(new Path(path));
        SstPathFactory sstPathFactory = fileStorePathFactory.createSstPathFactory(null, 0);
        int suggestedFileSize = ThreadLocalRandom.current().nextInt(8192) + 1024;
        return new SstFile(
                TestKeyValueGenerator.KEY_TYPE,
                TestKeyValueGenerator.ROW_TYPE,
                // normal avro format will buffer changes in memory and we can't determine
                // if the written file size is really larger than suggested, so we use a
                // special avro format which flushes for every added element
                flushingAvro,
                sstPathFactory,
                suggestedFileSize);
    }

    private void checkRollingFiles(
            SstFileMeta expected, List<SstFileMeta> actual, long suggestedFileSize) {
        // all but last file should be no smaller than suggestedFileSize
        for (int i = 0; i + 1 < actual.size(); i++) {
            assertThat(actual.get(i).fileSize() >= suggestedFileSize).isTrue();
        }

        // expected.rowCount == sum(rowCount)
        assertThat(actual.stream().mapToLong(SstFileMeta::rowCount).sum())
                .isEqualTo(expected.rowCount());

        // expected.minKey == firstFile.minKey
        assertThat(actual.get(0).minKey()).isEqualTo(expected.minKey());

        // expected.maxKey == lastFile.maxKey
        assertThat(actual.get(actual.size() - 1).maxKey()).isEqualTo(expected.maxKey());

        // TODO check stats after they're collected
        /*
        for (int i = 0; i < expected.stats().length; i++) {
            List<FieldStats> actualStats = new ArrayList<>();
            for (SstFileMeta meta : actual) {
                actualStats.add(meta.stats()[i]);
            }
            checkRollingFileStats(expected.stats()[i], actualStats);
        }
        */

        // expected.minSequenceNumber == min(minSequenceNumber)
        assertThat(actual.stream().mapToLong(SstFileMeta::minSequenceNumber).min().orElse(-1))
                .isEqualTo(expected.minSequenceNumber());

        // expected.maxSequenceNumber == max(maxSequenceNumber)
        assertThat(actual.stream().mapToLong(SstFileMeta::maxSequenceNumber).max().orElse(-1))
                .isEqualTo(expected.maxSequenceNumber());

        // expected.level == eachFile.level
        for (SstFileMeta meta : actual) {
            assertThat(meta.level()).isEqualTo(expected.level());
        }
    }

    @SuppressWarnings("unchecked")
    private void checkRollingFileStats(FieldStats expected, List<FieldStats> actual) {
        if (expected.minValue() instanceof Comparable) {
            Object actualMin = null;
            Object actualMax = null;
            for (FieldStats stats : actual) {
                if (stats.minValue() != null
                        && (actualMin == null
                                || ((Comparable<Object>) stats.minValue()).compareTo(actualMin)
                                        < 0)) {
                    actualMin = stats.minValue();
                }
                if (stats.maxValue() != null
                        && (actualMax == null
                                || ((Comparable<Object>) stats.maxValue()).compareTo(actualMax)
                                        > 0)) {
                    actualMax = stats.maxValue();
                }
            }
            assertThat(actualMin).isEqualTo(expected.minValue());
            assertThat(actualMax).isEqualTo(expected.maxValue());
        } else {
            for (FieldStats stats : actual) {
                assertThat(stats.minValue()).isNull();
                assertThat(stats.maxValue()).isNull();
            }
        }
        assertThat(actual.stream().mapToLong(FieldStats::nullCount).sum())
                .isEqualTo(expected.nullCount());
    }

    /** A special avro {@link FileFormat} which flushes for every added element. */
    public static class FlushingAvroFormat implements FileFormat {

        private final FileFormat avro =
                FileFormat.fromIdentifier(
                        SstFileTest.class.getClassLoader(), "avro", new Configuration());

        @Override
        public BulkFormat<RowData, FileSourceSplit> createReaderFactory(
                RowType type, List<ResolvedExpression> filters) {
            return avro.createReaderFactory(type, filters);
        }

        @Override
        public BulkWriter.Factory<RowData> createWriterFactory(RowType type) {
            return fsDataOutputStream -> {
                BulkWriter<RowData> wrapped =
                        avro.createWriterFactory(type).create(fsDataOutputStream);
                return new BulkWriter<RowData>() {
                    @Override
                    public void addElement(RowData rowData) throws IOException {
                        wrapped.addElement(rowData);
                        wrapped.flush();
                    }

                    @Override
                    public void flush() throws IOException {
                        wrapped.flush();
                    }

                    @Override
                    public void finish() throws IOException {
                        wrapped.finish();
                    }
                };
            };
        }
    }
}
