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

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializerTest;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.format.FlushingAvroFormat;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SstFileReader} and {@link SstFileWriter}. */
public class SstFileTest {

    private final SstTestDataGenerator gen =
            SstTestDataGenerator.builder().memTableCapacity(20).build();
    private final FileFormat flushingAvro = new FlushingAvroFormat();

    @TempDir java.nio.file.Path tempDir;

    @RepeatedTest(10)
    public void testWriteAndReadSstFile() throws Exception {
        SstTestDataGenerator.Data data = gen.next();
        SstFileWriter writer = createSstFileWriter(tempDir.toString());
        SstFileMetaSerializer serializer =
                new SstFileMetaSerializer(
                        TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.ROW_TYPE);

        List<SstFileMeta> actualMetas =
                writer.write(CloseableIterator.fromList(data.content, kv -> {}), 0);

        checkRollingFiles(data.meta, actualMetas, writer.suggestedFileSize());

        SstFileReader reader = createSstFileReader(tempDir.toString(), null, null);
        assertData(
                data,
                actualMetas,
                TestKeyValueGenerator.KEY_SERIALIZER,
                TestKeyValueGenerator.ROW_SERIALIZER,
                serializer,
                reader,
                kv -> kv);
    }

    @RepeatedTest(10)
    public void testCleanUpForException() throws IOException {
        FailingAtomicRenameFileSystem.get().reset(1, 10);
        SstTestDataGenerator.Data data = gen.next();
        SstFileWriter writer =
                createSstFileWriter(
                        FailingAtomicRenameFileSystem.getFailingPath(tempDir.toString()));

        try {
            writer.write(CloseableIterator.fromList(data.content, kv -> {}), 0);
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

    @Test
    public void testKeyProjection() throws Exception {
        SstTestDataGenerator.Data data = gen.next();
        SstFileWriter sstFileWriter = createSstFileWriter(tempDir.toString());
        SstFileMetaSerializer serializer =
                new SstFileMetaSerializer(
                        TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.ROW_TYPE);
        List<SstFileMeta> actualMetas =
                sstFileWriter.write(CloseableIterator.fromList(data.content, kv -> {}), 0);

        // projection: (shopId, orderId) -> (orderId)
        SstFileReader sstFileReader =
                createSstFileReader(tempDir.toString(), new int[][] {new int[] {1}}, null);
        RowType projectedKeyType =
                RowType.of(new LogicalType[] {new BigIntType(false)}, new String[] {"key_orderId"});
        RowDataSerializer projectedKeySerializer = new RowDataSerializer(projectedKeyType);
        assertData(
                data,
                actualMetas,
                projectedKeySerializer,
                TestKeyValueGenerator.ROW_SERIALIZER,
                serializer,
                sstFileReader,
                kv ->
                        new KeyValue()
                                .replace(
                                        GenericRowData.of(kv.key().getLong(1)),
                                        kv.sequenceNumber(),
                                        kv.valueKind(),
                                        kv.value()));
    }

    @Test
    public void testValueProjection() throws Exception {
        SstTestDataGenerator.Data data = gen.next();
        SstFileWriter sstFileWriter = createSstFileWriter(tempDir.toString());
        SstFileMetaSerializer serializer =
                new SstFileMetaSerializer(
                        TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.ROW_TYPE);
        List<SstFileMeta> actualMetas =
                sstFileWriter.write(CloseableIterator.fromList(data.content, kv -> {}), 0);

        // projection:
        // (dt, hr, shopId, orderId, itemId, priceAmount, comment) ->
        // (shopId, itemId, dt, hr)
        SstFileReader sstFileReader =
                createSstFileReader(
                        tempDir.toString(),
                        null,
                        new int[][] {new int[] {2}, new int[] {4}, new int[] {0}, new int[] {1}});
        RowType projectedValueType =
                RowType.of(
                        new LogicalType[] {
                            new IntType(false),
                            new BigIntType(),
                            new VarCharType(false, 8),
                            new IntType(false)
                        },
                        new String[] {"shopId", "itemId", "dt", "hr"});
        RowDataSerializer projectedValueSerializer = new RowDataSerializer(projectedValueType);
        assertData(
                data,
                actualMetas,
                TestKeyValueGenerator.KEY_SERIALIZER,
                projectedValueSerializer,
                serializer,
                sstFileReader,
                kv ->
                        new KeyValue()
                                .replace(
                                        kv.key(),
                                        kv.sequenceNumber(),
                                        kv.valueKind(),
                                        GenericRowData.of(
                                                kv.value().getInt(2),
                                                kv.value().isNullAt(4)
                                                        ? null
                                                        : kv.value().getLong(4),
                                                kv.value().getString(0),
                                                kv.value().getInt(1))));
    }

    private SstFileWriter createSstFileWriter(String path) {
        FileStorePathFactory pathFactory = new FileStorePathFactory(new Path(path));
        int suggestedFileSize = ThreadLocalRandom.current().nextInt(8192) + 1024;
        return new SstFileWriter.Factory(
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        // normal avro format will buffer changes in memory and we can't determine
                        // if the written file size is really larger than suggested, so we use a
                        // special avro format which flushes for every added element
                        flushingAvro,
                        pathFactory,
                        suggestedFileSize)
                .create(BinaryRowDataUtil.EMPTY_ROW, 0);
    }

    private SstFileReader createSstFileReader(
            String path, int[][] keyProjection, int[][] valueProjection) {
        FileStorePathFactory pathFactory = new FileStorePathFactory(new Path(path));
        SstFileReader.Factory factory =
                new SstFileReader.Factory(
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        flushingAvro,
                        pathFactory);
        if (keyProjection != null) {
            factory.withKeyProjection(keyProjection);
        }
        if (valueProjection != null) {
            factory.withValueProjection(valueProjection);
        }
        return factory.create(BinaryRowDataUtil.EMPTY_ROW, 0);
    }

    private void assertData(
            SstTestDataGenerator.Data data,
            List<SstFileMeta> actualMetas,
            RowDataSerializer keySerializer,
            RowDataSerializer projectedValueSerializer,
            SstFileMetaSerializer sstFileMetaSerializer,
            SstFileReader sstFileReader,
            Function<KeyValue, KeyValue> toExpectedKv)
            throws Exception {
        Iterator<KeyValue> expectedIterator = data.content.iterator();
        for (SstFileMeta meta : actualMetas) {
            // check the contents of sst file
            CloseableIterator<KeyValue> actualKvsIterator =
                    new RecordReaderIterator(sstFileReader.read(meta.fileName()));
            while (actualKvsIterator.hasNext()) {
                assertThat(expectedIterator.hasNext()).isTrue();
                KeyValue actualKv = actualKvsIterator.next();
                assertThat(
                                KeyValueSerializerTest.equals(
                                        toExpectedKv.apply(expectedIterator.next()),
                                        actualKv,
                                        keySerializer,
                                        projectedValueSerializer))
                        .isTrue();
            }
            actualKvsIterator.close();

            // check that each sst file meta is serializable
            assertThat(sstFileMetaSerializer.fromRow(sstFileMetaSerializer.toRow(meta)))
                    .isEqualTo(meta);
        }
        assertThat(expectedIterator.hasNext()).isFalse();
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
}
