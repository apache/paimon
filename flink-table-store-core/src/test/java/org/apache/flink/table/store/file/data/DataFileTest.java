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

package org.apache.flink.table.store.file.data;

import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializerTest;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.format.FlushingFileFormat;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.stats.StatsTestUtils;
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

/** Tests for {@link DataFileReader} and {@link DataFileWriter}. */
public class DataFileTest {

    private final DataFileTestDataGenerator gen =
            DataFileTestDataGenerator.builder().memTableCapacity(20).build();

    @TempDir java.nio.file.Path tempDir;

    @RepeatedTest(10)
    public void testWriteAndReadDataFileWithStatsCollectingRollingFile() throws Exception {
        testWriteAndReadDataFileImpl("avro");
    }

    @RepeatedTest(10)
    public void testWriteAndReadDataFileWithFileExtractingRollingFile() throws Exception {
        testWriteAndReadDataFileImpl("avro-extract");
    }

    private void testWriteAndReadDataFileImpl(String format) throws Exception {
        DataFileTestDataGenerator.Data data = gen.next();
        DataFileWriter writer = createDataFileWriter(tempDir.toString(), format);
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();

        List<DataFileMeta> actualMetas =
                writer.write(CloseableIterator.fromList(data.content, kv -> {}), 0);

        checkRollingFiles(
                TestKeyValueGenerator.KEY_TYPE,
                TestKeyValueGenerator.ROW_TYPE,
                data.meta,
                actualMetas,
                writer.suggestedFileSize());

        DataFileReader reader = createDataFileReader(tempDir.toString(), format, null, null);
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
        DataFileTestDataGenerator.Data data = gen.next();
        DataFileWriter writer =
                createDataFileWriter(
                        FailingAtomicRenameFileSystem.getFailingPath(tempDir.toString()), "avro");

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
        DataFileTestDataGenerator.Data data = gen.next();
        DataFileWriter dataFileWriter = createDataFileWriter(tempDir.toString(), "avro");
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        List<DataFileMeta> actualMetas =
                dataFileWriter.write(CloseableIterator.fromList(data.content, kv -> {}), 0);

        // projection: (shopId, orderId) -> (orderId)
        DataFileReader fileReader =
                createDataFileReader(tempDir.toString(), "avro", new int[][] {new int[] {1}}, null);
        RowType projectedKeyType =
                RowType.of(new LogicalType[] {new BigIntType(false)}, new String[] {"key_orderId"});
        RowDataSerializer projectedKeySerializer = new RowDataSerializer(projectedKeyType);
        assertData(
                data,
                actualMetas,
                projectedKeySerializer,
                TestKeyValueGenerator.ROW_SERIALIZER,
                serializer,
                fileReader,
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
        DataFileTestDataGenerator.Data data = gen.next();
        DataFileWriter dataFileWriter = createDataFileWriter(tempDir.toString(), "avro");
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        List<DataFileMeta> actualMetas =
                dataFileWriter.write(CloseableIterator.fromList(data.content, kv -> {}), 0);

        // projection:
        // (dt, hr, shopId, orderId, itemId, priceAmount, comment) ->
        // (shopId, itemId, dt, hr)
        DataFileReader fileReader =
                createDataFileReader(
                        tempDir.toString(),
                        "avro",
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
                fileReader,
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

    protected DataFileWriter createDataFileWriter(String path, String format) {
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        new Path(path),
                        RowType.of(),
                        FileSystemConnectorOptions.PARTITION_DEFAULT_NAME.defaultValue(),
                        format);
        int suggestedFileSize = ThreadLocalRandom.current().nextInt(8192) + 1024;
        return new DataFileWriter.Factory(
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        // normal format will buffer changes in memory and we can't determine
                        // if the written file size is really larger than suggested, so we use a
                        // special format which flushes for every added element
                        new FlushingFileFormat(format),
                        pathFactory,
                        suggestedFileSize)
                .create(BinaryRowDataUtil.EMPTY_ROW, 0);
    }

    private DataFileReader createDataFileReader(
            String path, String format, int[][] keyProjection, int[][] valueProjection) {
        FileStorePathFactory pathFactory = new FileStorePathFactory(new Path(path));
        DataFileReader.Factory factory =
                new DataFileReader.Factory(
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        new FlushingFileFormat(format),
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
            DataFileTestDataGenerator.Data data,
            List<DataFileMeta> actualMetas,
            RowDataSerializer keySerializer,
            RowDataSerializer projectedValueSerializer,
            DataFileMetaSerializer dataFileMetaSerializer,
            DataFileReader fileReader,
            Function<KeyValue, KeyValue> toExpectedKv)
            throws Exception {
        Iterator<KeyValue> expectedIterator = data.content.iterator();
        for (DataFileMeta meta : actualMetas) {
            // check the contents of data file
            CloseableIterator<KeyValue> actualKvsIterator =
                    new RecordReaderIterator(fileReader.read(meta.fileName()));
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

            // check that each data file meta is serializable
            assertThat(dataFileMetaSerializer.fromRow(dataFileMetaSerializer.toRow(meta)))
                    .isEqualTo(meta);
        }
        assertThat(expectedIterator.hasNext()).isFalse();
    }

    private void checkRollingFiles(
            RowType keyType,
            RowType valueType,
            DataFileMeta expected,
            List<DataFileMeta> actual,
            long suggestedFileSize) {
        FieldStatsArraySerializer keyStatsConverter = new FieldStatsArraySerializer(keyType);
        FieldStatsArraySerializer valueStatsConverter = new FieldStatsArraySerializer(valueType);

        // all but last file should be no smaller than suggestedFileSize
        for (int i = 0; i + 1 < actual.size(); i++) {
            assertThat(actual.get(i).fileSize() >= suggestedFileSize).isTrue();
        }

        // expected.rowCount == sum(rowCount)
        assertThat(actual.stream().mapToLong(DataFileMeta::rowCount).sum())
                .isEqualTo(expected.rowCount());

        // expected.minKey == firstFile.minKey
        assertThat(actual.get(0).minKey()).isEqualTo(expected.minKey());

        // expected.maxKey == lastFile.maxKey
        assertThat(actual.get(actual.size() - 1).maxKey()).isEqualTo(expected.maxKey());

        // check stats
        for (int i = 0; i < keyType.getFieldCount(); i++) {
            int idx = i;
            StatsTestUtils.checkRollingFileStats(
                    keyStatsConverter.fromBinary(expected.keyStats())[i],
                    actual,
                    m -> keyStatsConverter.fromBinary(m.keyStats())[idx]);
        }
        for (int i = 0; i < valueType.getFieldCount(); i++) {
            int idx = i;
            StatsTestUtils.checkRollingFileStats(
                    valueStatsConverter.fromBinary(expected.valueStats())[i],
                    actual,
                    m -> valueStatsConverter.fromBinary(m.valueStats())[idx]);
        }

        // expected.minSequenceNumber == min(minSequenceNumber)
        assertThat(actual.stream().mapToLong(DataFileMeta::minSequenceNumber).min().orElse(-1))
                .isEqualTo(expected.minSequenceNumber());

        // expected.maxSequenceNumber == max(maxSequenceNumber)
        assertThat(actual.stream().mapToLong(DataFileMeta::maxSequenceNumber).max().orElse(-1))
                .isEqualTo(expected.maxSequenceNumber());

        // expected.level == eachFile.level
        for (DataFileMeta meta : actual) {
            assertThat(meta.level()).isEqualTo(expected.level());
        }
    }
}
