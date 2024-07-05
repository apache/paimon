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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializerTest;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static org.apache.paimon.TestKeyValueGenerator.DEFAULT_ROW_TYPE;
import static org.apache.paimon.TestKeyValueGenerator.KEY_TYPE;
import static org.apache.paimon.TestKeyValueGenerator.createTestSchemaManager;
import static org.apache.paimon.stats.StatsTestUtils.convertWithoutSchemaEvolution;
import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for reading and writing {@link KeyValue} data files. */
public class KeyValueFileReadWriteTest {

    private final DataFileTestDataGenerator gen =
            DataFileTestDataGenerator.builder().memTableCapacity(20).build();

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testReadNonExistentFile() {
        KeyValueFileReaderFactory readerFactory =
                createReaderFactory(tempDir.toString(), "avro", null, null);
        assertThatThrownBy(() -> readerFactory.createRecordReader(0, "dummy_file.avro", 1, 0))
                .hasMessageContaining(
                        "you can configure 'snapshot.time-retained' option with a larger value.");
    }

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
        KeyValueFileWriterFactory writerFactory = createWriterFactory(tempDir.toString(), format);
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();

        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
        writer.write(CloseableIterator.fromList(data.content, kv -> {}));
        writer.close();
        List<DataFileMeta> actualMetas = writer.result();

        checkRollingFiles(data.meta, actualMetas, writer.targetFileSize());

        KeyValueFileReaderFactory readerFactory =
                createReaderFactory(tempDir.toString(), format, null, null);
        assertData(
                data,
                actualMetas,
                TestKeyValueGenerator.KEY_SERIALIZER,
                TestKeyValueGenerator.DEFAULT_ROW_SERIALIZER,
                serializer,
                readerFactory,
                kv -> kv);
    }

    @RepeatedTest(10)
    public void testCleanUpForException() throws IOException {
        String failingName = UUID.randomUUID().toString();
        FailingFileIO.reset(failingName, 1, 10);
        DataFileTestDataGenerator.Data data = gen.next();
        KeyValueFileWriterFactory writerFactory =
                createWriterFactory(
                        FailingFileIO.getFailingPath(failingName, tempDir.toString()), "avro");

        try {
            FileWriter<KeyValue, ?> writer =
                    writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
            writer.write(CloseableIterator.fromList(data.content, kv -> {}));
        } catch (Throwable e) {
            if (e.getCause() != null) {
                assertThat(e)
                        .hasRootCauseExactlyInstanceOf(FailingFileIO.ArtificialException.class);
            } else {
                assertThat(e).isExactlyInstanceOf(FailingFileIO.ArtificialException.class);
            }
            Path root = new Path(tempDir.toString());
            for (FileStatus bucketStatus : LocalFileIO.create().listStatus(root)) {
                assertThat(bucketStatus.isDir()).isTrue();
                assertThat(LocalFileIO.create().listStatus(bucketStatus.getPath())).isEmpty();
            }
        }
    }

    @Test
    public void testKeyProjection() throws Exception {
        DataFileTestDataGenerator.Data data = gen.next();
        KeyValueFileWriterFactory writerFactory = createWriterFactory(tempDir.toString(), "avro");
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();

        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
        writer.write(CloseableIterator.fromList(data.content, kv -> {}));
        writer.close();
        List<DataFileMeta> actualMetas = writer.result();

        // projection: (shopId, orderId) -> (orderId)
        KeyValueFileReaderFactory readerFactory =
                createReaderFactory(tempDir.toString(), "avro", new int[][] {new int[] {1}}, null);
        RowType projectedKeyType =
                RowType.builder()
                        .fields(
                                new DataType[] {new BigIntType(false)},
                                new String[] {"key_orderId"})
                        .build();
        InternalRowSerializer projectedKeySerializer = new InternalRowSerializer(projectedKeyType);
        assertData(
                data,
                actualMetas,
                projectedKeySerializer,
                TestKeyValueGenerator.DEFAULT_ROW_SERIALIZER,
                serializer,
                readerFactory,
                kv ->
                        new KeyValue()
                                .replace(
                                        GenericRow.of(kv.key().getLong(1)),
                                        kv.sequenceNumber(),
                                        kv.valueKind(),
                                        kv.value()));
    }

    @Test
    public void testValueProjection() throws Exception {
        DataFileTestDataGenerator.Data data = gen.next();
        KeyValueFileWriterFactory writerFactory = createWriterFactory(tempDir.toString(), "avro");
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();

        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
        writer.write(CloseableIterator.fromList(data.content, kv -> {}));
        writer.close();
        List<DataFileMeta> actualMetas = writer.result();

        // projection:
        // (dt, hr, shopId, orderId, itemId, priceAmount, comment) ->
        // (shopId, itemId, dt, hr)
        KeyValueFileReaderFactory readerFactory =
                createReaderFactory(
                        tempDir.toString(),
                        "avro",
                        null,
                        new int[][] {new int[] {2}, new int[] {4}, new int[] {0}, new int[] {1}});
        RowType projectedValueType =
                RowType.of(
                        new DataType[] {
                            new IntType(false),
                            new BigIntType(),
                            new VarCharType(false, 8),
                            new IntType(false)
                        },
                        new String[] {"shopId", "itemId", "dt", "hr"});
        InternalRowSerializer projectedValueSerializer =
                new InternalRowSerializer(projectedValueType);
        assertData(
                data,
                actualMetas,
                TestKeyValueGenerator.KEY_SERIALIZER,
                projectedValueSerializer,
                serializer,
                readerFactory,
                kv ->
                        new KeyValue()
                                .replace(
                                        kv.key(),
                                        kv.sequenceNumber(),
                                        kv.valueKind(),
                                        GenericRow.of(
                                                kv.value().getInt(2),
                                                kv.value().isNullAt(4)
                                                        ? null
                                                        : kv.value().getLong(4),
                                                kv.value().getString(0),
                                                kv.value().getInt(1))));
    }

    protected KeyValueFileWriterFactory createWriterFactory(String pathStr, String format) {
        Path path = new Path(pathStr);
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        path,
                        RowType.of(),
                        CoreOptions.PARTITION_DEFAULT_NAME.defaultValue(),
                        format);
        int suggestedFileSize = ThreadLocalRandom.current().nextInt(8192) + 1024;
        FileIO fileIO = FileIOFinder.find(path);
        Options options = new Options();
        options.set(CoreOptions.METADATA_STATS_MODE, "FULL");

        Map<String, FileStorePathFactory> pathFactoryMap = new HashMap<>();
        pathFactoryMap.put(format, pathFactory);
        pathFactoryMap.put(
                CoreOptions.FILE_FORMAT.defaultValue().toString(),
                new FileStorePathFactory(
                        path,
                        RowType.of(),
                        CoreOptions.PARTITION_DEFAULT_NAME.defaultValue(),
                        CoreOptions.FILE_FORMAT.defaultValue().toString()));

        return KeyValueFileWriterFactory.builder(
                        fileIO,
                        0,
                        KEY_TYPE,
                        DEFAULT_ROW_TYPE,
                        // normal format will buffer changes in memory and we can't determine
                        // if the written file size is really larger than suggested, so we use a
                        // special format which flushes for every added element
                        new FlushingFileFormat(format),
                        pathFactoryMap,
                        suggestedFileSize)
                .build(BinaryRow.EMPTY_ROW, 0, new CoreOptions(options));
    }

    private KeyValueFileReaderFactory createReaderFactory(
            String pathStr, String format, int[][] keyProjection, int[][] valueProjection) {
        Path path = new Path(pathStr);
        FileIO fileIO = FileIOFinder.find(path);
        FileStorePathFactory pathFactory = createNonPartFactory(path);
        KeyValueFileReaderFactory.Builder builder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        createTestSchemaManager(path),
                        createTestSchemaManager(path).schema(0),
                        KEY_TYPE,
                        DEFAULT_ROW_TYPE,
                        ignore -> new FlushingFileFormat(format),
                        pathFactory,
                        new TestKeyValueGenerator.TestKeyValueFieldsExtractor(),
                        new CoreOptions(new HashMap<>()));
        if (keyProjection != null) {
            builder.withKeyProjection(keyProjection);
        }
        if (valueProjection != null) {
            builder.withValueProjection(valueProjection);
        }
        return builder.build(BinaryRow.EMPTY_ROW, 0, DeletionVector.emptyFactory());
    }

    private void assertData(
            DataFileTestDataGenerator.Data data,
            List<DataFileMeta> actualMetas,
            InternalRowSerializer keySerializer,
            InternalRowSerializer projectedValueSerializer,
            DataFileMetaSerializer dataFileMetaSerializer,
            KeyValueFileReaderFactory readerFactory,
            Function<KeyValue, KeyValue> toExpectedKv)
            throws Exception {
        Iterator<KeyValue> expectedIterator = data.content.iterator();
        for (DataFileMeta meta : actualMetas) {
            // check the contents of data file
            CloseableIterator<KeyValue> actualKvsIterator =
                    new RecordReaderIterator<>(
                            readerFactory.createRecordReader(
                                    meta.schemaId(),
                                    meta.fileName(),
                                    meta.fileSize(),
                                    meta.level()));
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
                assertThat(actualKv.level()).isEqualTo(meta.level());
            }
            actualKvsIterator.close();

            // check that each data file meta is serializable
            assertThat(dataFileMetaSerializer.fromRow(dataFileMetaSerializer.toRow(meta)))
                    .isEqualTo(meta);
        }
        assertThat(expectedIterator.hasNext()).isFalse();
    }

    private void checkRollingFiles(
            DataFileMeta expected, List<DataFileMeta> actual, long suggestedFileSize) {
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
        SimpleColStats[] keyStats = convertWithoutSchemaEvolution(expected.keyStats(), KEY_TYPE);
        for (int i = 0; i < KEY_TYPE.getFieldCount(); i++) {
            int idx = i;
            StatsTestUtils.checkRollingFileStats(
                    keyStats[i],
                    actual,
                    m -> convertWithoutSchemaEvolution(m.keyStats(), KEY_TYPE)[idx]);
        }
        for (int i = 0; i < DEFAULT_ROW_TYPE.getFieldCount(); i++) {
            int idx = i;
            StatsTestUtils.checkRollingFileStats(
                    convertWithoutSchemaEvolution(expected.valueStats(), DEFAULT_ROW_TYPE)[i],
                    actual,
                    m -> convertWithoutSchemaEvolution(m.valueStats(), DEFAULT_ROW_TYPE)[idx]);
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

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "avro"})
    public void testReaderUseFileSizeFromMetadata(String format) throws Exception {
        DataFileTestDataGenerator.Data data = gen.next();
        KeyValueFileWriterFactory writerFactory = createWriterFactory(tempDir.toString(), format);
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();

        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
        writer.write(CloseableIterator.fromList(data.content, kv -> {}));
        writer.close();
        List<DataFileMeta> actualMetas = writer.result();

        KeyValueFileReaderFactory readerFactory =
                createReaderFactory(tempDir.toString(), format, null, null);
        assertData(
                data,
                actualMetas,
                TestKeyValueGenerator.KEY_SERIALIZER,
                TestKeyValueGenerator.DEFAULT_ROW_SERIALIZER,
                serializer,
                readerFactory,
                kv -> kv);
    }
}
