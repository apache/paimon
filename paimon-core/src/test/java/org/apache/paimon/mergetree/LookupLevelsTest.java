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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.hash.HashLookupStoreFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SchemaEvolutionTableTestBase;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.TARGET_FILE_SIZE;
import static org.apache.paimon.KeyValue.UNKNOWN_SEQUENCE;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link LookupLevels}. */
public class LookupLevelsTest {

    private static final String LOOKUP_FILE_PREFIX = "lookup-";

    @TempDir java.nio.file.Path tempDir;

    private final Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));

    private final RowType keyType = DataTypes.ROW(DataTypes.FIELD(0, "_key", DataTypes.INT()));
    private final RowType rowType =
            DataTypes.ROW(
                    DataTypes.FIELD(0, "key", DataTypes.INT()),
                    DataTypes.FIELD(1, "value", DataTypes.INT()));

    @Test
    public void testMultiLevels() throws IOException {
        Levels levels =
                new Levels(
                        comparator,
                        Arrays.asList(
                                newFile(1, kv(1, 11, 1), kv(3, 33, 2), kv(5, 5, 3)),
                                newFile(2, kv(2, 22, 4), kv(5, 55, 5))),
                        3);
        LookupLevels<KeyValue> lookupLevels =
                createLookupLevels(levels, MemorySize.ofMebiBytes(10));

        // only in level 1
        KeyValue kv = lookupLevels.lookup(row(1), 1);
        assertThat(kv).isNotNull();
        assertThat(kv.sequenceNumber()).isEqualTo(1);
        assertThat(kv.level()).isEqualTo(1);
        assertThat(kv.value().getInt(1)).isEqualTo(11);

        // only in level 2
        kv = lookupLevels.lookup(row(2), 1);
        assertThat(kv).isNotNull();
        assertThat(kv.sequenceNumber()).isEqualTo(4);
        assertThat(kv.level()).isEqualTo(2);
        assertThat(kv.value().getInt(1)).isEqualTo(22);

        // both in level 1 and level 2
        kv = lookupLevels.lookup(row(5), 1);
        assertThat(kv).isNotNull();
        assertThat(kv.sequenceNumber()).isEqualTo(3);
        assertThat(kv.level()).isEqualTo(1);
        assertThat(kv.value().getInt(1)).isEqualTo(5);

        // no exists
        kv = lookupLevels.lookup(row(4), 1);
        assertThat(kv).isNull();

        lookupLevels.close();
        assertThat(lookupLevels.lookupFiles().estimatedSize()).isEqualTo(0);
    }

    @Test
    public void testMultiFiles() throws IOException {
        Levels levels =
                new Levels(
                        comparator,
                        Arrays.asList(
                                newFile(1, kv(1, 11), kv(2, 22)),
                                newFile(1, kv(4, 44), kv(5, 55)),
                                newFile(1, kv(7, 77), kv(8, 88)),
                                newFile(1, kv(10, 1010), kv(11, 1111))),
                        1);
        LookupLevels<KeyValue> lookupLevels =
                createLookupLevels(levels, MemorySize.ofMebiBytes(10));

        Map<Integer, Integer> contains =
                new HashMap<Integer, Integer>() {
                    {
                        this.put(1, 11);
                        this.put(2, 22);
                        this.put(4, 44);
                        this.put(5, 55);
                        this.put(7, 77);
                        this.put(8, 88);
                        this.put(10, 1010);
                        this.put(11, 1111);
                    }
                };
        for (Map.Entry<Integer, Integer> entry : contains.entrySet()) {
            KeyValue kv = lookupLevels.lookup(row(entry.getKey()), 1);
            assertThat(kv).isNotNull();
            assertThat(kv.sequenceNumber()).isEqualTo(UNKNOWN_SEQUENCE);
            assertThat(kv.level()).isEqualTo(1);
            assertThat(kv.value().getInt(1)).isEqualTo(entry.getValue());
        }

        int[] notContains = new int[] {0, 3, 6, 9, 12};
        for (int key : notContains) {
            KeyValue kv = lookupLevels.lookup(row(key), 1);
            assertThat(kv).isNull();
        }

        lookupLevels.close();
        assertThat(lookupLevels.lookupFiles().estimatedSize()).isEqualTo(0);
    }

    @Test
    public void testMaxDiskSize() throws IOException {
        List<DataFileMeta> files = new ArrayList<>();
        int fileNum = 10;
        int recordInFile = 100;
        for (int i = 0; i < fileNum; i++) {
            List<KeyValue> kvs = new ArrayList<>();
            for (int j = 0; j < recordInFile; j++) {
                int key = i * recordInFile + j;
                kvs.add(kv(key, key));
            }
            files.add(newFile(1, kvs.toArray(new KeyValue[0])));
        }
        Levels levels = new Levels(comparator, files, 1);
        LookupLevels<KeyValue> lookupLevels =
                createLookupLevels(levels, MemorySize.ofKibiBytes(20));

        for (int i = 0; i < fileNum * recordInFile; i++) {
            KeyValue kv = lookupLevels.lookup(row(i), 1);
            assertThat(kv).isNotNull();
            assertThat(kv.sequenceNumber()).isEqualTo(UNKNOWN_SEQUENCE);
            assertThat(kv.level()).isEqualTo(1);
            assertThat(kv.value().getInt(1)).isEqualTo(i);
        }

        // some files are invalided
        long fileNumber = lookupLevels.lookupFiles().estimatedSize();
        String[] lookupFiles =
                tempDir.toFile().list((dir, name) -> name.startsWith(LOOKUP_FILE_PREFIX));
        assertThat(lookupFiles).isNotNull();
        assertThat(fileNumber).isNotEqualTo(fileNum).isEqualTo(lookupFiles.length);

        lookupLevels.close();
        assertThat(lookupLevels.lookupFiles().estimatedSize()).isEqualTo(0);
    }

    @Test
    public void testLookupEmptyLevel() throws IOException {
        Levels levels =
                new Levels(
                        comparator,
                        Arrays.asList(
                                newFile(1, kv(1, 11), kv(3, 33), kv(5, 5)),
                                // empty level 2
                                newFile(3, kv(2, 22), kv(5, 55))),
                        3);
        LookupLevels<KeyValue> lookupLevels =
                createLookupLevels(levels, MemorySize.ofMebiBytes(10));

        KeyValue kv = lookupLevels.lookup(row(2), 1);
        assertThat(kv).isNotNull();
    }

    @Test
    public void testLookupLevel0() throws Exception {
        Levels levels =
                new Levels(
                        comparator,
                        Arrays.asList(
                                newFile(0, kv(1, 0)),
                                newFile(1, kv(1, 11), kv(3, 33), kv(5, 5)),
                                newFile(2, kv(2, 22), kv(5, 55))),
                        3);
        LookupLevels<KeyValue> lookupLevels =
                createLookupLevels(levels, MemorySize.ofMebiBytes(10));

        KeyValue kv = lookupLevels.lookup(row(1), 0);
        assertThat(kv).isNotNull();
        assertThat(kv.sequenceNumber()).isEqualTo(UNKNOWN_SEQUENCE);
        assertThat(kv.level()).isEqualTo(0);
        assertThat(kv.value().getInt(1)).isEqualTo(0);

        levels =
                new Levels(
                        comparator,
                        Arrays.asList(
                                newFile(1, kv(1, 11), kv(3, 33), kv(5, 5)),
                                newFile(2, kv(2, 22), kv(5, 55))),
                        3);
        lookupLevels = createLookupLevels(levels, MemorySize.ofMebiBytes(10));

        // not in level 0
        kv = lookupLevels.lookup(row(1), 0);
        assertThat(kv).isNotNull();
        assertThat(kv.sequenceNumber()).isEqualTo(UNKNOWN_SEQUENCE);
        assertThat(kv.level()).isEqualTo(1);
        assertThat(kv.value().getInt(1)).isEqualTo(11);
    }

    private LookupLevels<KeyValue> createLookupLevels(Levels levels, MemorySize maxDiskSize) {
        return new LookupLevels<>(
                levels,
                comparator,
                keyType,
                new LookupLevels.KeyValueProcessor(rowType),
                file ->
                        createReaderFactory()
                                .createRecordReader(
                                        0, file.fileName(), file.fileSize(), file.level()),
                () -> new File(tempDir.toFile(), LOOKUP_FILE_PREFIX + UUID.randomUUID()),
                new HashLookupStoreFactory(
                        new CacheManager(MemorySize.ofMebiBytes(1)), 2048, 0.75, "none"),
                Duration.ofHours(1),
                maxDiskSize,
                rowCount -> BloomFilter.builder(rowCount, 0.05));
    }

    private KeyValue kv(int key, int value) {
        return kv(key, value, UNKNOWN_SEQUENCE);
    }

    private KeyValue kv(int key, int value, long seqNumber) {
        return new KeyValue()
                .replace(GenericRow.of(key), seqNumber, RowKind.INSERT, GenericRow.of(key, value));
    }

    private DataFileMeta newFile(int level, KeyValue... records) throws IOException {
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                createWriterFactory().createRollingMergeTreeFileWriter(level, FileSource.APPEND);
        for (KeyValue kv : records) {
            writer.write(kv);
        }
        writer.close();
        return writer.result().get(0);
    }

    private KeyValueFileWriterFactory createWriterFactory() {
        Path path = new Path(tempDir.toUri().toString());
        String identifier = "avro";
        Map<String, FileStorePathFactory> pathFactoryMap = new HashMap<>();
        pathFactoryMap.put(identifier, createNonPartFactory(path));
        return KeyValueFileWriterFactory.builder(
                        FileIOFinder.find(path),
                        0,
                        keyType,
                        rowType,
                        new FlushingFileFormat(identifier),
                        pathFactoryMap,
                        TARGET_FILE_SIZE.defaultValue().getBytes())
                .build(BinaryRow.EMPTY_ROW, 0, new CoreOptions(new Options()));
    }

    private KeyValueFileReaderFactory createReaderFactory() {
        Path path = new Path(tempDir.toUri().toString());
        KeyValueFileReaderFactory.Builder builder =
                KeyValueFileReaderFactory.builder(
                        FileIOFinder.find(path),
                        createSchemaManager(path),
                        createSchemaManager(path).schema(0),
                        keyType,
                        rowType,
                        ignore -> new FlushingFileFormat("avro"),
                        createNonPartFactory(path),
                        new KeyValueFieldsExtractor() {
                            @Override
                            public List<DataField> keyFields(TableSchema schema) {
                                return keyType.getFields();
                            }

                            @Override
                            public List<DataField> valueFields(TableSchema schema) {
                                return schema.fields();
                            }
                        },
                        new CoreOptions(new HashMap<>()));
        return builder.build(BinaryRow.EMPTY_ROW, 0, DeletionVector.emptyFactory());
    }

    private SchemaManager createSchemaManager(Path path) {
        TableSchema tableSchema =
                new TableSchema(
                        0,
                        rowType.getFields(),
                        rowType.getFieldCount(),
                        Collections.emptyList(),
                        Collections.singletonList("key"),
                        Collections.emptyMap(),
                        "");
        Map<Long, TableSchema> schemas = new HashMap<>();
        schemas.put(tableSchema.id(), tableSchema);
        return new SchemaEvolutionTableTestBase.TestingSchemaManager(path, schemas);
    }
}
