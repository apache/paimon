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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeletionVectorsIndexFile}. */
public class DeletionVectorsIndexFileTest {

    @TempDir java.nio.file.Path tempPath;

    @ParameterizedTest
    @MethodSource("bitmap64AndKeyTypes")
    public void testReadDvIndex(boolean bitmap64, KeyType keyType) {
        IndexPathFactory pathFactory = getPathFactory();

        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);

        // write
        Map<DeletionFileKey, DeletionVector> deleteMap = new HashMap<>();
        DeletionFileKey key1 = keyType.key(1);
        DeletionVector index1 = createEmptyDV(bitmap64);
        index1.delete(1);
        deleteMap.put(key1, index1);

        DeletionFileKey key2 = keyType.key(2);
        DeletionVector index2 = createEmptyDV(bitmap64);
        index2.delete(2);
        index2.delete(3);
        deleteMap.put(key2, index2);

        DeletionFileKey key33 = keyType.key(33);
        DeletionVector index3 = createEmptyDV(bitmap64);
        index3.delete(3);
        deleteMap.put(key33, index3);

        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(deleteMap);
        assertThat(indexFiles.size()).isEqualTo(1);

        // read
        IndexFileMeta file = indexFiles.get(0);
        Map<DeletionFileKey, DeletionVector> actualDeleteMap =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(actualDeleteMap.get(key1).isDeleted(1)).isTrue();
        assertThat(actualDeleteMap.get(key1).isDeleted(2)).isFalse();
        assertThat(actualDeleteMap.get(key2).isDeleted(2)).isTrue();
        assertThat(actualDeleteMap.get(key2).isDeleted(3)).isTrue();
        assertThat(actualDeleteMap.get(key33).isDeleted(3)).isTrue();

        // delete
        deletionVectorsIndexFile.delete(file);
        assertThat(deletionVectorsIndexFile.exists(file)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("bitmap64AndKeyTypes")
    public void testReadSingleDvIndex(boolean bitmap64, KeyType keyType) {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);
        DeletionFileKey key = keyType.key(10);
        DeletionVector deletionVector = createEmptyDV(bitmap64);
        deletionVector.delete(2);
        deletionVector.delete(5);
        Map<DeletionFileKey, DeletionVector> deleteMap = new HashMap<>();
        deleteMap.put(key, deletionVector);

        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(deleteMap);

        assertThat(indexFiles).hasSize(1);
        Map<DeletionFileKey, DeletionVector> actual =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(actual).containsOnlyKeys(key);
        assertThat(actual.get(key).isDeleted(2)).isTrue();
        assertThat(actual.get(key).isDeleted(5)).isTrue();
        assertThat(actual.get(key).isDeleted(6)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("bitmap64AndKeyTypes")
    public void testReadDvIndexWithCopiousDv(boolean bitmap64, KeyType keyType) {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);

        // write
        Random random = new Random();
        Map<DeletionFileKey, DeletionVector> deleteMap = new HashMap<>();
        Map<DeletionFileKey, Integer> deleteInteger = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            DeletionVector index = createEmptyDV(bitmap64);
            int num = random.nextInt(1000000);
            index.delete(num);
            DeletionFileKey key = keyType.key(i);
            deleteMap.put(key, index);
            deleteInteger.put(key, num);
        }

        // read
        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(deleteMap);
        assertThat(indexFiles.size()).isEqualTo(1);
        Map<DeletionFileKey, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(dvs.size()).isEqualTo(100000);
        for (Map.Entry<DeletionFileKey, Integer> entry : deleteInteger.entrySet()) {
            DeletionVector deletionVector = dvs.get(entry.getKey());
            assertThat(deletionVector.isDeleted(entry.getValue())).isTrue();
            assertThat(deletionVector.isDeleted(entry.getValue() + 1)).isFalse();
        }
    }

    @ParameterizedTest
    @MethodSource("bitmap64AndKeyTypes")
    public void testReadDvIndexWithEnormousDv(boolean bitmap64, KeyType keyType) {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);

        // write
        Random random = new Random();
        Map<DeletionFileKey, DeletionVector> fileToDV = new HashMap<>();
        Map<DeletionFileKey, Long> fileToCardinality = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            DeletionVector index = createEmptyDV(bitmap64);
            // the size of dv index file is about 20M
            for (int j = 0; j < 10000000; j++) {
                index.delete(random.nextInt(Integer.MAX_VALUE));
            }
            DeletionFileKey key = keyType.key(i);
            fileToCardinality.put(key, index.getCardinality());
            fileToDV.put(key, index);
        }
        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(fileToDV);

        // read
        assertThat(indexFiles.size()).isEqualTo(1);
        Map<DeletionFileKey, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(dvs.size()).isEqualTo(5);
        for (Map.Entry<DeletionFileKey, Long> entry : fileToCardinality.entrySet()) {
            assertThat(dvs.get(entry.getKey()).getCardinality()).isEqualTo(entry.getValue());
        }
    }

    @ParameterizedTest
    @MethodSource("bitmap64AndKeyTypes")
    public void testWriteDVIndexWithLimitedTargetSizePerIndexFile(
            boolean bitmap64, KeyType keyType) {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, MemorySize.parse("2MB"), bitmap64);

        // write1
        Random random = new Random();
        Map<DeletionFileKey, DeletionVector> fileToDV = new HashMap<>();
        Map<DeletionFileKey, Long> fileToCardinality = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            DeletionVector index = createEmptyDV(bitmap64);
            // the size of dv index file is about 1.7M
            for (int j = 0; j < 750000; j++) {
                index.delete(random.nextInt(Integer.MAX_VALUE));
            }
            DeletionFileKey key = keyType.key(i);
            fileToCardinality.put(key, index.getCardinality());
            fileToDV.put(key, index);
        }
        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(fileToDV);

        // assert 1
        assertThat(indexFiles.size()).isEqualTo(3);
        Map<DeletionFileKey, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        for (Map.Entry<DeletionFileKey, Long> entry : fileToCardinality.entrySet()) {
            assertThat(dvs.get(entry.getKey()).getCardinality()).isEqualTo(entry.getValue());
        }

        // write2
        fileToDV.clear();
        fileToCardinality.clear();
        for (int i = 0; i < 10; i++) {
            DeletionVector index = createEmptyDV(bitmap64);
            // the size of dv index file is about 0.42M
            for (int j = 0; j < 100000; j++) {
                index.delete(random.nextInt(Integer.MAX_VALUE));
            }
            DeletionFileKey key = keyType.key(i);
            fileToCardinality.put(key, index.getCardinality());
            fileToDV.put(key, index);
        }
        indexFiles = deletionVectorsIndexFile.writeWithRolling(fileToDV);

        // assert 2
        assertThat(indexFiles.size()).isGreaterThan(1);
        dvs = deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        for (Map.Entry<DeletionFileKey, Long> entry : fileToCardinality.entrySet()) {
            assertThat(dvs.get(entry.getKey()).getCardinality()).isEqualTo(entry.getValue());
        }
    }

    @ParameterizedTest
    @MethodSource("keyTypes")
    public void testReadV1AndV2(KeyType keyType) {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile v1DeletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, false);
        DeletionVectorsIndexFile v2DeletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, true);

        // write v1 dv
        Random random = new Random();
        Map<DeletionFileKey, Integer> deleteInteger = new HashMap<>();

        Map<DeletionFileKey, DeletionVector> deleteMap1 = new HashMap<>();
        for (int i = 0; i < 50000; i++) {
            DeletionVector index = createEmptyDV(false);
            int num = random.nextInt(1000000);
            index.delete(num);
            DeletionFileKey key = keyType.key(i);
            deleteMap1.put(key, index);
            deleteInteger.put(key, num);
        }
        List<IndexFileMeta> indexFiles1 = v1DeletionVectorsIndexFile.writeWithRolling(deleteMap1);
        assertThat(indexFiles1.size()).isEqualTo(1);

        // write v2 dv
        Map<DeletionFileKey, DeletionVector> deleteMap2 = new HashMap<>();
        for (int i = 50000; i < 100000; i++) {
            DeletionVector index = createEmptyDV(true);
            int num = random.nextInt(1000000);
            index.delete(num);
            DeletionFileKey key = keyType.key(i);
            deleteMap2.put(key, index);
            deleteInteger.put(key, num);
        }
        List<IndexFileMeta> indexFiles2 = v2DeletionVectorsIndexFile.writeWithRolling(deleteMap2);
        assertThat(indexFiles2.size()).isEqualTo(1);

        List<IndexFileMeta> totalIndexFiles =
                Stream.concat(indexFiles1.stream(), indexFiles2.stream())
                        .collect(Collectors.toList());
        // read when writeVersionID is V1
        Map<DeletionFileKey, DeletionVector> dvs1 =
                v1DeletionVectorsIndexFile.readAllDeletionVectors(totalIndexFiles);
        assertThat(dvs1.size()).isEqualTo(100000);
        for (Map.Entry<DeletionFileKey, Integer> entry : deleteInteger.entrySet()) {
            DeletionVector deletionVector = dvs1.get(entry.getKey());
            assertThat(deletionVector.isDeleted(entry.getValue())).isTrue();
            assertThat(deletionVector.isDeleted(entry.getValue() + 1)).isFalse();
        }

        // read when writeVersionID is V2
        Map<DeletionFileKey, DeletionVector> dvs2 =
                v2DeletionVectorsIndexFile.readAllDeletionVectors(totalIndexFiles);
        assertThat(dvs2.size()).isEqualTo(100000);
    }

    @ParameterizedTest
    @MethodSource("bitmap64AndKeyTypes")
    public void testReadAllDeletionVectorsWithOutOfOrderDvRanges(
            boolean bitmap64, KeyType keyType) {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);

        // write multiple DVs so they are stored sequentially in the index file
        Map<DeletionFileKey, DeletionVector> deleteMap = new HashMap<>();
        Map<DeletionFileKey, Integer> expected = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            DeletionVector dv = createEmptyDV(bitmap64);
            dv.delete(i * 100);
            dv.delete(i * 100 + 1);
            DeletionFileKey key = keyType.key(i);
            deleteMap.put(key, dv);
            expected.put(key, i * 100);
        }

        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(deleteMap);
        assertThat(indexFiles.size()).isEqualTo(1);
        IndexFileMeta original = indexFiles.get(0);

        // build a new IndexFileMeta with dvRanges in reverse offset order,
        // simulating compaction merging dvRanges from multiple sources
        LinkedHashMap<DeletionFileKey, DeletionVectorMeta> originalRanges = original.dvRanges();
        List<Map.Entry<DeletionFileKey, DeletionVectorMeta>> entries =
                new ArrayList<>(originalRanges.entrySet());
        Collections.reverse(entries);
        LinkedHashMap<DeletionFileKey, DeletionVectorMeta> reversedRanges = new LinkedHashMap<>();
        for (Map.Entry<DeletionFileKey, DeletionVectorMeta> entry : entries) {
            reversedRanges.put(entry.getKey(), entry.getValue());
        }

        IndexFileMeta reordered =
                new IndexFileMeta(
                        original.indexType(),
                        original.fileName(),
                        original.fileSize(),
                        original.rowCount(),
                        reversedRanges,
                        original.externalPath());

        // read with out-of-order dvRanges — this would fail without the seek fix
        Map<DeletionFileKey, DeletionVector> result =
                deletionVectorsIndexFile.readAllDeletionVectors(reordered);
        assertThat(result).hasSize(10);
        for (Map.Entry<DeletionFileKey, Integer> e : expected.entrySet()) {
            DeletionVector deletionVector = result.get(e.getKey());
            assertThat(deletionVector.isDeleted(e.getValue())).isTrue();
            assertThat(deletionVector.isDeleted(e.getValue() + 1)).isTrue();
            assertThat(deletionVector.isDeleted(e.getValue() + 2)).isFalse();
        }
    }

    @Test
    public void testReadEmptyDeletionVectorsAfterSerialization() {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, false);
        IndexFileMeta emptyFile = deletionVectorsIndexFile.writeSingleFile(Collections.emptyMap());
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        InternalRow row = serializer.toRow(emptyFile);
        IndexFileMeta serializedEmptyFile = serializer.fromRow(row);

        assertThat(emptyFile.dvRanges()).isEmpty();
        assertThat(row.isNullAt(4)).isTrue();
        assertThat(row.isNullAt(7)).isTrue();
        assertThat(serializedEmptyFile.dvRanges()).isEmpty();
        assertThat(deletionVectorsIndexFile.readAllDeletionVectors(serializedEmptyFile)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("bitmap64AndKeyTypes")
    public void testReadDeletionFile(boolean bitmap64, KeyType keyType) throws IOException {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);

        Map<DeletionFileKey, DeletionVector> deleteMap = new HashMap<>();
        DeletionFileKey key = keyType.key(1);
        DeletionVector index1 = createEmptyDV(bitmap64);
        index1.delete(1);
        index1.delete(10);
        index1.delete(100);
        deleteMap.put(key, index1);

        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(deleteMap);
        assertThat(indexFiles.size()).isEqualTo(1);

        IndexFileMeta indexFileMeta = indexFiles.get(0);
        DeletionVectorMeta deletionVectorMeta = indexFileMeta.dvRanges().get(key);

        DeletionFile deletionFile =
                new DeletionFile(
                        pathFactory.toPath(indexFileMeta).toString(),
                        deletionVectorMeta.offset(),
                        deletionVectorMeta.length(),
                        deletionVectorMeta.cardinality());

        // test DeletionVector#read()
        DeletionVector dv = DeletionVector.read(LocalFileIO.create(), deletionFile);
        assertThat(dv.isDeleted(1)).isTrue();
        assertThat(dv.isDeleted(10)).isTrue();
        assertThat(dv.isDeleted(100)).isTrue();
    }

    @Test
    public void testReadOldDeletionVector32Bit() throws IOException {
        try (InputStream inputStream =
                DeletionVectorsIndexFile.class
                        .getClassLoader()
                        .getResourceAsStream("compatibility/dvindex-32")) {
            // read version
            assertThat(inputStream.read()).isEqualTo(1);
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            DeletionVector dv1 = DeletionVector.read(dataInputStream, 24L);
            assertThat(dv1).isInstanceOf(BitmapDeletionVector.class);
            DeletionVector dv2 = DeletionVector.read(dataInputStream, 22L);
            assertThat(dv2).isInstanceOf(BitmapDeletionVector.class);
            assertThat(dv1.getCardinality()).isEqualTo(2L);
            assertThat(dv2.getCardinality()).isEqualTo(1L);
            assertThat(dv1.isDeleted(2)).isTrue();
            assertThat(dv1.isDeleted(3)).isTrue();
            assertThat(dv2.isDeleted(1)).isTrue();
        }
    }

    @Test
    public void testReadOldDeletionVector64Bit() throws IOException {
        try (InputStream inputStream =
                DeletionVectorsIndexFile.class
                        .getClassLoader()
                        .getResourceAsStream("compatibility/dvindex-64")) {
            assertThat(inputStream.read()).isEqualTo(1);
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            DeletionVector dv1 = DeletionVector.read(dataInputStream, 44L);
            assertThat(dv1).isInstanceOf(Bitmap64DeletionVector.class);
            DeletionVector dv2 = DeletionVector.read(dataInputStream, 42L);
            assertThat(dv2).isInstanceOf(Bitmap64DeletionVector.class);
            assertThat(dv1.getCardinality()).isEqualTo(2L);
            assertThat(dv2.getCardinality()).isEqualTo(1L);
            assertThat(dv1.isDeleted(2)).isTrue();
            assertThat(dv1.isDeleted(3)).isTrue();
            assertThat(dv2.isDeleted(1)).isTrue();
        }
    }

    private static Stream<Arguments> bitmap64AndKeyTypes() {
        return Stream.of(false, true)
                .flatMap(
                        bitmap64 ->
                                Stream.of(KeyType.values())
                                        .map(keyType -> Arguments.of(bitmap64, keyType)));
    }

    private static Stream<KeyType> keyTypes() {
        return Stream.of(KeyType.values());
    }

    private enum KeyType {
        FILE_NAME {
            @Override
            DeletionFileKey key(int id) {
                return DeletionFileKey.ofFileName("file" + id + ".parquet");
            }
        },
        ROW_ID_RANGE {
            @Override
            DeletionFileKey key(int id) {
                long start = id * 10L;
                return DeletionFileKey.ofRange(new Range(start, start + 9));
            }
        };

        abstract DeletionFileKey key(int id);
    }

    private DeletionVector createEmptyDV(boolean bitmap64) {
        return bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
    }

    private DeletionVectorsIndexFile deletionVectorsIndexFile(
            IndexPathFactory pathFactory, boolean bitmap64) {
        return deletionVectorsIndexFile(pathFactory, MemorySize.ofBytes(Long.MAX_VALUE), bitmap64);
    }

    private DeletionVectorsIndexFile deletionVectorsIndexFile(
            IndexPathFactory pathFactory, MemorySize targetSizePerIndexFile, boolean bitmap64) {
        return new DeletionVectorsIndexFile(
                LocalFileIO.create(), pathFactory, targetSizePerIndexFile, bitmap64);
    }

    private IndexPathFactory getPathFactory() {
        Path dir = new Path(tempPath.toUri());
        return new IndexPathFactory() {
            @Override
            public Path toPath(String fileName) {
                return new Path(dir, fileName);
            }

            @Override
            public Path newPath() {
                return new Path(dir, UUID.randomUUID().toString());
            }

            @Override
            public boolean isExternalPath() {
                return false;
            }
        };
    }
}
