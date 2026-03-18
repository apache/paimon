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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.source.DeletionFile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
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
    @ValueSource(booleans = {false, true})
    public void testReadDvIndex(boolean bitmap64) {
        IndexPathFactory pathFactory = getPathFactory();

        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);

        // write
        HashMap<String, DeletionVector> deleteMap = new HashMap<>();
        DeletionVector index1 = createEmptyDV(bitmap64);
        index1.delete(1);
        deleteMap.put("file1.parquet", index1);

        DeletionVector index2 = createEmptyDV(bitmap64);
        index2.delete(2);
        index2.delete(3);
        deleteMap.put("file2.parquet", index2);

        DeletionVector index3 = createEmptyDV(bitmap64);
        index3.delete(3);
        deleteMap.put("file33.parquet", index3);

        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(deleteMap);
        assertThat(indexFiles.size()).isEqualTo(1);

        // read
        IndexFileMeta file = indexFiles.get(0);
        Map<String, DeletionVector> actualDeleteMap =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(actualDeleteMap.get("file1.parquet").isDeleted(1)).isTrue();
        assertThat(actualDeleteMap.get("file1.parquet").isDeleted(2)).isFalse();
        assertThat(actualDeleteMap.get("file2.parquet").isDeleted(2)).isTrue();
        assertThat(actualDeleteMap.get("file2.parquet").isDeleted(3)).isTrue();
        assertThat(actualDeleteMap.get("file33.parquet").isDeleted(3)).isTrue();

        // delete
        deletionVectorsIndexFile.delete(file);
        assertThat(deletionVectorsIndexFile.exists(file)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReadDvIndexWithCopiousDv(boolean bitmap64) {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);

        // write
        Random random = new Random();
        HashMap<String, DeletionVector> deleteMap = new HashMap<>();
        HashMap<String, Integer> deleteInteger = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            DeletionVector index = createEmptyDV(bitmap64);
            int num = random.nextInt(1000000);
            index.delete(num);
            deleteMap.put(String.format("file%s.parquet", i), index);
            deleteInteger.put(String.format("file%s.parquet", i), num);
        }

        // read
        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(deleteMap);
        assertThat(indexFiles.size()).isEqualTo(1);
        Map<String, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(dvs.size()).isEqualTo(100000);
        for (String file : dvs.keySet()) {
            int delete = deleteInteger.get(file);
            assertThat(dvs.get(file).isDeleted(delete)).isTrue();
            assertThat(dvs.get(file).isDeleted(delete + 1)).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReadDvIndexWithEnormousDv(boolean bitmap64) {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);

        // write
        Random random = new Random();
        Map<String, DeletionVector> fileToDV = new HashMap<>();
        Map<String, Long> fileToCardinality = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            DeletionVector index = createEmptyDV(bitmap64);
            // the size of dv index file is about 20M
            for (int j = 0; j < 10000000; j++) {
                index.delete(random.nextInt(Integer.MAX_VALUE));
            }
            fileToCardinality.put("f" + i, index.getCardinality());
            fileToDV.put("f" + i, index);
        }
        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(fileToDV);

        // read
        assertThat(indexFiles.size()).isEqualTo(1);
        Map<String, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(dvs.size()).isEqualTo(5);
        for (String file : dvs.keySet()) {
            assertThat(dvs.get(file).getCardinality()).isEqualTo(fileToCardinality.get(file));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWriteDVIndexWithLimitedTargetSizePerIndexFile(boolean bitmap64) {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, MemorySize.parse("2MB"), bitmap64);

        // write1
        Random random = new Random();
        Map<String, DeletionVector> fileToDV = new HashMap<>();
        Map<String, Long> fileToCardinality = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            DeletionVector index = createEmptyDV(bitmap64);
            // the size of dv index file is about 1.7M
            for (int j = 0; j < 750000; j++) {
                index.delete(random.nextInt(Integer.MAX_VALUE));
            }
            fileToCardinality.put("f" + i, index.getCardinality());
            fileToDV.put("f" + i, index);
        }
        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(fileToDV);

        // assert 1
        assertThat(indexFiles.size()).isEqualTo(3);
        Map<String, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        for (String file : dvs.keySet()) {
            assertThat(dvs.get(file).getCardinality()).isEqualTo(fileToCardinality.get(file));
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
            fileToCardinality.put("f" + i, index.getCardinality());
            fileToDV.put("f" + i, index);
        }
        indexFiles = deletionVectorsIndexFile.writeWithRolling(fileToDV);

        // assert 2
        assertThat(indexFiles.size()).isGreaterThan(1);
        dvs = deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        for (String file : dvs.keySet()) {
            assertThat(dvs.get(file).getCardinality()).isEqualTo(fileToCardinality.get(file));
        }
    }

    @Test
    public void testReadV1AndV2() {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile v1DeletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, false);
        DeletionVectorsIndexFile v2DeletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, true);

        // write v1 dv
        Random random = new Random();
        HashMap<String, Integer> deleteInteger = new HashMap<>();

        HashMap<String, DeletionVector> deleteMap1 = new HashMap<>();
        for (int i = 0; i < 50000; i++) {
            DeletionVector index = createEmptyDV(false);
            int num = random.nextInt(1000000);
            index.delete(num);
            deleteMap1.put(String.format("file%s.parquet", i), index);
            deleteInteger.put(String.format("file%s.parquet", i), num);
        }
        List<IndexFileMeta> indexFiles1 = v1DeletionVectorsIndexFile.writeWithRolling(deleteMap1);
        assertThat(indexFiles1.size()).isEqualTo(1);

        // write v2 dv
        HashMap<String, DeletionVector> deleteMap2 = new HashMap<>();
        for (int i = 50000; i < 100000; i++) {
            DeletionVector index = createEmptyDV(true);
            int num = random.nextInt(1000000);
            index.delete(num);
            deleteMap2.put(String.format("file%s.parquet", i), index);
            deleteInteger.put(String.format("file%s.parquet", i), num);
        }
        List<IndexFileMeta> indexFiles2 = v2DeletionVectorsIndexFile.writeWithRolling(deleteMap2);
        assertThat(indexFiles2.size()).isEqualTo(1);

        List<IndexFileMeta> totalIndexFiles =
                Stream.concat(indexFiles1.stream(), indexFiles2.stream())
                        .collect(Collectors.toList());
        // read when writeVersionID is V1
        Map<String, DeletionVector> dvs1 =
                v1DeletionVectorsIndexFile.readAllDeletionVectors(totalIndexFiles);
        assertThat(dvs1.size()).isEqualTo(100000);
        for (String file : dvs1.keySet()) {
            int delete = deleteInteger.get(file);
            assertThat(dvs1.get(file).isDeleted(delete)).isTrue();
            assertThat(dvs1.get(file).isDeleted(delete + 1)).isFalse();
        }

        // read when writeVersionID is V2
        Map<String, DeletionVector> dvs2 =
                v2DeletionVectorsIndexFile.readAllDeletionVectors(totalIndexFiles);
        assertThat(dvs2.size()).isEqualTo(100000);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReadDeletionFile(boolean bitmap64) throws IOException {
        IndexPathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                deletionVectorsIndexFile(pathFactory, bitmap64);

        HashMap<String, DeletionVector> deleteMap = new HashMap<>();
        DeletionVector index1 = createEmptyDV(bitmap64);
        index1.delete(1);
        index1.delete(10);
        index1.delete(100);
        deleteMap.put("file1.parquet", index1);

        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.writeWithRolling(deleteMap);
        assertThat(indexFiles.size()).isEqualTo(1);

        IndexFileMeta indexFileMeta = indexFiles.get(0);
        DeletionVectorMeta deletionVectorMeta = indexFileMeta.dvRanges().get("file1.parquet");

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
