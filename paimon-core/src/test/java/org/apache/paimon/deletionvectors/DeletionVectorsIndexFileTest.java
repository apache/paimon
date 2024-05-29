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
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.PathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeletionVectorsIndexFile}. */
public class DeletionVectorsIndexFileTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void testReadDvIndex() {
        PathFactory pathFactory = getPathFactory();

        DeletionVectorsIndexFile deletionVectorsIndexFile =
                new DeletionVectorsIndexFile(
                        LocalFileIO.create(), pathFactory, MemorySize.ofMebiBytes(Long.MAX_VALUE));

        // write
        HashMap<String, DeletionVector> deleteMap = new HashMap<>();
        BitmapDeletionVector index1 = new BitmapDeletionVector();
        index1.delete(1);
        deleteMap.put("file1.parquet", index1);

        BitmapDeletionVector index2 = new BitmapDeletionVector();
        index2.delete(2);
        index2.delete(3);
        deleteMap.put("file2.parquet", index2);

        BitmapDeletionVector index3 = new BitmapDeletionVector();
        index3.delete(3);
        deleteMap.put("file33.parquet", index3);

        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.write(deleteMap);
        assertThat(indexFiles.size()).isEqualTo(1);

        // read
        String fileName = indexFiles.get(0).fileName();
        Map<String, DeletionVector> actualDeleteMap =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(actualDeleteMap.get("file1.parquet").isDeleted(1)).isTrue();
        assertThat(actualDeleteMap.get("file1.parquet").isDeleted(2)).isFalse();
        assertThat(actualDeleteMap.get("file2.parquet").isDeleted(2)).isTrue();
        assertThat(actualDeleteMap.get("file2.parquet").isDeleted(3)).isTrue();
        assertThat(actualDeleteMap.get("file33.parquet").isDeleted(3)).isTrue();

        // delete
        deletionVectorsIndexFile.delete(fileName);
        assertThat(deletionVectorsIndexFile.exists(fileName)).isFalse();
    }

    @Test
    public void testReadDvIndexWithCopiousDv() {
        PathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                new DeletionVectorsIndexFile(
                        LocalFileIO.create(), pathFactory, MemorySize.ofMebiBytes(Long.MAX_VALUE));

        // write
        Random random = new Random();
        HashMap<String, DeletionVector> deleteMap = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            BitmapDeletionVector index = new BitmapDeletionVector();
            index.delete(random.nextInt(1000000));
            deleteMap.put(String.format("file%s.parquet", i), index);
        }

        // read
        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.write(deleteMap);
        assertThat(indexFiles.size()).isEqualTo(1);
        Map<String, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(dvs.size()).isEqualTo(100000);
    }

    @Test
    public void testReadDvIndexWithEnormousDv() {
        PathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                new DeletionVectorsIndexFile(
                        LocalFileIO.create(), pathFactory, MemorySize.ofMebiBytes(Long.MAX_VALUE));

        // write
        Random random = new Random();
        Map<String, DeletionVector> fileToDV = new HashMap<>();
        Map<String, Long> fileToCardinality = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            BitmapDeletionVector index = new BitmapDeletionVector();
            // the size of dv index file is about 20M
            for (int j = 0; j < 10000000; j++) {
                index.delete(random.nextInt(Integer.MAX_VALUE));
            }
            fileToCardinality.put("f" + i, index.getCardinality());
            fileToDV.put("f" + i, index);
        }
        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.write(fileToDV);

        // read
        assertThat(indexFiles.size()).isEqualTo(1);
        Map<String, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        assertThat(dvs.size()).isEqualTo(5);
        for (String file : dvs.keySet()) {
            assertThat(dvs.get(file).getCardinality()).isEqualTo(fileToCardinality.get(file));
        }
    }

    @Test
    public void testWriteDVIndexWithLimitedTargetSizePerIndexFile() {
        PathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                new DeletionVectorsIndexFile(
                        LocalFileIO.create(), pathFactory, MemorySize.parse("2MB"));

        // write1
        Random random = new Random();
        Map<String, DeletionVector> fileToDV = new HashMap<>();
        Map<String, Long> fileToCardinality = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            BitmapDeletionVector index = new BitmapDeletionVector();
            // the size of dv index file is about 1.7M
            for (int j = 0; j < 750000; j++) {
                index.delete(random.nextInt(Integer.MAX_VALUE));
            }
            fileToCardinality.put("f" + i, index.getCardinality());
            fileToDV.put("f" + i, index);
        }
        List<IndexFileMeta> indexFiles = deletionVectorsIndexFile.write(fileToDV);

        // assert 1
        assertThat(indexFiles.size()).isEqualTo(5);
        Map<String, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        for (String file : dvs.keySet()) {
            assertThat(dvs.get(file).getCardinality()).isEqualTo(fileToCardinality.get(file));
        }

        // write2
        fileToDV.clear();
        fileToCardinality.clear();
        for (int i = 0; i < 10; i++) {
            BitmapDeletionVector index = new BitmapDeletionVector();
            // the size of dv index file is about 0.42M
            for (int j = 0; j < 100000; j++) {
                index.delete(random.nextInt(Integer.MAX_VALUE));
            }
            fileToCardinality.put("f" + i, index.getCardinality());
            fileToDV.put("f" + i, index);
        }
        indexFiles = deletionVectorsIndexFile.write(fileToDV);

        // assert 2
        assertThat(indexFiles.size()).isGreaterThan(1);
        dvs = deletionVectorsIndexFile.readAllDeletionVectors(indexFiles);
        for (String file : dvs.keySet()) {
            assertThat(dvs.get(file).getCardinality()).isEqualTo(fileToCardinality.get(file));
        }
    }

    private PathFactory getPathFactory() {
        Path dir = new Path(tempPath.toUri());
        return new PathFactory() {
            @Override
            public Path newPath() {
                return new Path(dir, UUID.randomUUID().toString());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(dir, fileName);
            }
        };
    }
}
