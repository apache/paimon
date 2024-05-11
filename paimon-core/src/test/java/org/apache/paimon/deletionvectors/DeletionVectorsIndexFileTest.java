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
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
                new DeletionVectorsIndexFile(LocalFileIO.create(), pathFactory);

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

        Pair<String, LinkedHashMap<String, Pair<Integer, Integer>>> pair =
                deletionVectorsIndexFile.write(deleteMap);
        String fileName = pair.getLeft();
        LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorRanges = pair.getRight();

        // read
        Map<String, DeletionVector> actualDeleteMap =
                deletionVectorsIndexFile.readAllDeletionVectors(fileName, deletionVectorRanges);
        assertThat(actualDeleteMap.get("file1.parquet").isDeleted(1)).isTrue();
        assertThat(actualDeleteMap.get("file1.parquet").isDeleted(2)).isFalse();
        assertThat(actualDeleteMap.get("file2.parquet").isDeleted(2)).isTrue();
        assertThat(actualDeleteMap.get("file2.parquet").isDeleted(3)).isTrue();
        assertThat(actualDeleteMap.get("file33.parquet").isDeleted(3)).isTrue();

        DeletionVector file1DeletionVector =
                deletionVectorsIndexFile.readDeletionVector(
                        fileName, deletionVectorRanges.get("file1.parquet"));
        assertThat(file1DeletionVector.isDeleted(1)).isTrue();
        assertThat(file1DeletionVector.isDeleted(2)).isFalse();

        // delete
        deletionVectorsIndexFile.delete(fileName);
        assertThat(deletionVectorsIndexFile.exists(fileName)).isFalse();
    }

    @Test
    public void testReadDvIndexWithCopiousDv() {
        PathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                new DeletionVectorsIndexFile(LocalFileIO.create(), pathFactory);

        // write
        Random random = new Random();
        HashMap<String, DeletionVector> deleteMap = new HashMap<>();
        for (int i = 0; i < 100000; i++) {
            BitmapDeletionVector index = new BitmapDeletionVector();
            index.delete(random.nextInt(1000000));
            deleteMap.put(String.format("file%s.parquet", i), index);
        }

        Pair<String, LinkedHashMap<String, Pair<Integer, Integer>>> pair =
                deletionVectorsIndexFile.write(deleteMap);

        // read
        Map<String, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(pair.getLeft(), pair.getRight());
        assertThat(dvs.size()).isEqualTo(100000);
    }

    @Test
    public void testReadDvIndexWithEnormousDv() {
        PathFactory pathFactory = getPathFactory();
        DeletionVectorsIndexFile deletionVectorsIndexFile =
                new DeletionVectorsIndexFile(LocalFileIO.create(), pathFactory);

        // write
        Random random = new Random();
        HashMap<String, DeletionVector> deleteMap = new HashMap<>();
        BitmapDeletionVector index = new BitmapDeletionVector();
        // dv index's size is about 20M
        for (int i = 0; i < 10000000; i++) {
            index.delete(random.nextInt(Integer.MAX_VALUE));
        }
        deleteMap.put("largeFile.parquet", index);
        Pair<String, LinkedHashMap<String, Pair<Integer, Integer>>> pair =
                deletionVectorsIndexFile.write(deleteMap);

        // read
        Map<String, DeletionVector> dvs =
                deletionVectorsIndexFile.readAllDeletionVectors(pair.getLeft(), pair.getRight());
        assertThat(dvs.size()).isEqualTo(1);
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
