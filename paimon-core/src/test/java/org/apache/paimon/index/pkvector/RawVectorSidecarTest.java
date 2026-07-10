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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for raw vector sidecars. */
class RawVectorSidecarTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testRoundTripByRowPosition() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempPath.resolve("raw-vector").toUri());
        try (RawVectorSidecarWriter writer = new RawVectorSidecarWriter(fileIO, path, 3)) {
            writer.write(new float[] {1, 2, 3});
            writer.write(null);
            writer.write(new GenericArray(new float[] {4, 5, 6}));
        }

        try (RawVectorSidecarReader reader = new RawVectorSidecarReader(fileIO, path)) {
            assertThat(reader.dimension()).isEqualTo(3);
            assertThat(reader.rowCount()).isEqualTo(3);
            assertThat(reader.rowPositionForOrdinal(2)).isEqualTo(2);
            assertThat(reader.readVector(2)).containsExactly(4, 5, 6);
            assertThat(reader.readVector(1)).isNull();
            assertThat(reader.readVector(0)).containsExactly(1, 2, 3);
        }
    }

    @Test
    void testRejectNonFiniteVectorWithoutCorruptingFile() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempPath.resolve("finite-vector").toUri());
        try (RawVectorSidecarWriter writer = new RawVectorSidecarWriter(fileIO, path, 2)) {
            assertThatThrownBy(() -> writer.write(new float[] {Float.NaN, 1}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must be finite");
            assertThat(writer.rowCount()).isZero();
            writer.write(new float[] {2, 3});
        }

        try (RawVectorSidecarReader reader = new RawVectorSidecarReader(fileIO, path)) {
            assertThat(reader.rowCount()).isEqualTo(1);
            assertThat(reader.readVector(0)).containsExactly(2, 3);
        }
    }

    @Test
    void testSequentialReadIntoReusableBuffer() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempPath.resolve("sequential-vector").toUri());
        try (RawVectorSidecarWriter writer = new RawVectorSidecarWriter(fileIO, path, 2)) {
            writer.write(new float[] {1, 2});
            writer.write(null);
            writer.write(new float[] {3, 4});
        }

        try (RawVectorSidecarReader reader = new RawVectorSidecarReader(fileIO, path)) {
            float[] buffer = new float[2];
            reader.rewind();
            assertThat(reader.readNextVector(buffer)).isTrue();
            assertThat(buffer).containsExactly(1, 2);
            assertThat(reader.readNextVector(buffer)).isFalse();
            assertThat(reader.readNextVector(buffer)).isTrue();
            assertThat(buffer).containsExactly(3, 4);
            assertThatThrownBy(() -> reader.readNextVector(buffer))
                    .hasMessageContaining("No raw vector remains");
        }
    }

    @Test
    void testExactSearchSkipsNullAndDeletedPositions() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempPath.resolve("search-vector").toUri());
        try (RawVectorSidecarWriter writer = new RawVectorSidecarWriter(fileIO, path, 2)) {
            writer.write(new float[] {0, 0});
            writer.write(null);
            writer.write(new float[] {1, 0});
            writer.write(new float[] {0, 2});
            writer.write(new float[] {3, 3});
        }

        try (RawVectorSidecarReader reader = new RawVectorSidecarReader(fileIO, path)) {
            List<RawVectorSidecarReader.Candidate> candidates =
                    reader.search(new float[] {0, 0}, "l2", 2, position -> position == 0);

            assertThat(candidates)
                    .extracting(RawVectorSidecarReader.Candidate::rowPosition)
                    .containsExactly(2L, 3L);
            assertThat(candidates)
                    .extracting(RawVectorSidecarReader.Candidate::distance)
                    .containsExactly(1.0f, 4.0f);
        }
    }

    @Test
    void testCosineSearch() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempPath.resolve("cosine-vector").toUri());
        try (RawVectorSidecarWriter writer = new RawVectorSidecarWriter(fileIO, path, 2)) {
            writer.write(new float[] {1, 0});
            writer.write(new float[] {0, 1});
            writer.write(new float[] {2, 0});
        }

        try (RawVectorSidecarReader reader = new RawVectorSidecarReader(fileIO, path)) {
            List<RawVectorSidecarReader.Candidate> candidates =
                    reader.search(new float[] {1, 0}, "cosine", 3, position -> false);

            assertThat(candidates)
                    .extracting(RawVectorSidecarReader.Candidate::rowPosition)
                    .containsExactly(0L, 2L, 1L);
            assertThat(candidates)
                    .extracting(RawVectorSidecarReader.Candidate::distance)
                    .containsExactly(0.0f, 0.0f, 1.0f);
        }
    }

    @Test
    void testInnerProductSearch() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempPath.resolve("inner-product-vector").toUri());
        try (RawVectorSidecarWriter writer = new RawVectorSidecarWriter(fileIO, path, 2)) {
            writer.write(new float[] {1, 0});
            writer.write(new float[] {0, 1});
            writer.write(new float[] {2, 0});
        }

        try (RawVectorSidecarReader reader = new RawVectorSidecarReader(fileIO, path)) {
            List<RawVectorSidecarReader.Candidate> candidates =
                    reader.search(new float[] {1, 0}, "inner_product", 3, position -> false);

            assertThat(candidates)
                    .extracting(RawVectorSidecarReader.Candidate::rowPosition)
                    .containsExactly(2L, 0L, 1L);
        }
    }
}
