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

package org.apache.paimon.lumina.index;

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link LuminaVectorGlobalIndexWriter}. */
public class LuminaVectorGlobalIndexWriterTest {

    @Test
    public void testVectorTypeUsesTypeDimensionByDefault() {
        DataType vecFieldType = new VectorType(2, new FloatType());
        Options options = new Options();
        options.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), "l2");

        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);

        try (LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(
                        createNoopFileWriter(), vecFieldType, indexOptions)) {
            assertThat(writerLuminaOptions(writer))
                    .containsEntry(
                            LuminaVectorIndexOptions.toLuminaKey(
                                    LuminaVectorIndexOptions.DIMENSION),
                            "2");

            writer.write(new float[] {1.0f, 0.0f});

            assertThatThrownBy(() -> writer.write(new float[] {1.0f, 0.0f, 0.0f}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("expected 2")
                    .hasMessageContaining("got 3");
        }
    }

    @Test
    public void testVectorTypePqMUsesTypeDimensionForCap() {
        assertVectorTypePqMOption(256, 192, 192);
        assertVectorTypePqMOption(256, 300, 256);
    }

    @Test
    public void testVectorTypeRejectsExplicitDimensionConflict() {
        DataType vecFieldType = new VectorType(2, new FloatType());
        Options options = createDefaultOptions(3);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);

        assertThatThrownBy(
                        () ->
                                new LuminaVectorGlobalIndexWriter(
                                        createNoopFileWriter(), vecFieldType, indexOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(LuminaVectorIndexOptions.DIMENSION.key())
                .hasMessageContaining("configured 3")
                .hasMessageContaining("VECTOR length 2");
    }

    @Test
    public void testArrayTypeUsesOptionDimension() {
        DataType arrayFieldType = new ArrayType(new FloatType());
        Options options = createDefaultOptions(3);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);

        try (LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(
                        createNoopFileWriter(), arrayFieldType, indexOptions)) {
            writer.write(new float[] {1.0f, 0.0f, 0.0f});

            assertThatThrownBy(() -> writer.write(new float[] {1.0f, 0.0f}))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("expected 3")
                    .hasMessageContaining("got 2");
        }
    }

    private static Options createDefaultOptions(int dimension) {
        Options options = new Options();
        options.setInteger(LuminaVectorIndexOptions.DIMENSION.key(), dimension);
        options.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
        return options;
    }

    private static GlobalIndexFileWriter createNoopFileWriter() {
        return new GlobalIndexFileWriter() {
            @Override
            public String newFileName(String prefix) {
                return prefix;
            }

            @Override
            public PositionOutputStream newOutputStream(String fileName) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static void assertVectorTypePqMOption(
            int vectorDimension, int configuredPqM, int expectedPqM) {
        DataType vecFieldType = new VectorType(vectorDimension, new FloatType());
        Options options = new Options();
        options.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
        options.setInteger(LuminaVectorIndexOptions.ENCODING_PQ_M.key(), configuredPqM);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);

        try (LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(
                        createNoopFileWriter(), vecFieldType, indexOptions)) {
            Map<String, String> luminaOptions = writerLuminaOptions(writer);
            assertThat(luminaOptions)
                    .containsEntry(
                            LuminaVectorIndexOptions.toLuminaKey(
                                    LuminaVectorIndexOptions.DIMENSION),
                            String.valueOf(vectorDimension))
                    .containsEntry(
                            LuminaVectorIndexOptions.toLuminaKey(
                                    LuminaVectorIndexOptions.ENCODING_PQ_M),
                            String.valueOf(expectedPqM));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> writerLuminaOptions(LuminaVectorGlobalIndexWriter writer) {
        try {
            Field field = LuminaVectorGlobalIndexWriter.class.getDeclaredField("luminaOptions");
            field.setAccessible(true);
            return (Map<String, String>) field.get(writer);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
