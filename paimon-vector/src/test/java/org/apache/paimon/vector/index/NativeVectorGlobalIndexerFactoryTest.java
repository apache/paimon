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

package org.apache.paimon.vector.index;

import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for vector global indexer factory SPI registration. */
public class NativeVectorGlobalIndexerFactoryTest {

    @Test
    public void testIdentifier() {
        assertThat(new IvfFlatVectorGlobalIndexerFactory().identifier()).isEqualTo("ivf-flat");
        assertThat(new IvfPqAlgorithmVectorGlobalIndexerFactory().identifier()).isEqualTo("ivf-pq");
        assertThat(new IvfHnswFlatVectorGlobalIndexerFactory().identifier())
                .isEqualTo("ivf-hnsw-flat");
        assertThat(new IvfHnswSqVectorGlobalIndexerFactory().identifier()).isEqualTo("ivf-hnsw-sq");
    }

    @Test
    public void testLoadByIdentifier() {
        assertThat(GlobalIndexerFactoryUtils.load("ivf-flat"))
                .isExactlyInstanceOf(IvfFlatVectorGlobalIndexerFactory.class);
        assertThat(GlobalIndexerFactoryUtils.load("ivf-pq"))
                .isExactlyInstanceOf(IvfPqAlgorithmVectorGlobalIndexerFactory.class);
        assertThat(GlobalIndexerFactoryUtils.load("ivf-hnsw-flat"))
                .isExactlyInstanceOf(IvfHnswFlatVectorGlobalIndexerFactory.class);
        assertThat(GlobalIndexerFactoryUtils.load("ivf-hnsw-sq"))
                .isExactlyInstanceOf(IvfHnswSqVectorGlobalIndexerFactory.class);
    }

    @Test
    public void testNativeOptionsOnlyUsesIdentifierPrefix() {
        Options options = new Options();
        options.setString("bucket", "4");
        options.setString("vector.file.format", "vortex");
        options.setString("vector.nlist", "64");
        options.setString("ivf-flat.dimension", "32");
        options.setString("ivf-flat.distance.metric", "cosine");
        options.setString("ivf-flat.nlist", "128");
        options.setString("ivf-pq.nlist", "256");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");

        assertThat(nativeOptions)
                .containsEntry("index.type", "ivf_flat")
                .containsEntry("dimension", "32")
                .containsEntry("metric", "cosine")
                .containsEntry("nlist", "128")
                .doesNotContainEntry("nlist", "64")
                .doesNotContainEntry("nlist", "256")
                .doesNotContainKey("bucket")
                .doesNotContainKey("vector.file.format");
    }

    @Test
    public void testNativeOptionsUsesVectorTypeDimension() {
        Options options = new Options();
        options.setString("ivf-flat.dimension", "32");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new VectorType(8, new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");

        assertThat(nativeOptions).containsEntry("dimension", "8");
    }

    @Test
    public void testInvalidDimension() {
        Options options = new Options();
        options.setString("ivf-flat.dimension", "0");

        assertThatThrownBy(
                        () ->
                                NativeVectorGlobalIndexerFactory.nativeOptions(
                                        new ArrayType(new FloatType()),
                                        options,
                                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                        "vec"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ivf-flat.dimension")
                .hasMessageContaining("positive integer");
    }

    @Test
    public void testFieldLevelOptionsOverrideIndexTypeOptions() {
        Options options = new Options();
        options.setString("ivf-flat.dimension", "32");
        options.setString("ivf-flat.nlist", "128");
        options.setString("fields.vec.nlist", "256");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");

        assertThat(nativeOptions)
                .containsEntry("dimension", "32")
                .containsEntry("nlist", "256")
                .doesNotContainEntry("nlist", "128");
    }

    @Test
    public void testFieldLevelDimensionOverridesIndexTypeDimension() {
        Options options = new Options();
        options.setString("ivf-flat.dimension", "32");
        options.setString("fields.vec.dimension", "64");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");

        assertThat(nativeOptions).containsEntry("dimension", "64");
    }

    @Test
    public void testFieldLevelOptionsOnlyApplyToMatchingField() {
        Options options = new Options();
        options.setString("ivf-flat.nlist", "128");
        options.setString("fields.vec.nlist", "256");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "other");

        assertThat(nativeOptions).containsEntry("nlist", "128");
    }

    @Test
    public void testFieldLevelOptionsRequireExactFieldName() {
        Options options = new Options();
        options.setString("ivf-flat.nlist", "128");
        options.setString("fields.vec_extra.nlist", "512");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");

        assertThat(nativeOptions).containsEntry("nlist", "128");
    }

    @Test
    public void testFieldLevelOptionsWithoutIndexTypeOption() {
        Options options = new Options();
        options.setString("fields.vec.distance.metric", "cosine");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");

        assertThat(nativeOptions).containsEntry("metric", "cosine");
    }

    @Test
    public void testFieldLevelVectorOptionsCoexistWithCoreFieldOptions() {
        Options options = new Options();
        options.setString("ivf-flat.nlist", "128");
        options.setString("fields.vec.nlist", "256");
        options.setString("fields.vec.aggregate-function", "sum");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");

        assertThat(nativeOptions)
                .containsEntry("nlist", "256")
                .doesNotContainKey("aggregate-function");
    }

    @Test
    public void testTrainSampleRatioDefaultAndOverrides() {
        Options options = new Options();

        assertThat(
                        NativeVectorGlobalIndexerFactory.trainSampleRatio(
                                options, IvfFlatVectorGlobalIndexerFactory.IDENTIFIER, "vec"))
                .isEqualTo(NativeVectorGlobalIndexerFactory.DEFAULT_TRAIN_SAMPLE_RATIO);

        options.setString("ivf-flat.train.sample-ratio", "0.25");
        assertThat(
                        NativeVectorGlobalIndexerFactory.trainSampleRatio(
                                options, IvfFlatVectorGlobalIndexerFactory.IDENTIFIER, "vec"))
                .isEqualTo(0.25);

        options.setString("fields.vec.train.sample-ratio", "0.5");
        assertThat(
                        NativeVectorGlobalIndexerFactory.trainSampleRatio(
                                options, IvfFlatVectorGlobalIndexerFactory.IDENTIFIER, "vec"))
                .isEqualTo(0.5);

        assertThat(
                        NativeVectorGlobalIndexerFactory.trainSampleRatio(
                                options, IvfFlatVectorGlobalIndexerFactory.IDENTIFIER, "other"))
                .isEqualTo(0.25);
    }

    @Test
    public void testInvalidTrainSampleRatio() {
        Options options = new Options();
        options.setString("ivf-flat.train.sample-ratio", "0");

        assertThatThrownBy(
                        () ->
                                NativeVectorGlobalIndexerFactory.trainSampleRatio(
                                        options,
                                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                        "vec"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ivf-flat.train.sample-ratio")
                .hasMessageContaining("greater than 0");

        options.setString("fields.vec.train.sample-ratio", "bad");
        assertThatThrownBy(
                        () ->
                                NativeVectorGlobalIndexerFactory.trainSampleRatio(
                                        options,
                                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                        "vec"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fields.vec.train.sample-ratio")
                .hasMessageContaining("less than or equal to 1");
    }

    @Test
    public void testTrainSampleRatioIsNotNativeOption() {
        Options options = new Options();
        options.setString("ivf-flat.dimension", "32");
        options.setString("ivf-flat.nlist", "128");
        options.setString("ivf-flat.train.sample-ratio", "0.25");
        options.setString("fields.vec.train.sample-ratio", "0.5");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");

        assertThat(nativeOptions)
                .containsEntry("dimension", "32")
                .containsEntry("nlist", "128")
                .doesNotContainKey("train.sample-ratio");
    }
}
