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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for vector global indexer factory SPI registration. */
public class NativeVectorGlobalIndexerFactoryTest {

    @Test
    public void testIdentifier() {
        assertThat(new IvfFlatVectorGlobalIndexerFactory().identifier()).isEqualTo("ivf-flat");
        assertThat(new IvfPqAlgorithmVectorGlobalIndexerFactory().identifier()).isEqualTo("ivf-pq");
        assertThat(new IvfSqVectorGlobalIndexerFactory().identifier()).isEqualTo("ivf-sq");
        assertThat(new IvfRqVectorGlobalIndexerFactory().identifier()).isEqualTo("ivf-rq");
        assertThat(new DiskAnnVectorGlobalIndexerFactory().identifier()).isEqualTo("diskann");
    }

    @Test
    public void testLoadByIdentifier() {
        assertThat(GlobalIndexerFactoryUtils.load("ivf-flat"))
                .isExactlyInstanceOf(IvfFlatVectorGlobalIndexerFactory.class);
        assertThat(GlobalIndexerFactoryUtils.load("ivf-pq"))
                .isExactlyInstanceOf(IvfPqAlgorithmVectorGlobalIndexerFactory.class);
        assertThat(GlobalIndexerFactoryUtils.load("ivf-sq"))
                .isExactlyInstanceOf(IvfSqVectorGlobalIndexerFactory.class);
        assertThat(GlobalIndexerFactoryUtils.load("ivf-rq"))
                .isExactlyInstanceOf(IvfRqVectorGlobalIndexerFactory.class);
        assertThat(GlobalIndexerFactoryUtils.load("diskann"))
                .isExactlyInstanceOf(DiskAnnVectorGlobalIndexerFactory.class);
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
    public void testNativeOptionsUseDefaultMetric() {
        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        new Options(),
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");

        assertThat(nativeOptions).containsEntry("metric", "inner_product");
    }

    @Test
    public void testNewVectorIndexOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("ivf-rq.rq.bits", "5");
        tableOptions.put("ivf-rq.max-bytes-per-vector", "96");
        tableOptions.put("diskann.build-preset", "balanced");
        tableOptions.put("diskann.pq.code-ratio", "0.0625");
        tableOptions.put("diskann.raw-vector-encoding", "f16");
        Options options = new Options(tableOptions);

        Map<String, String> rqOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfRqVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");
        assertThat(rqOptions)
                .containsEntry("index.type", "ivf_rq")
                .containsEntry("rq.bits", "5")
                .containsEntry("max-bytes-per-vector", "96");

        Map<String, String> diskAnnOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        DiskAnnVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");
        assertThat(diskAnnOptions)
                .containsEntry("index.type", "diskann")
                .containsEntry("diskann.build-preset", "balanced")
                .containsEntry("pq.code-ratio", "0.0625")
                .containsEntry("diskann.raw-vector-encoding", "f16");
    }

    @Test
    public void test050BuildOptions() {
        Options nativeNames = new Options();
        nativeNames.setString("ivf.coarse-assignment", "auto");
        nativeNames.setString("ivf.pq-encoding", "auto");
        nativeNames.setString("ivf.train.max-points-per-centroid", "32");
        nativeNames.setString("pq.train.max-points-per-centroid", "64");

        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        nativeNames,
                        IvfPqAlgorithmVectorGlobalIndexerFactory.IDENTIFIER,
                        "vec");
        assertThat(nativeOptions)
                .containsEntry("ivf.coarse-assignment", "auto")
                .containsEntry("ivf.pq-encoding", "auto")
                .containsEntry("ivf.train.max-points-per-centroid", "32")
                .containsEntry("pq.train.max-points-per-centroid", "64");

        Options prefixedNames = new Options();
        prefixedNames.setString("ivf-pq.ivf.coarse-assignment", "exact");
        prefixedNames.setString("fields.vec.ivf.pq-encoding", "canonical");
        prefixedNames.setString("ivf-pq.ivf.train.max-points-per-centroid", "16");
        prefixedNames.setString("fields.vec.pq.train.max-points-per-centroid", "48");
        assertThat(
                        NativeVectorGlobalIndexerFactory.nativeOptions(
                                new ArrayType(new FloatType()),
                                prefixedNames,
                                IvfPqAlgorithmVectorGlobalIndexerFactory.IDENTIFIER,
                                "vec"))
                .containsEntry("ivf.coarse-assignment", "exact")
                .containsEntry("ivf.pq-encoding", "canonical")
                .containsEntry("ivf.train.max-points-per-centroid", "16")
                .containsEntry("pq.train.max-points-per-centroid", "48");

        Options diskAnn = new Options();
        diskAnn.setString("diskann.pq.train.max-points-per-centroid", "24");
        assertThat(
                        NativeVectorGlobalIndexerFactory.nativeOptions(
                                new ArrayType(new FloatType()),
                                diskAnn,
                                DiskAnnVectorGlobalIndexerFactory.IDENTIFIER,
                                "vec"))
                .containsEntry("pq.train.max-points-per-centroid", "24");

        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("fields.vec.ivf.pq-encoding", "auto");
        Map<String, String> userOptions = new HashMap<>();
        userOptions.put("ivf.pq-encoding", "canonical");
        assertThat(
                        NativeVectorGlobalIndexerFactory.nativeOptions(
                                new ArrayType(new FloatType()),
                                new Options(
                                        new HashMap<>(), new Options(tableOptions, userOptions)),
                                IvfPqAlgorithmVectorGlobalIndexerFactory.IDENTIFIER,
                                "vec"))
                .containsEntry("ivf.pq-encoding", "canonical");
        assertThat(
                        NativeVectorGlobalIndexerFactory.nativeOptions(
                                new ArrayType(new FloatType()),
                                new Options(tableOptions),
                                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                "vec"))
                .doesNotContainKey("ivf.pq-encoding");
    }

    @Test
    public void testRejectsInapplicable050BuildOptions() {
        Options mutableOptions = new Options();
        mutableOptions.setString("ivf-flat.ivf.pq-encoding", "canonical");
        assertThatThrownBy(
                        () ->
                                NativeVectorGlobalIndexerFactory.nativeOptions(
                                        new ArrayType(new FloatType()),
                                        mutableOptions,
                                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                        "vec"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ivf-flat.ivf.pq-encoding");

        Map<String, String> flatUserOptions = new HashMap<>();
        flatUserOptions.put("ivf-flat.ivf.pq-encoding", "canonical");
        Options flatOptions = new Options(new HashMap<>(), flatUserOptions);
        assertThatThrownBy(
                        () ->
                                NativeVectorGlobalIndexerFactory.nativeOptions(
                                        new ArrayType(new FloatType()),
                                        flatOptions,
                                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                        "vec"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ivf-flat.ivf.pq-encoding");

        Map<String, String> diskAnnUserOptions = new HashMap<>();
        diskAnnUserOptions.put("diskann.ivf.coarse-assignment", "exact");
        Options diskAnnOptions = new Options(new HashMap<>(), diskAnnUserOptions);
        assertThatThrownBy(
                        () ->
                                NativeVectorGlobalIndexerFactory.nativeOptions(
                                        new ArrayType(new FloatType()),
                                        diskAnnOptions,
                                        DiskAnnVectorGlobalIndexerFactory.IDENTIFIER,
                                        "vec"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("diskann.ivf.coarse-assignment");
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

        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("fields.vec.train.sample-ratio", "0.75");
        Map<String, String> userOptions = new HashMap<>();
        userOptions.put("ivf-flat.train.sample-ratio", "0.5");
        assertThat(
                        NativeVectorGlobalIndexerFactory.trainSampleRatio(
                                new Options(tableOptions, userOptions),
                                IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                "vec"))
                .isEqualTo(0.5);
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
