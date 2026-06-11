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
public class VectorGlobalIndexerFactoryTest {

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
                VectorGlobalIndexerFactory.nativeOptions(
                        new ArrayType(new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER);

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
                VectorGlobalIndexerFactory.nativeOptions(
                        new VectorType(8, new FloatType()),
                        options,
                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER);

        assertThat(nativeOptions).containsEntry("dimension", "8");
    }

    @Test
    public void testInvalidDimension() {
        Options options = new Options();
        options.setString("ivf-flat.dimension", "0");

        assertThatThrownBy(
                        () ->
                                VectorGlobalIndexerFactory.nativeOptions(
                                        new ArrayType(new FloatType()),
                                        options,
                                        IvfFlatVectorGlobalIndexerFactory.IDENTIFIER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ivf-flat.dimension")
                .hasMessageContaining("positive integer");
    }
}
