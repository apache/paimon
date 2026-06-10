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

import org.apache.paimon.index.ivfpq.IndexType;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VectorIndexOptions}. */
public class VectorIndexOptionsTest {

    @Test
    public void testDefaults() {
        Options options = new Options();
        VectorIndexOptions indexOptions = new VectorIndexOptions(options);
        assertThat(indexOptions.dimension()).isEqualTo(128);
        assertThat(indexOptions.indexType()).isEqualTo(IndexType.IVF_PQ);
        assertThat(indexOptions.metric()).isEqualTo(VectorMetric.INNER_PRODUCT);
        assertThat(indexOptions.nlist()).isEqualTo(256);
        assertThat(indexOptions.m()).isEqualTo(16);
        assertThat(indexOptions.useOpq()).isFalse();
        assertThat(indexOptions.hnswConfig().m()).isEqualTo(20);
        assertThat(indexOptions.hnswConfig().efConstruction()).isEqualTo(150);
        assertThat(indexOptions.hnswConfig().maxLevel()).isEqualTo(7);
        assertThat(indexOptions.nprobe()).isEqualTo(16);
        assertThat(indexOptions.efSearch()).isEqualTo(0);
        assertThat(indexOptions.trainSampleRatio()).isEqualTo(1.0);
        assertThat(indexOptions.addBatchSize()).isEqualTo(10000);
    }

    @Test
    public void testCustomOptions() {
        Options options = new Options();
        options.setString("vector.index.type", "ivf-hnsw-sq");
        options.setInteger("vector.index.dimension", 64);
        options.setString("vector.distance.metric", "l2");
        options.setInteger("vector.nlist", 128);
        options.setInteger("vector.pq.m", 8);
        options.setString("vector.pq.use-opq", "true");
        options.setInteger("vector.hnsw.m", 12);
        options.setInteger("vector.hnsw.ef-construction", 64);
        options.setInteger("vector.hnsw.max-level", 5);
        options.setInteger("vector.nprobe", 32);
        options.setInteger("vector.hnsw.ef-search", 96);
        options.setString("vector.train.sample-ratio", "0.5");
        options.setInteger("vector.add.batch-size", 5000);

        VectorIndexOptions indexOptions = new VectorIndexOptions(options);
        assertThat(indexOptions.dimension()).isEqualTo(64);
        assertThat(indexOptions.indexType()).isEqualTo(IndexType.IVF_HNSW_SQ);
        assertThat(indexOptions.metric()).isEqualTo(VectorMetric.L2);
        assertThat(indexOptions.nlist()).isEqualTo(128);
        assertThat(indexOptions.m()).isEqualTo(8);
        assertThat(indexOptions.useOpq()).isTrue();
        assertThat(indexOptions.hnswConfig().m()).isEqualTo(12);
        assertThat(indexOptions.hnswConfig().efConstruction()).isEqualTo(64);
        assertThat(indexOptions.hnswConfig().maxLevel()).isEqualTo(5);
        assertThat(indexOptions.nprobe()).isEqualTo(32);
        assertThat(indexOptions.efSearch()).isEqualTo(96);
        assertThat(indexOptions.trainSampleRatio()).isEqualTo(0.5);
        assertThat(indexOptions.addBatchSize()).isEqualTo(5000);
    }

    @Test
    public void testIdentifierSelectsIndexType() {
        assertThat(new VectorIndexOptions(new Options(), "ivf-flat").indexType())
                .isEqualTo(IndexType.IVF_FLAT);
        assertThat(new VectorIndexOptions(new Options(), "ivf-pq").indexType())
                .isEqualTo(IndexType.IVF_PQ);
        assertThat(new VectorIndexOptions(new Options(), "ivf-hnsw-flat").indexType())
                .isEqualTo(IndexType.IVF_HNSW_FLAT);
        assertThat(new VectorIndexOptions(new Options(), "ivf-hnsw-sq").indexType())
                .isEqualTo(IndexType.IVF_HNSW_SQ);
    }

    @Test
    public void testIdentifierRejectsConflictingIndexType() {
        Options options = new Options();
        options.setString("vector.index.type", "ivf-pq");

        assertThatThrownBy(() -> new VectorIndexOptions(options, "ivf-flat"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Conflicting vector index type");
    }

    @Test
    public void testMDivisibilityValidation() {
        Options options = new Options();
        options.setInteger("vector.index.dimension", 10);
        options.setInteger("vector.pq.m", 3);
        assertThatThrownBy(() -> new VectorIndexOptions(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must divide");
    }

    @Test
    public void testInvalidSampleRatio() {
        Options options = new Options();
        options.setString("vector.train.sample-ratio", "0.0");
        assertThatThrownBy(() -> new VectorIndexOptions(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("vector.train.sample-ratio");

        Options options2 = new Options();
        options2.setString("vector.train.sample-ratio", "1.5");
        assertThatThrownBy(() -> new VectorIndexOptions(options2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("vector.train.sample-ratio");
    }

    @Test
    public void testMetricParsing() {
        for (String metric : new String[] {"l2", "cosine", "inner_product"}) {
            Options options = new Options();
            options.setString("vector.distance.metric", metric);
            VectorIndexOptions indexOptions = new VectorIndexOptions(options);
            assertThat(indexOptions.metric().getConfigName()).isEqualTo(metric);
        }
    }

    @Test
    public void testMetricParsingUpperCase() {
        Options options = new Options();
        options.setString("vector.distance.metric", "L2");
        VectorIndexOptions indexOptions = new VectorIndexOptions(options);
        assertThat(indexOptions.metric()).isEqualTo(VectorMetric.L2);
    }
}
