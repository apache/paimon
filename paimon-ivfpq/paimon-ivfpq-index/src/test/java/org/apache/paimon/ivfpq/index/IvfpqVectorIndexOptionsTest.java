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

package org.apache.paimon.ivfpq.index;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IvfpqVectorIndexOptions}. */
public class IvfpqVectorIndexOptionsTest {

    @Test
    public void testDefaults() {
        Options options = new Options();
        IvfpqVectorIndexOptions indexOptions = new IvfpqVectorIndexOptions(options);
        assertThat(indexOptions.dimension()).isEqualTo(128);
        assertThat(indexOptions.metric()).isEqualTo(IvfpqVectorMetric.INNER_PRODUCT);
        assertThat(indexOptions.nlist()).isEqualTo(256);
        assertThat(indexOptions.m()).isEqualTo(16);
        assertThat(indexOptions.useOpq()).isFalse();
        assertThat(indexOptions.nprobe()).isEqualTo(16);
        assertThat(indexOptions.trainSampleRatio()).isEqualTo(1.0);
        assertThat(indexOptions.addBatchSize()).isEqualTo(10000);
    }

    @Test
    public void testCustomOptions() {
        Options options = new Options();
        options.setInteger("ivfpq.index.dimension", 64);
        options.setString("ivfpq.distance.metric", "l2");
        options.setInteger("ivfpq.nlist", 128);
        options.setInteger("ivfpq.m", 8);
        options.setBoolean("ivfpq.use_opq", true);
        options.setInteger("ivfpq.nprobe", 32);
        options.setDouble("ivfpq.train.sample_ratio", 0.5);
        options.setInteger("ivfpq.add.batch_size", 5000);

        IvfpqVectorIndexOptions indexOptions = new IvfpqVectorIndexOptions(options);
        assertThat(indexOptions.dimension()).isEqualTo(64);
        assertThat(indexOptions.metric()).isEqualTo(IvfpqVectorMetric.L2);
        assertThat(indexOptions.nlist()).isEqualTo(128);
        assertThat(indexOptions.m()).isEqualTo(8);
        assertThat(indexOptions.useOpq()).isTrue();
        assertThat(indexOptions.nprobe()).isEqualTo(32);
        assertThat(indexOptions.trainSampleRatio()).isEqualTo(0.5);
        assertThat(indexOptions.addBatchSize()).isEqualTo(5000);
    }

    @Test
    public void testMDivisibilityValidation() {
        Options options = new Options();
        options.setInteger("ivfpq.index.dimension", 10);
        options.setInteger("ivfpq.m", 3);
        assertThatThrownBy(() -> new IvfpqVectorIndexOptions(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must divide");
    }

    @Test
    public void testInvalidSampleRatio() {
        Options options = new Options();
        options.setDouble("ivfpq.train.sample_ratio", 0.0);
        assertThatThrownBy(() -> new IvfpqVectorIndexOptions(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sample_ratio");

        Options options2 = new Options();
        options2.setDouble("ivfpq.train.sample_ratio", 1.5);
        assertThatThrownBy(() -> new IvfpqVectorIndexOptions(options2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sample_ratio");
    }

    @Test
    public void testMetricParsing() {
        for (String metric : new String[] {"l2", "cosine", "inner_product"}) {
            Options options = new Options();
            options.setString("ivfpq.distance.metric", metric);
            IvfpqVectorIndexOptions indexOptions = new IvfpqVectorIndexOptions(options);
            assertThat(indexOptions.metric().getConfigName()).isEqualTo(metric);
        }
    }

    @Test
    public void testMetricParsingUpperCase() {
        Options options = new Options();
        options.setString("ivfpq.distance.metric", "L2");
        IvfpqVectorIndexOptions indexOptions = new IvfpqVectorIndexOptions(options);
        assertThat(indexOptions.metric()).isEqualTo(IvfpqVectorMetric.L2);
    }
}
