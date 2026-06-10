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
import org.apache.paimon.index.ivfpq.IndexType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
    public void testFactoryIndexType() {
        assertThat(new IvfFlatVectorGlobalIndexerFactory().indexType())
                .isEqualTo(IndexType.IVF_FLAT);
        assertThat(new IvfPqAlgorithmVectorGlobalIndexerFactory().indexType())
                .isEqualTo(IndexType.IVF_PQ);
        assertThat(new IvfHnswFlatVectorGlobalIndexerFactory().indexType())
                .isEqualTo(IndexType.IVF_HNSW_FLAT);
        assertThat(new IvfHnswSqVectorGlobalIndexerFactory().indexType())
                .isEqualTo(IndexType.IVF_HNSW_SQ);
    }
}
