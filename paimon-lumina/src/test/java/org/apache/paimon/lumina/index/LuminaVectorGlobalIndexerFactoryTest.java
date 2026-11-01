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

import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Lumina global indexer factory identifiers and the indexer metric. */
public class LuminaVectorGlobalIndexerFactoryTest {

    @Test
    public void testIdentifiers() {
        assertThat(new LuminaVectorGlobalIndexerFactory().identifier()).isEqualTo("lumina");
        assertThat(new LegacyLuminaVectorGlobalIndexerFactory().identifier())
                .isEqualTo("lumina-vector-ann");
    }

    @Test
    public void testLoadNewAndLegacyIdentifiers() {
        assertThat(GlobalIndexerFactoryUtils.load("lumina"))
                .isExactlyInstanceOf(LuminaVectorGlobalIndexerFactory.class);
        assertThat(GlobalIndexerFactoryUtils.load("lumina-vector-ann"))
                .isExactlyInstanceOf(LegacyLuminaVectorGlobalIndexerFactory.class);
    }

    @Test
    public void testMetricBeforeReaderUsesConfiguration() {
        LuminaVectorGlobalIndexer indexer = newIndexer();
        assertThat(indexer.metric()).isEqualTo(configuredMetric());
    }

    @Test
    public void testMetricFollowsPersistedIndexMeta() throws Exception {
        LuminaVectorGlobalIndexer indexer = newIndexer();
        readShard(indexer, metaBytes(LuminaVectorMetric.L2));
        assertThat(indexer.metric()).isEqualTo(LuminaVectorMetric.L2.getLuminaName());
    }

    @Test
    public void testConflictingShardMetricsKeepConfiguredMetric() throws Exception {
        LuminaVectorGlobalIndexer indexer = newIndexer();
        readShard(indexer, metaBytes(LuminaVectorMetric.L2));
        readShard(indexer, metaBytes(LuminaVectorMetric.COSINE));
        assertThat(indexer.metric()).isEqualTo(configuredMetric());

        readShard(indexer, metaBytes(LuminaVectorMetric.L2));
        assertThat(indexer.metric()).isEqualTo(configuredMetric());
    }

    @Test
    public void testCorruptMetaKeepsConfiguredMetric() throws Exception {
        LuminaVectorGlobalIndexer indexer = newIndexer();
        readShard(indexer, "not json".getBytes(StandardCharsets.UTF_8));
        assertThat(indexer.metric()).isEqualTo(configuredMetric());

        Map<String, String> unknownMetric = new HashMap<>();
        unknownMetric.put("index.dimension", "2");
        unknownMetric.put("distance.metric", "unknown");
        readShard(indexer, new LuminaIndexMeta(unknownMetric).serialize());
        assertThat(indexer.metric()).isEqualTo(configuredMetric());
    }

    private static LuminaVectorGlobalIndexer newIndexer() {
        return new LuminaVectorGlobalIndexer(DataTypes.ARRAY(DataTypes.FLOAT()), new Options());
    }

    private static String configuredMetric() {
        return LuminaVectorMetric.INNER_PRODUCT.getLuminaName();
    }

    private static byte[] metaBytes(LuminaVectorMetric metric) throws Exception {
        Map<String, String> meta = new HashMap<>();
        meta.put("index.dimension", "2");
        meta.put("distance.metric", metric.getLuminaName());
        return new LuminaIndexMeta(meta).serialize();
    }

    private static void readShard(LuminaVectorGlobalIndexer indexer, byte[] metadata)
            throws Exception {
        GlobalIndexFileReader fileReader =
                meta -> {
                    throw new AssertionError("Index file should not be opened");
                };
        GlobalIndexIOMeta file = new GlobalIndexIOMeta(new Path("/shard.index"), 1L, metadata);
        try (GlobalIndexReader reader =
                indexer.createReader(fileReader, Collections.singletonList(file), 1L, null, null)) {
            assertThat(reader).isInstanceOf(LuminaVectorGlobalIndexReader.class);
        }
    }
}
