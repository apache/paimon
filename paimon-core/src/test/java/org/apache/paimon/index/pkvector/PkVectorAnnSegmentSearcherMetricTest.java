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

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the segment metric validation in {@link PkVectorAnnSegmentSearcher}. */
class PkVectorAnnSegmentSearcherMetricTest {

    private static VectorGlobalIndexer indexerWithMetric(String segmentMetric) {
        return new VectorGlobalIndexer() {
            @Override
            public String metric() {
                return "inner_product";
            }

            @Override
            public String segmentMetric(byte[] indexMeta) {
                return segmentMetric;
            }

            @Override
            public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter)
                    throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public GlobalIndexReader createReader(
                    GlobalIndexFileReader fileReader,
                    List<GlobalIndexIOMeta> files,
                    long totalRowCount,
                    List<org.apache.paimon.utils.Range> rowRanges,
                    ExecutorService executor) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Test
    void testSegmentMetricMismatchIsRejected() {
        assertThatThrownBy(
                        () ->
                                PkVectorAnnSegmentSearcher.checkSegmentMetric(
                                        indexerWithMetric("cosine"), "l2", new byte[] {1}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("built with metric cosine but the current metric is l2");
    }

    @Test
    void testMatchingAndLegacySegmentMetricsPass() {
        assertThatCode(
                        () ->
                                PkVectorAnnSegmentSearcher.checkSegmentMetric(
                                        indexerWithMetric("cosine"), "cosine", new byte[] {1}))
                .doesNotThrowAnyException();
        // legacy segments record no metric and must not be rejected
        assertThatCode(
                        () ->
                                PkVectorAnnSegmentSearcher.checkSegmentMetric(
                                        indexerWithMetric(null), "l2", new byte[] {1}))
                .doesNotThrowAnyException();
    }
}
