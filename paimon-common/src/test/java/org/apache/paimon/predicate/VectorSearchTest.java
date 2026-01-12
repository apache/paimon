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

package org.apache.paimon.predicate;

import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test vector search. */
public class VectorSearchTest {

    @Test
    public void testVectorSearchOffset() {
        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f}, new float[] {0.95f, 0.1f}, new float[] {0.1f, 0.95f},
                    new float[] {0.98f, 0.05f}, new float[] {0.0f, 1.0f}, new float[] {0.05f, 0.98f}
                };

        VectorSearch vectorSearch = new VectorSearch(vectors[0], 1, "test");

        RoaringNavigableMap64 includeRowIds = new RoaringNavigableMap64();
        includeRowIds.addRange(new Range(100L, 200L));
        vectorSearch.withIncludeRowIds(includeRowIds);

        vectorSearch = vectorSearch.offsetRange(60, 150);

        List<Range> ranges = vectorSearch.includeRowIds().toRangeList();
        assertThat(ranges.get(0)).isEqualTo(new Range(40L, 90L));
    }
}
