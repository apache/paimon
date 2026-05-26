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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowRangeMappingIndex}. */
public class RowRangeMappingIndexTest {

    @Test
    public void testMapSingleRange() {
        RowRangeMappingIndex index =
                RowRangeMappingIndex.create(
                        Arrays.asList(RowRangeMappingIndex.mapping(10, 19, 100)));

        assertThat(index.map(new Range(12, 15))).isEqualTo(new Range(102, 105));
    }

    @Test
    public void testMapAcrossContiguousRanges() {
        RowRangeMappingIndex index =
                RowRangeMappingIndex.create(
                        Arrays.asList(
                                RowRangeMappingIndex.mapping(10, 14, 100),
                                RowRangeMappingIndex.mapping(15, 19, 105),
                                RowRangeMappingIndex.mapping(20, 24, 110)));

        assertThat(index.map(new Range(12, 22))).isEqualTo(new Range(102, 112));
    }

    @Test
    public void testMapFailsWhenOldRangeIsNotCovered() {
        RowRangeMappingIndex index =
                RowRangeMappingIndex.create(
                        Arrays.asList(
                                RowRangeMappingIndex.mapping(10, 14, 100),
                                RowRangeMappingIndex.mapping(20, 24, 105)));

        assertThatThrownBy(() -> index.map(new Range(12, 22)))
                .hasMessageContaining("is not fully covered");
    }

    @Test
    public void testMapFailsWhenNewRangeIsNotContiguous() {
        RowRangeMappingIndex index =
                RowRangeMappingIndex.create(
                        Arrays.asList(
                                RowRangeMappingIndex.mapping(10, 14, 100),
                                RowRangeMappingIndex.mapping(15, 19, 200)));

        assertThatThrownBy(() -> index.map(new Range(12, 17)))
                .hasMessageContaining("maps to non-contiguous new row range");
    }
}
