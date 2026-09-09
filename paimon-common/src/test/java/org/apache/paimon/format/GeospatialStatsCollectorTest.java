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

package org.apache.paimon.format;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GeospatialStatsCollectorTest {

    @Test
    void testWkbDoesNotProduceLexicographicBounds() {
        SimpleStatsCollector collector =
                new SimpleStatsCollector(RowType.of(DataTypes.GEOMETRY(), DataTypes.GEOGRAPHY()));

        collector.collect(GenericRow.of(new byte[] {2}, new byte[] {9}));
        collector.collect(GenericRow.of(new byte[] {1}, null));

        SimpleColStats[] stats = collector.extract();
        assertThat(stats[0].min()).isNull();
        assertThat(stats[0].max()).isNull();
        assertThat(stats[0].nullCount()).isZero();
        assertThat(stats[1].min()).isNull();
        assertThat(stats[1].max()).isNull();
        assertThat(stats[1].nullCount()).isOne();
    }
}
