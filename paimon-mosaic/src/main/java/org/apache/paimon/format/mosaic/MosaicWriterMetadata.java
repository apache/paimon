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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.mosaic.ColumnStatistics;

import java.util.List;
import java.util.Map;

/** In-memory metadata captured from MosaicWriter after close. */
public class MosaicWriterMetadata {

    private final int numRowGroups;
    private final List<Map<String, ColumnStatistics>> rowGroupStats;
    private final List<String> statsColumnNames;

    public MosaicWriterMetadata(
            int numRowGroups,
            List<Map<String, ColumnStatistics>> rowGroupStats,
            List<String> statsColumnNames) {
        this.numRowGroups = numRowGroups;
        this.rowGroupStats = rowGroupStats;
        this.statsColumnNames = statsColumnNames;
    }

    public int numRowGroups() {
        return numRowGroups;
    }

    public Map<String, ColumnStatistics> getRowGroupStatistics(int rowGroupIndex) {
        return rowGroupStats.get(rowGroupIndex);
    }

    public List<String> statsColumnNames() {
        return statsColumnNames;
    }
}
