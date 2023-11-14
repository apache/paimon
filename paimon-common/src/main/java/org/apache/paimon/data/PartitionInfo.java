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

package org.apache.paimon.data;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.VectorMappingUtils;

/**
 * Partition infomation about how the row mapping of outer row. used in {@link VectorMappingUtils}.
 */
public class PartitionInfo {

    // This map is a demonstration of how the index column mapping to partition row and outer row.
    // If the value of map[i] is minus, that means the row located in partition row, otherwise, the
    // row is located in outer row.
    // To avoid conflict of map[0] = 0, (because 0 is equals to -0), add every index by 1, that
    // means, if map[0] = -1, then the 0 row is located in partition row (Math.abs(-1) - 1), if
    // map[0] = 1, mean it is located in (Math.abs(1) - 1) in the outer row.
    private final int[] map;
    private final RowType partitionType;
    private final BinaryRow partition;

    public PartitionInfo(int[] map, RowType partitionType, BinaryRow partition) {
        this.map = map;
        this.partitionType = partitionType;
        this.partition = partition;
    }

    public int[] getMap() {
        return map;
    }

    public BinaryRow getPartitionRow() {
        return partition;
    }

    public boolean inPartitionRow(int i) {
        return map[i] < 0;
    }

    public int getRealIndex(int i) {
        return Math.abs(map[i]) - 1;
    }

    public DataType getType(int i) {
        return partitionType.getTypeAt(getRealIndex(i));
    }

    public int size() {
        return map.length - 1;
    }
}
