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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** Tests for {@link VectorMappingUtils}. */
public class VectorMappingUtilsTest {

    @Test
    public void testCreatePartitionMappedVectors() {
        ColumnVector[] columnVectors = new ColumnVector[5];

        Arrays.fill(columnVectors, (ColumnVector) i -> false);

        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);
        binaryRowWriter.writeString(0, BinaryString.fromString("a"));
        binaryRowWriter.complete();

        PartitionInfo partitionInfo =
                new PartitionInfo(
                        new int[] {1, 2, -1, 3, 4, 5, 0},
                        RowType.of(DataTypes.STRING()),
                        binaryRow);

        ColumnVector[] newColumnVectors =
                VectorMappingUtils.createPartitionMappedVectors(partitionInfo, columnVectors);

        for (int i = 0; i < partitionInfo.size(); i++) {
            if (!partitionInfo.isPartitionRow(i)) {
                Assertions.assertThat(newColumnVectors[i])
                        .isEqualTo(columnVectors[partitionInfo.getRealIndex(i)]);
            }
        }
    }

    @Test
    public void testCreateIndexMappedVectors() {

        ColumnVector[] columnVectors = new ColumnVector[5];

        Arrays.fill(columnVectors, (ColumnVector) i -> false);

        int[] mapping = new int[] {0, 2, 1, 3, 2, 3, 1, 0, 4};

        ColumnVector[] newColumnVectors =
                VectorMappingUtils.createIndexMappedVectors(mapping, columnVectors);

        for (int i = 0; i < mapping.length; i++) {
            Assertions.assertThat(newColumnVectors[i]).isEqualTo(columnVectors[mapping[i]]);
        }
    }
}
