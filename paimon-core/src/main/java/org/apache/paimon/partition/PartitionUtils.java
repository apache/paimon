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

package org.apache.paimon.partition;

import java.util.stream.IntStream;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import shaded.parquet.it.unimi.dsi.fastutil.ints.IntArrays;

/** Utils to fetch partition map information from data schema and row type. */
public class PartitionUtils {

    public static Pair<int[], int[][]> constructPartitionMapping(
            TableSchema dataSchema) {
        int[][] projection = new int[dataSchema.fields().size()][];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = new int[]{i};
        }
        return constructPartitionMapping(
                dataSchema.logicalRowType(), dataSchema.partitionKeys(), projection);
    }

    public static Pair<int[], int[][]> constructPartitionMapping(
            TableSchema dataSchema, int[][] projection) {
        return constructPartitionMapping(
                dataSchema.logicalRowType(), dataSchema.partitionKeys(), projection);
    }

    public static Pair<int[], int[][]> constructPartitionMapping(
            RowType rowType, List<String> partitions, int[][] projection) {
        List<String> fields = rowType.getFieldNames();
        int[][] dataProjection = Arrays.copyOf(projection, projection.length);

        int[] map = new int[dataProjection.length + 1];
        int pCount = 0;

        for (int i = 0; i < dataProjection.length; i++) {
            String field = fields.get(dataProjection[i][0]);
            if (partitions.contains(field)) {
                // if the map[i] is minus, represent the related column is stored in partition row
                map[i] = -(partitions.indexOf(field) + 1);
                pCount++;
                dataProjection[i][0] = -1;
            } else {
                // else if the map[i] is positive, the related column is stored in the file-read row
                map[i] = (i - pCount) + 1;
            }
        }

        // partition field is not selected, we just return null back
        if (pCount == 0) {
            return null;
        }

        int[][] projectionReturn = new int[dataProjection.length - pCount][];
        int count = 0;
        for (int i = 0; i < dataProjection.length; i++) {
            if (dataProjection[i][0] != -1) {
                projectionReturn[count++] = dataProjection[i];
            }
        }

        return Pair.of(map, projectionReturn);
    }

    public static PartitionInfo create(@Nullable Pair<int[], RowType> pair, BinaryRow binaryRow) {
        return pair == null ? null : new PartitionInfo(pair.getLeft(), pair.getRight(), binaryRow);
    }
}
