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

package org.apache.paimon.index;

import org.apache.paimon.data.BinaryRow;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SimpleHashBucketAssigner}. */
public class SimpleHashBucketAssignerTest {

    @Test
    public void testAssign() {
        SimpleHashBucketAssigner simpleHashBucketAssigner = new SimpleHashBucketAssigner(2, 0, 100);

        BinaryRow binaryRow = BinaryRow.EMPTY_ROW;
        int hash = 0;

        for (int i = 0; i < 100; i++) {
            int bucket = simpleHashBucketAssigner.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isEqualTo(0);
        }

        for (int i = 0; i < 100; i++) {
            int bucket = simpleHashBucketAssigner.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isEqualTo(2);
        }

        int bucket = simpleHashBucketAssigner.assign(binaryRow, hash++);
        Assertions.assertThat(bucket).isEqualTo(4);
    }

    @Test
    public void testAssignWithSameHash() {
        SimpleHashBucketAssigner simpleHashBucketAssigner = new SimpleHashBucketAssigner(2, 0, 100);

        BinaryRow binaryRow = BinaryRow.EMPTY_ROW;
        int hash = 0;

        for (int i = 0; i < 100; i++) {
            int bucket = simpleHashBucketAssigner.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isEqualTo(0);
        }

        // reset hash, the record will go into bucket 0
        hash = 0;
        for (int i = 0; i < 100; i++) {
            int bucket = simpleHashBucketAssigner.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isEqualTo(0);
        }
    }

    @Test
    public void testPartitionCopy() {
        SimpleHashBucketAssigner assigner = new SimpleHashBucketAssigner(1, 0, 5);

        BinaryRow partition = row(1);
        assertThat(assigner.assign(partition, 0)).isEqualTo(0);
        assertThat(assigner.assign(partition, 1)).isEqualTo(0);

        partition.setInt(0, 2);
        assertThat(assigner.assign(partition, 5)).isEqualTo(0);
        assertThat(assigner.assign(partition, 6)).isEqualTo(0);

        assertThat(assigner.currentPartitions()).contains(row(1));
        assertThat(assigner.currentPartitions()).contains(row(2));
    }
}
