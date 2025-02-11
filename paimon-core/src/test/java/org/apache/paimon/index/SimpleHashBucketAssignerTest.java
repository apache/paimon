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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SimpleHashBucketAssigner}. */
public class SimpleHashBucketAssignerTest {

    @Test
    public void testAssign() {
        SimpleHashBucketAssigner simpleHashBucketAssigner =
                new SimpleHashBucketAssigner(2, 0, 100, -1);

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
    public void testAssignWithUpperBound() {
        SimpleHashBucketAssigner simpleHashBucketAssigner =
                new SimpleHashBucketAssigner(2, 0, 100, 3);

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

        // exceed upper bound
        for (int i = 0; i < 200; i++) {
            int bucket = simpleHashBucketAssigner.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isIn(0, 2);
        }
    }

    @Test
    public void testAssignWithUpperBoundMultiAssigners() {
        SimpleHashBucketAssigner simpleHashBucketAssigner0 =
                new SimpleHashBucketAssigner(2, 0, 100, 3);
        SimpleHashBucketAssigner simpleHashBucketAssigner1 =
                new SimpleHashBucketAssigner(2, 1, 100, 3);

        BinaryRow binaryRow = BinaryRow.EMPTY_ROW;
        int hash = 0;

        for (int i = 0; i < 100; i++) {
            int bucket = simpleHashBucketAssigner0.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isEqualTo(0);
        }

        for (int i = 0; i < 100; i++) {
            int bucket = simpleHashBucketAssigner1.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isEqualTo(1);
        }

        for (int i = 0; i < 100; i++) {
            int bucket = simpleHashBucketAssigner0.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isEqualTo(2);
        }

        // exceed upper bound
        for (int i = 0; i < 200; i++) {
            int bucket = simpleHashBucketAssigner0.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isIn(0, 2);
        }
        for (int i = 0; i < 200; i++) {
            int bucket = simpleHashBucketAssigner1.assign(binaryRow, hash++);
            Assertions.assertThat(bucket).isIn(1);
        }
    }

    @ParameterizedTest(name = "maxBuckets: {0}")
    @ValueSource(ints = {-1, 1, 2})
    public void testAssignWithSameHash(int maxBucketsNum) {
        SimpleHashBucketAssigner simpleHashBucketAssigner =
                new SimpleHashBucketAssigner(2, 0, 100, maxBucketsNum);

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

    @ParameterizedTest(name = "maxBuckets: {0}")
    @ValueSource(ints = {-1, 1, 2})
    public void testPartitionCopy(int maxBucketsNum) {
        SimpleHashBucketAssigner assigner = new SimpleHashBucketAssigner(1, 0, 5, maxBucketsNum);

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
