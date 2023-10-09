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

/** Tests for {@link SimpleBucketAssigner}. */
public class SimpleBucketAssignerTest {

    @Test
    public void testAssign() {
        SimpleBucketAssigner simpleBucketAssigner = new SimpleBucketAssigner(2, 0, 100);

        BinaryRow binaryRow = BinaryRow.EMPTY_ROW;

        for (int i = 0; i < 100; i++) {
            int bucket = simpleBucketAssigner.assign(binaryRow, 0);
            Assertions.assertThat(bucket).isEqualTo(0);
        }

        for (int i = 0; i < 100; i++) {
            int bucket = simpleBucketAssigner.assign(binaryRow, 0);
            Assertions.assertThat(bucket).isEqualTo(2);
        }

        int bucket = simpleBucketAssigner.assign(binaryRow, 0);
        Assertions.assertThat(bucket).isEqualTo(4);
    }
}
