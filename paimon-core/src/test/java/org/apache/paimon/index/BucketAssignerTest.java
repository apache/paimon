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

import org.junit.jupiter.api.Test;

import static java.lang.Integer.MAX_VALUE;
import static org.apache.paimon.index.BucketAssigner.computeAssigner;
import static org.assertj.core.api.Assertions.assertThat;

class BucketAssignerTest {

    @Test
    public void testComputeAssigner() {
        assertThat(computeAssigner(MAX_VALUE, 0, 5, 5)).isEqualTo(2);
        assertThat(computeAssigner(MAX_VALUE, 1, 5, 5)).isEqualTo(3);
        assertThat(computeAssigner(MAX_VALUE, 2, 5, 5)).isEqualTo(4);
        assertThat(computeAssigner(MAX_VALUE, 3, 5, 5)).isEqualTo(0);

        assertThat(computeAssigner(2, 0, 5, 3)).isEqualTo(2);
        assertThat(computeAssigner(2, 1, 5, 3)).isEqualTo(3);
        assertThat(computeAssigner(2, 2, 5, 3)).isEqualTo(4);
        assertThat(computeAssigner(2, 3, 5, 3)).isEqualTo(2);

        assertThat(computeAssigner(3, 1, 5, 1)).isEqualTo(3);
        assertThat(computeAssigner(3, 2, 5, 1)).isEqualTo(3);
    }
}
