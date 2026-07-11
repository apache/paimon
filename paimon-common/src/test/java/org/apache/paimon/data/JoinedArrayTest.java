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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link JoinedArray}. */
public class JoinedArrayTest {

    @Test
    public void testFlattenNestedJoinedArrays() {
        JoinedArray left =
                new JoinedArray(new GenericArray(new int[] {1}), new GenericArray(new int[] {2}));
        JoinedArray right =
                new JoinedArray(new GenericArray(new int[] {3}), new GenericArray(new int[] {4}));
        JoinedArray joined = new JoinedArray(left, right);

        assertThat(joined.arrays()).hasSize(4);
        assertThat(joined.toIntArray()).containsExactly(1, 2, 3, 4);
    }
}
