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

package org.apache.paimon.predicate;

import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FieldRef}. */
class FieldRefTest {

    @Test
    void testTopLevelName() {
        FieldRef topLevel = new FieldRef(0, "a.b", DataTypes.INT());
        assertThat(topLevel.topLevelName()).isEqualTo("a.b");

        FieldRef nested = new FieldRef(0, "a.b", DataTypes.INT(), new int[] {1}, new int[] {2});
        assertThat(nested.topLevelName()).isEqualTo("a");

        FieldRef deepNested =
                new FieldRef(0, "a.b.c", DataTypes.INT(), new int[] {1, 0}, new int[] {2, 3});
        assertThat(deepNested.topLevelName()).isEqualTo("a");
    }

    @Test
    void testWithIndexPreservesNestedMetadata() {
        FieldRef nested = new FieldRef(3, "a.b", DataTypes.INT(), new int[] {1}, new int[] {2});
        FieldRef remapped = nested.withIndex(0);
        assertThat(remapped.index()).isEqualTo(0);
        assertThat(remapped.name()).isEqualTo("a.b");
        assertThat(remapped.type()).isEqualTo(DataTypes.INT());
        assertThat(remapped.nestedIndexes()).containsExactly(1);
        assertThat(remapped.nestedArities()).containsExactly(2);
    }
}
