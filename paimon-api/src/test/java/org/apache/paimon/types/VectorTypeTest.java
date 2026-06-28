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

package org.apache.paimon.types;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorType}. */
class VectorTypeTest {

    @Test
    void testDefaultSize() {
        assertThat(DataTypes.VECTOR(3, DataTypes.FLOAT()).defaultSize()).isEqualTo(12);
        assertThat(DataTypes.VECTOR(2, DataTypes.DOUBLE()).defaultSize()).isEqualTo(16);
    }

    @Test
    void testDefaultSizeOverflowReturnsMaxValue() {
        assertThat(DataTypes.VECTOR(Integer.MAX_VALUE, DataTypes.FLOAT()).defaultSize())
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(DataTypes.VECTOR(Integer.MAX_VALUE, DataTypes.DOUBLE()).defaultSize())
                .isEqualTo(Integer.MAX_VALUE);
    }
}
