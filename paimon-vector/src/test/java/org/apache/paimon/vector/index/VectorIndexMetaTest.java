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

package org.apache.paimon.vector.index;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorIndexMeta} metric persistence. */
class VectorIndexMetaTest {

    @Test
    void testMetricRoundTrip() throws Exception {
        byte[] data = new VectorIndexMeta("cosine").serialize();
        assertThat(VectorIndexMeta.deserialize(data).metric()).isEqualTo("cosine");

        // a null metric writes an empty map, matching legacy segments
        byte[] legacy = new VectorIndexMeta(null).serialize();
        assertThat(VectorIndexMeta.deserialize(legacy).metric()).isNull();
        assertThat(VectorIndexMeta.deserialize("{}".getBytes()).metric()).isNull();
    }
}
