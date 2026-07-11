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

package org.apache.paimon.index.pkvector;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PkVectorRawSegmentMeta}. */
class PkVectorRawSegmentMetaTest {

    @Test
    void testRoundTrip() {
        PkVectorRawSegmentMeta metadata =
                new PkVectorRawSegmentMeta(
                        "index-definition", new PkVectorSourceFile("data-1", 100));

        PkVectorRawSegmentMeta restored = PkVectorRawSegmentMeta.deserialize(metadata.serialize());

        assertThat(restored.indexDefinitionId()).isEqualTo("index-definition");
        assertThat(restored.sourceFile()).isEqualTo(new PkVectorSourceFile("data-1", 100));
    }

    @Test
    void testRejectsTrailingBytes() {
        PkVectorRawSegmentMeta metadata =
                new PkVectorRawSegmentMeta("index", new PkVectorSourceFile("data", 1));
        byte[] bytes = Arrays.copyOf(metadata.serialize(), metadata.serialize().length + 1);

        assertThatThrownBy(() -> PkVectorRawSegmentMeta.deserialize(bytes))
                .hasMessageContaining("Unexpected trailing bytes");
    }
}
