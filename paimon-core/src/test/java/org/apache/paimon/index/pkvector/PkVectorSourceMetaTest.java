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

import org.apache.paimon.io.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PkVectorSourceMeta}. */
class PkVectorSourceMetaTest {

    @Test
    void testRoundTrip() {
        PkVectorSourceMeta metadata =
                new PkVectorSourceMeta(
                        Arrays.asList(
                                new PkVectorSourceFile("data-1", 10),
                                new PkVectorSourceFile("data-2", 20)));

        PkVectorSourceMeta restored = PkVectorSourceMeta.deserialize(metadata.serialize());

        assertThat(restored.sourceFiles()).isEqualTo(metadata.sourceFiles());
    }

    @Test
    void testRejectsTruncatedSourceMetadata() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(128);
        output.writeInt(1);
        output.writeInt(1);
        output.writeUTF("data-1");

        assertThatThrownBy(() -> PkVectorSourceMeta.deserialize(output.getCopyOfBuffer()))
                .hasMessageContaining("Failed to deserialize vector source metadata");
    }
}
