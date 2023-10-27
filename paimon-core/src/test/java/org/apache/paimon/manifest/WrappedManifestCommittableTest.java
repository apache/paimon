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

package org.apache.paimon.manifest;

import org.apache.paimon.catalog.Identifier;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.create;
import static org.assertj.core.api.Assertions.assertThat;

class WrappedManifestCommittableTest {

    @Test
    public void testEquals() {
        ManifestCommittable committable1 = create();
        ManifestCommittable committable2 = create();

        WrappedManifestCommittable wrapped1 = new WrappedManifestCommittable(-1, -1);
        wrapped1.putManifestCommittable(Identifier.create("db", "table1"), committable1);
        wrapped1.putManifestCommittable(Identifier.create("db", "table2"), committable2);

        // add manifest committables in reverse order
        WrappedManifestCommittable wrapped2 = new WrappedManifestCommittable(-1, -1);
        wrapped2.putManifestCommittable(Identifier.create("db", "table2"), committable2);
        wrapped2.putManifestCommittable(Identifier.create("db", "table1"), committable1);

        assertThat(wrapped1).isEqualTo(wrapped2);
    }
}
