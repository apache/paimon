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

package org.apache.paimon.lumina.index;

import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Lumina global indexer factory identifiers. */
public class LuminaVectorGlobalIndexerFactoryTest {

    @Test
    public void testIdentifiers() {
        assertThat(new LuminaVectorGlobalIndexerFactory().identifier()).isEqualTo("lumina");
        assertThat(new LegacyLuminaVectorGlobalIndexerFactory().identifier())
                .isEqualTo("lumina-vector-ann");
    }

    @Test
    public void testLoadNewAndLegacyIdentifiers() {
        assertThat(GlobalIndexerFactoryUtils.load("lumina"))
                .isExactlyInstanceOf(LuminaVectorGlobalIndexerFactory.class);
        assertThat(GlobalIndexerFactoryUtils.load("lumina-vector-ann"))
                .isExactlyInstanceOf(LegacyLuminaVectorGlobalIndexerFactory.class);
    }
}
