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

package org.apache.paimon.vector;

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link VectorGlobalIndexerFactory}. */
public class VectorGlobalIndexTest {

    @Test
    public void testFactoryRegistration() {
        GlobalIndexerFactory factory =
                GlobalIndexerFactoryUtils.load(VectorGlobalIndexerFactory.IDENTIFIER);
        assertThat(factory).isNotNull();
        assertThat(factory).isInstanceOf(VectorGlobalIndexerFactory.class);
        assertThat(factory.identifier()).isEqualTo("vector");
    }

    @Test
    public void testCreateIndexer() {
        GlobalIndexerFactory factory = new VectorGlobalIndexerFactory();
        DataType vectorType = new ArrayType(new FloatType());
        Options options = new Options();
        options.setInteger("vector.dim", 128);
        options.setString("vector.metric", "COSINE");

        GlobalIndexer indexer = factory.create(vectorType, options);
        assertThat(indexer).isNotNull();
        assertThat(indexer).isInstanceOf(VectorGlobalIndexer.class);
    }
}
