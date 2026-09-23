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

package org.apache.paimon.fulltext.index;

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NativeFullTextGlobalIndexerFactory}. */
public class NativeFullTextGlobalIndexerFactoryTest {

    @Test
    public void testFactoryAcceptsFullTextSearcherPoolOption() {
        NativeFullTextGlobalIndexerFactory factory = new NativeFullTextGlobalIndexerFactory();
        DataField field = new DataField(0, "text", DataTypes.STRING());

        Options options = new Options();
        options.set("full-text.searcher-pool.max-size", "0");
        GlobalIndexer indexer = factory.create(field, options);

        assertThat(indexer).isInstanceOf(NativeFullTextGlobalIndexer.class);
    }
}
