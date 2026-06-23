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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TantivyFullTextGlobalIndexerFactory}. */
public class TantivyFullTextGlobalIndexerFactoryTest {

    @Test
    public void testFactoryDisablesSearcherPoolForZeroMaxSize() throws Exception {
        TantivyFullTextGlobalIndexerFactory factory = new TantivyFullTextGlobalIndexerFactory();
        DataField field = new DataField(0, "text", DataTypes.STRING());

        GlobalIndexer pooled = factory.create(field, new Options());
        Options disabledOptions = new Options();
        disabledOptions.set(TantivyFullTextIndexOptions.SEARCHER_POOL_MAX_SIZE, 0);
        GlobalIndexer disabled = factory.create(field, disabledOptions);

        assertThat(searcherPool(pooled)).isNotSameAs(searcherPool(disabled));
    }

    private static TantivySearcherPool searcherPool(GlobalIndexer indexer) throws Exception {
        Field field = TantivyFullTextGlobalIndexer.class.getDeclaredField("searcherPool");
        field.setAccessible(true);
        return (TantivySearcherPool) field.get(indexer);
    }
}
