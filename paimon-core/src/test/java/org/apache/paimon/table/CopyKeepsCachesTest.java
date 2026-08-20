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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DVMetaCache;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A caching catalog installs its caches on the table it loads, but engines read through a copy
 * carrying dynamic options. Every cache has to survive that copy or it is inert in practice.
 */
public class CopyKeepsCachesTest extends TableTestBase {

    @Test
    public void testCopyKeepsTheDeletionVectorMetaCache() throws Exception {
        Identifier id = identifier("dv_cache");
        catalog.createTable(
                id,
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build(),
                false);
        AbstractFileStoreTable table = (AbstractFileStoreTable) catalog.getTable(id);

        DVMetaCache dvMetaCache = new DVMetaCache(100);
        table.setDVMetaCache(dvMetaCache);

        AbstractFileStoreTable copied =
                (AbstractFileStoreTable)
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_MAX_SPLITS_PER_TASK.key(), "7"));

        assertThat(copied.dvmetaCache).isSameAs(dvMetaCache);
    }
}
