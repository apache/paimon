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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.cache.CaffeineCache;
import org.apache.paimon.io.cache.GuavaCache;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MemoryFileStoreWrite}. */
public class MemoryFileStoreWriteTest {

    @Test
    public void testCacheTypeForPrimaryKeyClustering() {
        Options options = new Options();
        assertThat(MemoryFileStoreWrite.createCacheManager(new CoreOptions(options)).dataCache())
                .isInstanceOf(GuavaCache.class);

        options.set(CoreOptions.PK_CLUSTERING_OVERRIDE, true);
        assertThat(MemoryFileStoreWrite.createCacheManager(new CoreOptions(options)).dataCache())
                .isInstanceOf(CaffeineCache.class);
    }
}
