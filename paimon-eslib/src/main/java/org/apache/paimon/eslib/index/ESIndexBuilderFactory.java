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

package org.apache.paimon.eslib.index;

import org.elasticsearch.eslib.api.ESIndexBuilder;
import org.elasticsearch.eslib.api.ESIndexSearcher;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.builder.DefaultESIndexBuilder;
import org.elasticsearch.eslib.searcher.DefaultESIndexSearcher;

import java.io.IOException;
import java.util.Map;

/** Factory to create ESIndexBuilder/ESIndexSearcher from eslib-core. */
public class ESIndexBuilderFactory {

    public static ESIndexBuilder create(Map<String, FieldIndexConfig> fieldConfigs)
            throws IOException {
        return new DefaultESIndexBuilder(fieldConfigs);
    }

    public static ESIndexSearcher createSearcher() {
        return new DefaultESIndexSearcher();
    }
}
