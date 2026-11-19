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

package org.apache.paimon.globalindex;

import org.apache.paimon.index.IndexFileMeta;

import java.util.function.Predicate;

/** Identifies routed vector index files through their registered index type. */
public final class CentroidVectorIndexUtils {
    private CentroidVectorIndexUtils() {}

    public static boolean isRoutingModel(IndexFileMeta file) {
        return file.globalIndexMeta() != null
                && routingModelPredicate(file.indexType()).test(file.globalIndexMeta().indexMeta());
    }

    /** Resolve the plugin once when checking multiple files of the same type. */
    public static Predicate<byte[]> routingModelPredicate(String indexType) {
        CentroidVectorIndexFactory factory = GlobalIndexerFactoryUtils.centroidFactory(indexType);
        return factory == null ? meta -> false : factory::isRoutingModel;
    }
}
