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

package org.apache.paimon.spark.globalindex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Utility class for loading {@link GlobalIndexTopologyBuilder} implementations via Java's {@link
 * ServiceLoader} mechanism.
 *
 * <p>Factories are loaded once during class initialization and cached for subsequent lookups.
 */
public class GlobalIndexTopologyBuilderUtils {

    private static final Logger LOG =
            LoggerFactory.getLogger(GlobalIndexTopologyBuilderUtils.class);

    private static final Map<String, GlobalIndexTopologyBuilder> FACTORIES = new HashMap<>();

    static {
        ServiceLoader<GlobalIndexTopologyBuilder> serviceLoader =
                ServiceLoader.load(GlobalIndexTopologyBuilder.class);

        for (GlobalIndexTopologyBuilder builder : serviceLoader) {
            String identifier = builder.identifier();
            if (FACTORIES.put(identifier, builder) != null) {
                LOG.warn(
                        "Found multiple GlobalIndexBuilderFactory implementations for type '{}'. "
                                + "Using the last one loaded.",
                        identifier);
            }
        }
    }

    public static GlobalIndexTopologyBuilder createTopoBuilder(String indexType) {
        GlobalIndexTopologyBuilder builder = FACTORIES.get(indexType);
        return builder == null ? new DefaultGlobalIndexTopoBuilder() : builder;
    }
}
