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

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Utility class for loading {@link GlobalIndexBuilderFactory} implementations via Java's {@link
 * ServiceLoader} mechanism.
 *
 * <p>Factories are loaded once during class initialization and cached for subsequent lookups.
 */
public class GlobalIndexBuilderFactoryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexBuilderFactoryUtils.class);

    private static final Map<String, GlobalIndexBuilderFactory> FACTORIES = new HashMap<>();

    static {
        ServiceLoader<GlobalIndexBuilderFactory> serviceLoader =
                ServiceLoader.load(GlobalIndexBuilderFactory.class);

        for (GlobalIndexBuilderFactory factory : serviceLoader) {
            String identifier = factory.identifier();
            if (FACTORIES.put(identifier, factory) != null) {
                LOG.warn(
                        "Found multiple GlobalIndexBuilderFactory implementations for type '{}'. "
                                + "Using the last one loaded.",
                        identifier);
            }
        }
    }

    public static GlobalIndexBuilder createIndexBuilder(GlobalIndexBuilderContext context) {
        GlobalIndexBuilderFactory factory = FACTORIES.get(context.indexType());
        if (factory == null) {
            return new DefaultGlobalIndexBuilder(context);
        }
        return factory.create(context);
    }

    @Nullable
    public static GlobalIndexTopoBuilder createTopoBuilder(String indexType) {
        GlobalIndexBuilderFactory factory = FACTORIES.get(indexType);
        if (factory == null) {
            return null;
        }
        return factory.createTopoBuilder();
    }
}
