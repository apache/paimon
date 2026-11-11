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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/** Load utils to load GlobalIndexerFactory. */
public class GlobalIndexerFactoryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexerFactoryUtils.class);

    private static final Map<String, GlobalIndexerFactory> factories = new HashMap<>();

    static {
        // Not the context classloader: it may not see Paimon, e.g. on a Flink JobMaster RPC thread.
        ServiceLoader<GlobalIndexerFactory> serviceLoader =
                ServiceLoader.load(
                        GlobalIndexerFactory.class, GlobalIndexerFactory.class.getClassLoader());

        for (GlobalIndexerFactory indexerFactory : serviceLoader) {
            if (factories.put(indexerFactory.identifier(), indexerFactory) != null) {
                LOG.warn(
                        "Found multiple GlobalIndexer for type: "
                                + indexerFactory.identifier()
                                + ", choose one of them");
            }
        }
    }

    private static GlobalIndexerFactory find(String type) {
        GlobalIndexerFactory factory = factories.get(type);
        if (factory != null) {
            return factory;
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader == null
                || contextClassLoader == GlobalIndexerFactory.class.getClassLoader()) {
            return null;
        }
        // Keep job-scoped providers local so their classloaders are not retained or shared.
        for (GlobalIndexerFactory contextFactory :
                ServiceLoader.load(GlobalIndexerFactory.class, contextClassLoader)) {
            if (contextFactory.identifier().equals(type)) {
                return contextFactory;
            }
        }
        return null;
    }

    public static GlobalIndexerFactory load(String type) {
        GlobalIndexerFactory globalIndexerFactory = find(type);
        if (globalIndexerFactory == null) {
            throw new RuntimeException("Can't find global index for type: " + type);
        }
        return globalIndexerFactory;
    }

    /** Keep unrecognized index types for the reader to resolve at execution time. */
    public static List<GlobalIndexIOMeta> selectFiles(
            String type,
            List<DataField> indexFields,
            Predicate predicate,
            List<GlobalIndexIOMeta> files) {
        GlobalIndexerFactory factory = find(type);
        return factory == null ? files : factory.selectFiles(indexFields, predicate, files);
    }
}
