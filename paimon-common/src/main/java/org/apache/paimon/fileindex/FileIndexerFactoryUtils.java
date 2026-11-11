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

package org.apache.paimon.fileindex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** Load utils to load FileIndexerFactory. */
public class FileIndexerFactoryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileIndexerFactoryUtils.class);

    private static final Map<String, FileIndexerFactory> factories = new HashMap<>();

    static {
        // Not the context classloader: it may not see Paimon, e.g. on a Flink JobMaster RPC thread.
        ServiceLoader<FileIndexerFactory> serviceLoader =
                ServiceLoader.load(
                        FileIndexerFactory.class, FileIndexerFactory.class.getClassLoader());

        for (FileIndexerFactory indexerFactory : serviceLoader) {
            if (factories.put(indexerFactory.identifier(), indexerFactory) != null) {
                LOG.warn(
                        "Found multiple FileIndexer for type: "
                                + indexerFactory.identifier()
                                + ", choose one of them");
            }
        }
    }

    private static FileIndexerFactory find(String type) {
        FileIndexerFactory factory = factories.get(type);
        if (factory != null) {
            return factory;
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader == null
                || contextClassLoader == FileIndexerFactory.class.getClassLoader()) {
            return null;
        }
        // Keep job-scoped providers local so their classloaders are not retained or shared.
        for (FileIndexerFactory contextFactory :
                ServiceLoader.load(FileIndexerFactory.class, contextClassLoader)) {
            if (contextFactory.identifier().equals(type)) {
                return contextFactory;
            }
        }
        return null;
    }

    public static boolean isRegistered(String type) {
        return find(type) != null;
    }

    public static FileIndexerFactory load(String type) {
        FileIndexerFactory fileIndexerFactory = find(type);
        if (fileIndexerFactory == null) {
            throw new RuntimeException("Can't find file index for type: " + type);
        }
        return fileIndexerFactory;
    }
}
