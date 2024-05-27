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

import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** File index interface. To build a file index. */
public interface FileIndexer {

    Logger LOG = LoggerFactory.getLogger(FileIndexer.class);

    FileIndexWriter createWriter();

    FileIndexReader createReader(byte[] serializedBytes);

    static FileIndexer create(String type, DataType dataType, Options options) {

        FileIndexerFactory fileIndexerFactory = FileIndexerLoadFactory.load(type);
        return fileIndexerFactory.create(dataType, options);
    }

    /** Load factory to load FileIndexerFactory. */
    class FileIndexerLoadFactory {

        private static final Map<String, FileIndexerFactory> factories = new HashMap<>();

        static {
            ServiceLoader<FileIndexerFactory> serviceLoader =
                    ServiceLoader.load(FileIndexerFactory.class);

            for (FileIndexerFactory indexerFactory : serviceLoader) {
                if (factories.put(indexerFactory.identifier(), indexerFactory) != null) {
                    LOG.warn(
                            "Found multiple FileIndexer for type: "
                                    + indexerFactory.identifier()
                                    + ", choose one of them");
                }
            }
        }

        static FileIndexerFactory load(String type) {
            FileIndexerFactory fileIndexerFactory = factories.get(type);
            if (fileIndexerFactory == null) {
                throw new RuntimeException("Can't find file index for type: " + type);
            }
            return fileIndexerFactory;
        }
    }
}
