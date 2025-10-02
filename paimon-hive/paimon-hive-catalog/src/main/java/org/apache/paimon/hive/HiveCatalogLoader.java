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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.fs.FileIO;

/** Loader to create {@link HiveCatalog}. */
public class HiveCatalogLoader implements CatalogLoader {

    private static final long serialVersionUID = 2L;

    private final FileIO fileIO;
    private final SerializableHiveConf hiveConf;
    private final String clientClassName;
    private final CatalogContext context;
    private final String warehouse;

    public HiveCatalogLoader(
            FileIO fileIO,
            SerializableHiveConf hiveConf,
            String clientClassName,
            CatalogContext context,
            String warehouse) {
        this.fileIO = fileIO;
        this.hiveConf = hiveConf;
        this.clientClassName = clientClassName;
        this.context = context;
        this.warehouse = warehouse;
    }

    @Override
    public Catalog load() {
        return new HiveCatalog(fileIO, hiveConf.conf(), clientClassName, context, warehouse);
    }
}
