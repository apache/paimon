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

import org.apache.paimon.lineage.LineageMetaFactory;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.Lock;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Catalog environment in table which contains log factory, metastore client factory and lineage
 * meta.
 */
public class CatalogEnvironment implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Lock.Factory lockFactory;
    @Nullable private final MetastoreClient.Factory metastoreClientFactory;
    @Nullable private final LineageMetaFactory lineageMetaFactory;

    public CatalogEnvironment(
            Lock.Factory lockFactory,
            @Nullable MetastoreClient.Factory metastoreClientFactory,
            @Nullable LineageMetaFactory lineageMetaFactory) {
        this.lockFactory = lockFactory;
        this.metastoreClientFactory = metastoreClientFactory;
        this.lineageMetaFactory = lineageMetaFactory;
    }

    public Lock.Factory lockFactory() {
        return lockFactory;
    }

    @Nullable
    public MetastoreClient.Factory metastoreClientFactory() {
        return metastoreClientFactory;
    }

    @Nullable
    public LineageMetaFactory lineageMetaFactory() {
        return lineageMetaFactory;
    }
}
