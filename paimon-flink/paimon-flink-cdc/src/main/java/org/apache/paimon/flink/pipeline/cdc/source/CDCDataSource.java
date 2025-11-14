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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;

/** The {@link DataSource} for cdc source. */
public class CDCDataSource implements DataSource {
    private final CatalogContext catalogContext;
    private final Configuration cdcConfig;
    private Catalog catalog;

    public CDCDataSource(CatalogContext catalogContext, Configuration cdcConfig) {
        this.catalogContext = catalogContext;
        this.cdcConfig = cdcConfig;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        return new CDCSourceProvider(catalogContext, cdcConfig);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        if (catalog == null) {
            catalog = CatalogFactory.createCatalog(catalogContext);
        }
        return new CDCMetadataAccessor(catalog);
    }
}
