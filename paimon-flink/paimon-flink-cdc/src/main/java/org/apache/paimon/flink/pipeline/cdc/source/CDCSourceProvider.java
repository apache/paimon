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

import org.apache.paimon.catalog.CatalogContext;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;

/** The {@link FlinkSourceProvider} for paimon cdc source. */
public class CDCSourceProvider implements FlinkSourceProvider {
    private final CatalogContext catalogContext;
    private final Configuration cdcConfig;
    private final org.apache.flink.configuration.Configuration flinkConfig;

    public CDCSourceProvider(
            CatalogContext catalogContext,
            Configuration cdcConfig,
            org.apache.flink.configuration.Configuration flinkConfig) {
        this.catalogContext = catalogContext;
        this.cdcConfig = cdcConfig;
        this.flinkConfig = flinkConfig;
    }

    @Override
    public Source<Event, ?, ?> getSource() {
        return new CDCSource(catalogContext, cdcConfig, flinkConfig);
    }
}
