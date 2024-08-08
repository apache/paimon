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

package org.apache.paimon.flink.source.table;

import org.apache.paimon.flink.source.FlinkTableSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;

/** The {@link BaseTableSource} with Lookup and watermark. */
public class RichTableSource extends BaseTableSource
        implements LookupTableSource, SupportsWatermarkPushDown {

    private final FlinkTableSource source;
    protected final ReadableConfig config;

    public RichTableSource(FlinkTableSource source, ReadableConfig config) {
        super(source);
        this.source = source;
        this.config = config;
    }

    @Override
    public RichTableSource copy() {
        return new RichTableSource(source.copy(), config);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return source.getLookupRuntimeProvider(context, config);
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        source.pushWatermark(watermarkStrategy);
    }
}
