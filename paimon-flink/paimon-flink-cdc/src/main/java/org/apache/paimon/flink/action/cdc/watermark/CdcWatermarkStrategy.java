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

package org.apache.paimon.flink.action.cdc.watermark;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * Watermark strategy for CDC sources, generating watermarks based on timestamps extracted from
 * records.
 */
public class CdcWatermarkStrategy implements WatermarkStrategy<CdcSourceRecord> {

    private final CdcTimestampExtractor timestampExtractor;
    private static final long serialVersionUID = 1L;
    private long currentMaxTimestamp;

    public CdcWatermarkStrategy(CdcTimestampExtractor extractor) {
        this.timestampExtractor = extractor;
    }

    @Override
    public WatermarkGenerator<CdcSourceRecord> createWatermarkGenerator(
            WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<CdcSourceRecord>() {

            @Override
            public void onEvent(CdcSourceRecord record, long timestamp, WatermarkOutput output) {
                long tMs;
                try {
                    tMs = timestampExtractor.extractTimestamp(record);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
                // If the record is a schema-change event ts_ms would be null, just ignore the
                // record.
                if (tMs != Long.MIN_VALUE) {
                    currentMaxTimestamp = Math.max(currentMaxTimestamp, tMs);
                    output.emitWatermark(new Watermark(currentMaxTimestamp - 1));
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                long timeMillis = System.currentTimeMillis();
                currentMaxTimestamp = Math.max(timeMillis, currentMaxTimestamp);
                output.emitWatermark(new Watermark(currentMaxTimestamp - 1));
            }
        };
    }
}
