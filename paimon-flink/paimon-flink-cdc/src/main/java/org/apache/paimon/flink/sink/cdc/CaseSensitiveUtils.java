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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.catalog.Catalog;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Add convert operator if the catalog is case-insensitive. */
public class CaseSensitiveUtils {

    public static DataStream<CdcRecord> cdcRecordConvert(
            CatalogLoader catalogLoader, DataStream<CdcRecord> input) {
        if (caseSensitive(catalogLoader)) {
            return input;
        }

        return input.forward()
                .process(
                        new ProcessFunction<CdcRecord, CdcRecord>() {
                            @Override
                            public void processElement(
                                    CdcRecord record, Context ctx, Collector<CdcRecord> out) {
                                out.collect(record.fieldNameLowerCase());
                            }
                        })
                .name("Case-insensitive Convert");
    }

    public static DataStream<CdcMultiplexRecord> cdcMultiplexRecordConvert(
            CatalogLoader catalogLoader, DataStream<CdcMultiplexRecord> input) {
        if (caseSensitive(catalogLoader)) {
            return input;
        }

        return input.forward()
                .process(
                        new ProcessFunction<CdcMultiplexRecord, CdcMultiplexRecord>() {
                            @Override
                            public void processElement(
                                    CdcMultiplexRecord record,
                                    Context ctx,
                                    Collector<CdcMultiplexRecord> out) {
                                out.collect(record.fieldNameLowerCase());
                            }
                        })
                .name("Case-insensitive Convert");
    }

    private static boolean caseSensitive(CatalogLoader catalogLoader) {
        try (Catalog catalog = catalogLoader.load()) {
            return catalog.caseSensitive();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
