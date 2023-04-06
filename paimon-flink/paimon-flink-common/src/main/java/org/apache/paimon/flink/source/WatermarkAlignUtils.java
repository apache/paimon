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

package org.apache.paimon.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.data.RowData;

import java.time.Duration;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Since Flink 1.15, watermark alignment is supported. */
public class WatermarkAlignUtils {

    public static WatermarkStrategy<RowData> withWatermarkAlignment(
            WatermarkStrategy<RowData> strategy,
            String group,
            Duration drift,
            Duration updateInterval) {
        checkArgument(
                drift != null,
                String.format(
                        "Watermark alignment max drift can not be null when group (%s) configured.",
                        group));
        return strategy.withWatermarkAlignment(group, drift, updateInterval);
    }
}
