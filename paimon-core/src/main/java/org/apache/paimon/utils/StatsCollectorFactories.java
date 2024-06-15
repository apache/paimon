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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.SimpleColStatsCollector;

import java.util.List;

import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.CoreOptions.STATS_MODE_SUFFIX;
import static org.apache.paimon.options.ConfigOptions.key;

/** The stats utils to create {@link SimpleColStatsCollector.Factory}s. */
public class StatsCollectorFactories {

    public static SimpleColStatsCollector.Factory[] createStatsFactories(
            CoreOptions options, List<String> fields) {
        Options cfg = options.toConfiguration();
        SimpleColStatsCollector.Factory[] modes =
                new SimpleColStatsCollector.Factory[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            String fieldMode =
                    cfg.get(
                            key(String.format(
                                            "%s.%s.%s",
                                            FIELDS_PREFIX, fields.get(i), STATS_MODE_SUFFIX))
                                    .stringType()
                                    .noDefaultValue());
            if (fieldMode != null) {
                modes[i] = SimpleColStatsCollector.from(fieldMode);
            } else {
                modes[i] = SimpleColStatsCollector.from(cfg.get(CoreOptions.METADATA_STATS_MODE));
            }
        }
        return modes;
    }
}
