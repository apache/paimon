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

package org.apache.paimon.statistics;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.SimpleColStats;

import java.util.Arrays;
import java.util.regex.Matcher;

import static org.apache.paimon.statistics.TruncateSimpleColStatsCollector.TRUNCATE_PATTERN;

/** The mode of the col stats. */
public interface SimpleColStatsCollector {

    /**
     * collect stats from the field.
     *
     * @param field The target field object.
     * @param fieldSerializer The serializer of the field object.
     */
    void collect(Object field, Serializer<Object> fieldSerializer);

    /** @return The collected col stats. */
    SimpleColStats result();

    /**
     * Convert the col stats according to the strategy.
     *
     * @param source The source col stats, extracted from the file.
     * @return The converted col stats.
     */
    SimpleColStats convert(SimpleColStats source);

    /** Factory to create {@link SimpleColStatsCollector}. */
    interface Factory {
        SimpleColStatsCollector create();
    }

    static SimpleColStatsCollector[] create(SimpleColStatsCollector.Factory[] factories) {
        SimpleColStatsCollector[] collectors = new SimpleColStatsCollector[factories.length];
        for (int i = 0; i < factories.length; i++) {
            collectors[i] = factories[i].create();
        }
        return collectors;
    }

    static Factory from(String option) {
        String upper = option.toUpperCase();
        switch (upper) {
            case "NONE":
                return NoneSimpleColStatsCollector::new;
            case "FULL":
                return FullSimpleColStatsCollector::new;
            case "COUNTS":
                return CountsSimpleColStatsCollector::new;
            default:
                Matcher matcher = TRUNCATE_PATTERN.matcher(upper);
                if (matcher.matches()) {
                    String length = matcher.group(1);
                    return () -> new TruncateSimpleColStatsCollector(Integer.parseInt(length));
                }
                throw new IllegalArgumentException("Unexpected option: " + option);
        }
    }

    static SimpleColStatsCollector.Factory[] createFullStatsFactories(int numFields) {
        SimpleColStatsCollector.Factory[] factories =
                new SimpleColStatsCollector.Factory[numFields];
        Arrays.fill(factories, (SimpleColStatsCollector.Factory) FullSimpleColStatsCollector::new);
        return factories;
    }
}
