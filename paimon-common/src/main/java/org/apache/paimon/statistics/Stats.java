/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.statistics;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.FieldStats;

import java.util.regex.Matcher;

import static org.apache.paimon.statistics.TruncateStats.TRUNCATE_PATTERN;

/** The mode of the Stats */
public interface Stats {

    /**
     * collect stats from the field.
     *
     * @param field The target field object.
     * @param fieldSerializer The serializer of the field object.
     */
    void collect(Object field, Serializer<Object> fieldSerializer);

    /** @return The collected field stats. */
    FieldStats result();

    /**
     * Convert the field stats according to the strategy.
     *
     * @param source The source field stats, extracted from the file.
     * @return The converted field stats.
     */
    FieldStats convert(FieldStats source);

    static Stats from(String option) {
        String upper = option.toUpperCase();
        switch (upper) {
            case "NONE":
                return new NoneStats();
            case "FULL":
                return new FullStats();
            case "COUNTS":
                return new CountsStats();
            default:
                Matcher matcher = TRUNCATE_PATTERN.matcher(upper);
                if (matcher.matches()) {
                    String length = matcher.group(1);
                    return new TruncateStats(Integer.parseInt(length));
                }
                throw new IllegalArgumentException("Unexpected option: " + option);
        }
    }
}
