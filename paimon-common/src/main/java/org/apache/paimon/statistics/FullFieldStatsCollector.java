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
import org.apache.paimon.format.FieldStats;

/** The full stats collector which will report null count, min value, max value if available. */
public class FullFieldStatsCollector extends AbstractFieldStatsCollector {

    @Override
    public void collect(Object field, Serializer<Object> fieldSerializer) {
        if (field == null) {
            nullCount++;
            return;
        }

        // TODO use comparator for not comparable types and extract this logic to a util class
        if (!(field instanceof Comparable)) {
            return;
        }
        Comparable<Object> c = (Comparable<Object>) field;
        if (minValue == null || c.compareTo(minValue) < 0) {
            minValue = fieldSerializer.copy(c);
        }
        if (maxValue == null || c.compareTo(maxValue) > 0) {
            maxValue = fieldSerializer.copy(c);
        }
    }

    @Override
    public FieldStats convert(FieldStats source) {
        return source;
    }
}
