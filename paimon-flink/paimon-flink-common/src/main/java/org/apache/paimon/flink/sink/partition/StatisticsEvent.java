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

package org.apache.paimon.flink.sink.partition;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/** Event to send statistics between operator and coordinator. */
class StatisticsEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;
    private final byte[] statisticsBytes;

    private StatisticsEvent(long checkpointId, byte[] statisticsBytes) {
        this.checkpointId = checkpointId;
        this.statisticsBytes = statisticsBytes;
    }

    static StatisticsEvent createStatisticsEvent(
            long checkpointId,
            DataStatistics statistics,
            TypeSerializer<DataStatistics> statisticsSerializer) {
        return new StatisticsEvent(
                checkpointId,
                StatisticsUtil.serializeDataStatistics(statistics, statisticsSerializer));
    }

    long getCheckpointId() {
        return checkpointId;
    }

    byte[] getStatisticsBytes() {
        return statisticsBytes;
    }
}
