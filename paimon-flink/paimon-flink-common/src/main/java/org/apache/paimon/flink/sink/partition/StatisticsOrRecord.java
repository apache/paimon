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

import org.apache.paimon.data.InternalRow;

import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Either a record or a statistics. */
public class StatisticsOrRecord {

    private DataStatistics statistics;
    private InternalRow record;

    private StatisticsOrRecord(DataStatistics statistics, InternalRow record) {
        checkArgument(
                record != null ^ statistics != null,
                "DataStatistics or record, not neither or both");
        this.statistics = statistics;
        this.record = record;
    }

    public static StatisticsOrRecord fromRecord(InternalRow record) {
        return new StatisticsOrRecord(null, record);
    }

    public static StatisticsOrRecord fromStatistics(DataStatistics statistics) {
        return new StatisticsOrRecord(statistics, null);
    }

    public boolean isStatistics() {
        return statistics != null;
    }

    public boolean isRecord() {
        return record != null;
    }

    public DataStatistics statistics() {
        return statistics;
    }

    public InternalRow record() {
        return record;
    }

    @Override
    public String toString() {
        return "StatisticsOrRecord{" + "statistics=" + statistics + ", record=" + record + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StatisticsOrRecord)) {
            return false;
        }
        StatisticsOrRecord that = (StatisticsOrRecord) o;
        return Objects.equals(statistics, that.statistics) && Objects.equals(record, that.record);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statistics, record);
    }
}
