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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/** Serializer for {@link StatisticsOrRecord}. */
public class StatisticsOrRecordSerializer extends TypeSerializer<StatisticsOrRecord> {

    private static final long serialVersionUID = 1L;

    private final TypeSerializer<DataStatistics> statisticsSerializer;
    private final TypeSerializer<InternalRow> recordSerializer;

    public StatisticsOrRecordSerializer(
            TypeSerializer<DataStatistics> statisticsSerializer,
            TypeSerializer<InternalRow> recordSerializer) {
        this.statisticsSerializer = statisticsSerializer;
        this.recordSerializer = recordSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @SuppressWarnings("ReferenceEquality")
    @Override
    public TypeSerializer<StatisticsOrRecord> duplicate() {
        TypeSerializer<DataStatistics> dupStats = statisticsSerializer.duplicate();
        TypeSerializer<InternalRow> dupRecord = recordSerializer.duplicate();
        if ((statisticsSerializer != dupStats) || (recordSerializer != dupRecord)) {
            return new StatisticsOrRecordSerializer(dupStats, dupRecord);
        }
        return this;
    }

    @Override
    public StatisticsOrRecord createInstance() {
        return StatisticsOrRecord.fromRecord(recordSerializer.createInstance());
    }

    @Override
    public StatisticsOrRecord copy(StatisticsOrRecord from) {
        if (from.isRecord()) {
            return StatisticsOrRecord.fromRecord(recordSerializer.copy(from.record()));
        } else {
            return StatisticsOrRecord.fromStatistics(statisticsSerializer.copy(from.statistics()));
        }
    }

    @Override
    public StatisticsOrRecord copy(StatisticsOrRecord from, StatisticsOrRecord reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(StatisticsOrRecord statisticsOrRecord, DataOutputView target)
            throws IOException {
        if (statisticsOrRecord.isRecord()) {
            target.writeBoolean(true);
            recordSerializer.serialize(statisticsOrRecord.record(), target);
        } else {
            target.writeBoolean(false);
            statisticsSerializer.serialize(statisticsOrRecord.statistics(), target);
        }
    }

    @Override
    public StatisticsOrRecord deserialize(DataInputView source) throws IOException {
        boolean isRecord = source.readBoolean();
        if (isRecord) {
            return StatisticsOrRecord.fromRecord(recordSerializer.deserialize(source));
        } else {
            return StatisticsOrRecord.fromStatistics(statisticsSerializer.deserialize(source));
        }
    }

    @Override
    public StatisticsOrRecord deserialize(StatisticsOrRecord reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        boolean isRecord = source.readBoolean();
        target.writeBoolean(isRecord);
        if (isRecord) {
            recordSerializer.copy(source, target);
        } else {
            statisticsSerializer.copy(source, target);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StatisticsOrRecordSerializer)) {
            return false;
        }
        StatisticsOrRecordSerializer other = (StatisticsOrRecordSerializer) obj;
        return Objects.equals(statisticsSerializer, other.statisticsSerializer)
                && Objects.equals(recordSerializer, other.recordSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statisticsSerializer, recordSerializer);
    }

    @Override
    public TypeSerializerSnapshot<StatisticsOrRecord> snapshotConfiguration() {
        return new StatisticsOrRecordSerializerSnapshot(this);
    }

    /** Snapshot for {@link StatisticsOrRecordSerializer}. */
    public static class StatisticsOrRecordSerializerSnapshot
            extends CompositeTypeSerializerSnapshot<
                    StatisticsOrRecord, StatisticsOrRecordSerializer> {
        private static final int CURRENT_VERSION = 1;

        @SuppressWarnings("unused")
        public StatisticsOrRecordSerializerSnapshot() {}

        public StatisticsOrRecordSerializerSnapshot(StatisticsOrRecordSerializer serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return CURRENT_VERSION;
        }

        @Override
        @SuppressWarnings("rawtypes")
        protected TypeSerializer<?>[] getNestedSerializers(
                StatisticsOrRecordSerializer outerSerializer) {
            return new TypeSerializer<?>[] {
                outerSerializer.statisticsSerializer, outerSerializer.recordSerializer
            };
        }

        @SuppressWarnings("unchecked")
        @Override
        protected StatisticsOrRecordSerializer createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            TypeSerializer<DataStatistics> statsSerializer =
                    (TypeSerializer<DataStatistics>) nestedSerializers[0];
            TypeSerializer<InternalRow> recordSerializer =
                    (TypeSerializer<InternalRow>) nestedSerializers[1];
            return new StatisticsOrRecordSerializer(statsSerializer, recordSerializer);
        }
    }
}
