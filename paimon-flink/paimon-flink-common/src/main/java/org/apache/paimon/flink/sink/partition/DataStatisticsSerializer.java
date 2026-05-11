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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Map;

/** Serializer for {@link DataStatistics}. */
public class DataStatisticsSerializer extends TypeSerializer<DataStatistics> {

    private static final long serialVersionUID = 1L;

    private final MapSerializer<String, Long> mapSerializer;

    public DataStatisticsSerializer() {
        this.mapSerializer =
                new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<DataStatistics> duplicate() {
        return new DataStatisticsSerializer();
    }

    @Override
    public DataStatistics createInstance() {
        return new DataStatistics();
    }

    @Override
    public DataStatistics copy(DataStatistics from) {
        return new DataStatistics(new java.util.HashMap<>(from.result()));
    }

    @Override
    public DataStatistics copy(DataStatistics from, DataStatistics reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(DataStatistics record, DataOutputView target) throws IOException {
        mapSerializer.serialize(record.result(), target);
    }

    @Override
    public DataStatistics deserialize(DataInputView source) throws IOException {
        Map<String, Long> partitionFrequency = mapSerializer.deserialize(source);
        return new DataStatistics(partitionFrequency);
    }

    @Override
    public DataStatistics deserialize(DataStatistics reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public TypeSerializerSnapshot<DataStatistics> snapshotConfiguration() {
        return new DataStatisticsSerializerSnapshot(this);
    }

    /** Snapshot class for the {@link DataStatisticsSerializer}. */
    public static class DataStatisticsSerializerSnapshot
            extends CompositeTypeSerializerSnapshot<DataStatistics, DataStatisticsSerializer> {
        private static final int CURRENT_VERSION = 1;

        @SuppressWarnings("unused")
        public DataStatisticsSerializerSnapshot() {}

        public DataStatisticsSerializerSnapshot(DataStatisticsSerializer serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return CURRENT_VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                DataStatisticsSerializer outerSerializer) {
            return new TypeSerializer<?>[0];
        }

        @Override
        protected DataStatisticsSerializer createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            return new DataStatisticsSerializer();
        }
    }
}
