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

package org.apache.paimon.flink.sink;

import org.apache.paimon.append.MultiTableUnawareAppendCompactionTask;
import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.table.sink.MultiTableCompactionTaskSerializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;

/** Type information of {@link MultiTableUnawareAppendCompactionTask}. */
public class MultiTableCompactionTaskTypeInfo
        extends TypeInformation<MultiTableUnawareAppendCompactionTask> {
    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<MultiTableUnawareAppendCompactionTask> getTypeClass() {
        return MultiTableUnawareAppendCompactionTask.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<MultiTableUnawareAppendCompactionTask> createSerializer(
            ExecutionConfig executionConfig) {
        return new SimpleVersionedSerializerTypeSerializerProxy<
                MultiTableUnawareAppendCompactionTask>(
                () -> new VersionedSerializerWrapper<>(new MultiTableCompactionTaskSerializer())) {
            @Override
            public MultiTableUnawareAppendCompactionTask copy(
                    MultiTableUnawareAppendCompactionTask from) {
                return from;
            }

            @Override
            public MultiTableUnawareAppendCompactionTask copy(
                    MultiTableUnawareAppendCompactionTask from,
                    MultiTableUnawareAppendCompactionTask reuse) {
                return from;
            }
        };
    }

    @Override
    public String toString() {
        return "MultiTableAppendOnlyCompactionTask";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof MultiTableUnawareAppendCompactionTask;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object o) {
        return o instanceof MultiTableUnawareAppendCompactionTask;
    }
}
