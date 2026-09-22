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

package org.apache.paimon.flink.utils;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import javax.annotation.Nullable;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_COVER_ALL_OPERATORS;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.FlinkConnectorOptions.SOURCE_OPERATOR_UID_COVER_ALL_OPERATORS;
import static org.apache.paimon.flink.FlinkConnectorOptions.SOURCE_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.FlinkConnectorOptions.generateCustomUid;

/**
 * Assigns uids to the operators Paimon adds beyond the writer, the committer, the bucket assigner
 * and the source, which carry one of their own.
 *
 * <p>An operator without a uid takes its id from the shape of the stream graph, so a change
 * elsewhere in the job moves the id and orphans the checkpoint entry behind it. Assigning one is
 * opt-in: the table has to set both {@code cover-all-operators} and the matching uid suffix. A job
 * that sets neither builds the same graph as before.
 *
 * <p>A {@code *_NAME} constant passed here is a uid prefix, frozen once released.
 */
public class OperatorUidAssigner {

    private static final OperatorUidAssigner DISABLED = new OperatorUidAssigner(null, null);

    @Nullable private final String tableName;

    @Nullable private final String uidSuffix;

    private OperatorUidAssigner(@Nullable String tableName, @Nullable String uidSuffix) {
        this.tableName = tableName;
        this.uidSuffix = uidSuffix;
    }

    public static OperatorUidAssigner forSink(@Nullable Table table) {
        return create(table, SINK_OPERATOR_UID_COVER_ALL_OPERATORS, SINK_OPERATOR_UID_SUFFIX);
    }

    public static OperatorUidAssigner forSource(@Nullable Table table) {
        return create(table, SOURCE_OPERATOR_UID_COVER_ALL_OPERATORS, SOURCE_OPERATOR_UID_SUFFIX);
    }

    private static OperatorUidAssigner create(
            @Nullable Table table,
            ConfigOption<Boolean> coverAllOperators,
            ConfigOption<String> suffix) {
        if (table == null) {
            return DISABLED;
        }
        Options options = Options.fromMap(table.options());
        String uidSuffix = options.get(suffix);
        if (!options.get(coverAllOperators) || uidSuffix == null) {
            return DISABLED;
        }
        return new OperatorUidAssigner(table.name(), uidSuffix);
    }

    /** Assigns {@code ${uidPrefix}_${tableName}_${uidSuffix}} and returns the operator. */
    public <T> SingleOutputStreamOperator<T> assign(
            SingleOutputStreamOperator<T> operator, String uidPrefix) {
        if (uidSuffix != null) {
            operator.uid(generateCustomUid(uidPrefix, tableName, uidSuffix));
        }
        return operator;
    }

    /** The same for a terminal sink. */
    public <T> DataStreamSink<T> assign(DataStreamSink<T> sink, String uidPrefix) {
        if (uidSuffix != null) {
            sink.uid(generateCustomUid(uidPrefix, tableName, uidSuffix));
        }
        return sink;
    }
}
