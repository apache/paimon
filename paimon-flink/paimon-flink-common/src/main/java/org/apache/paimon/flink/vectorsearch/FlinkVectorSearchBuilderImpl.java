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

package org.apache.paimon.flink.vectorsearch;

import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.source.VectorRead;
import org.apache.paimon.table.source.VectorSearchBuilderImpl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Flink-aware vector-search builder which creates distributed Flink readers. */
public class FlinkVectorSearchBuilderImpl extends VectorSearchBuilderImpl {

    private static final long serialVersionUID = 1L;

    private final transient StreamExecutionEnvironment env;

    public FlinkVectorSearchBuilderImpl(InnerTable table, StreamExecutionEnvironment env) {
        super(table);
        this.env = checkNotNull(env);
    }

    @Override
    public VectorRead newVectorRead() {
        checkNotNull(vector, "vector must be set via withVector()");
        if (isPrimaryKeyVectorSearch()) {
            return new FlinkPrimaryKeyVectorRead(
                    table, vectorColumn, vector, limit, options, filter, env);
        }
        return new FlinkDataEvolutionVectorRead(
                table, partitionFilter, filter, limit, vectorColumn, vector, options, env);
    }
}
