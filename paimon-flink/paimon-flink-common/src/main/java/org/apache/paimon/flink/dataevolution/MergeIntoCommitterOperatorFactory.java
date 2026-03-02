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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.util.Set;

/** Factory for {@link MergeIntoCommitterOperator}. */
public class MergeIntoCommitterOperatorFactory extends AbstractStreamOperatorFactory<Committable>
        implements OneInputStreamOperatorFactory<Committable, Committable> {

    private final FileStoreTable table;

    private final Set<String> updatedColumns;

    public MergeIntoCommitterOperatorFactory(FileStoreTable table, Set<String> updatedColumns) {
        this.table = table;
        this.updatedColumns = updatedColumns;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Committable>> T createStreamOperator(
            StreamOperatorParameters<Committable> parameters) {
        return (T) new MergeIntoCommitterOperator(parameters, table, updatedColumns);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return MergeIntoCommitterOperator.class;
    }
}
