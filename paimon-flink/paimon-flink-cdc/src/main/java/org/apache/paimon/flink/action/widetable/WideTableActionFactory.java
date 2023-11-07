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

package org.apache.paimon.flink.action.widetable;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.widetable.utils.options.DimensionOptions;
import org.apache.paimon.flink.widetable.utils.options.JobOptions;
import org.apache.paimon.flink.widetable.utils.options.SinkOptions;
import org.apache.paimon.flink.widetable.utils.options.SourceOptions;
import org.apache.paimon.flink.widetable.utils.options.SqlInfoOptions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Optional;

/** Factory to create {@link WideTableAction}. */
public class WideTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "wide-table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        checkRequiredArgument(params, JobOptions.PRE_NAME);
        checkRequiredArgument(params, SourceOptions.PRE_NAME);
        checkRequiredArgument(params, DimensionOptions.PRE_NAME);
        checkRequiredArgument(params, SqlInfoOptions.PRE_NAME);
        checkRequiredArgument(params, SinkOptions.PRE_NAME);
        Tuple3<String, String, String> tablePath = getTablePath(params);

        WideTableAction action =
                new WideTableAction(
                        tablePath.f0,
                        tablePath.f1,
                        tablePath.f2,
                        optionalConfigMap(params, "catalog-conf"));

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"wide-table\" creates a streaming job.");
        System.out.println();
    }
}
