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

package org.apache.paimon.flink.action;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Factory to create {@link CompactDuplicatedManifestsAction}. */
public class CompactDuplicatedManifestsActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "compact_duplicated_manifests";
    private static final String PARALLELISM = "parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        CompactDuplicatedManifestsAction action =
                new CompactDuplicatedManifestsAction(
                        params.getRequired(DATABASE),
                        params.getRequired(TABLE),
                        catalogConfigMap(params));

        if (params.has(PARTITION)) {
            List<Map<String, String>> partitions = getPartitions(params);
            action.withPartitions(partitions);
        }

        if (params.has(PARALLELISM)) {
            action.withParallelism(Integer.parseInt(params.get(PARALLELISM)));
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        // TODO
    }
}
