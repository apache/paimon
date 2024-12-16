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

import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.api.java.tuple.Tuple3;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Factory to create {@link ReplaceTagAction} or {@link ReplaceTagAction}. */
public abstract class CreateOrReplaceTagActionFactory implements ActionFactory {

    private static final String TAG_NAME = "tag_name";
    private static final String SNAPSHOT = "snapshot";
    private static final String TIME_RETAINED = "time_retained";

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        checkRequiredArgument(params, TAG_NAME);

        Tuple3<String, String, String> tablePath = getTablePath(params);
        Map<String, String> catalogConfig = optionalConfigMap(params, CATALOG_CONF);
        String tagName = params.get(TAG_NAME);

        Long snapshot = null;
        if (params.has(SNAPSHOT)) {
            snapshot = Long.parseLong(params.get(SNAPSHOT));
        }

        Duration timeRetained = null;
        if (params.has(TIME_RETAINED)) {
            timeRetained = TimeUtils.parseDuration(params.get(TIME_RETAINED));
        }

        return Optional.of(
                createOrReplaceTagAction(
                        tablePath, catalogConfig, tagName, snapshot, timeRetained));
    }

    abstract Action createOrReplaceTagAction(
            Tuple3<String, String, String> tablePath,
            Map<String, String> catalogConfig,
            String tagName,
            Long snapshot,
            Duration timeRetained);
}
