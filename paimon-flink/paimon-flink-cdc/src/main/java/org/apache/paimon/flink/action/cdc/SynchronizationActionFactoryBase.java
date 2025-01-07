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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Base {@link ActionFactory} for table/database synchronizing job. */
public abstract class SynchronizationActionFactoryBase<T extends SynchronizationActionBase>
        implements ActionFactory {

    protected Map<String, String> catalogConfig;
    protected Map<String, String> cdcSourceConfig;

    protected abstract String cdcConfigIdentifier();

    public abstract T createAction();

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        checkArgument(
                params.has(cdcConfigIdentifier()),
                "Argument '%s' is required. Run '<action> --help' for help.",
                cdcConfigIdentifier());
        this.catalogConfig = catalogConfigMap(params);
        this.cdcSourceConfig = optionalConfigMap(params, cdcConfigIdentifier());

        T action = createAction();

        action.withTableConfig(optionalConfigMap(params, TABLE_CONF));
        withParams(params, action);

        return Optional.of(action);
    }

    protected abstract void withParams(MultipleParameterToolAdapter params, T action);
}
