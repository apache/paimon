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

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.COMPUTED_COLUMN;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.METADATA_COLUMN;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.PARTITION_KEYS;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.PRIMARY_KEYS;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TYPE_MAPPING;

/** Base {@link ActionFactory} for synchronizing into one Paimon table. */
public abstract class SyncTableActionFactoryBase
        extends SynchronizationActionFactoryBase<SyncTableActionBase> {

    protected Tuple3<String, String, String> tablePath;

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        this.tablePath = getTablePath(params);
        return super.create(params);
    }

    @Override
    protected void withParams(MultipleParameterToolAdapter params, SyncTableActionBase action) {
        if (params.has(PARTITION_KEYS)) {
            action.withPartitionKeys(params.get(PARTITION_KEYS).split(","));
        }

        if (params.has(PRIMARY_KEYS)) {
            action.withPrimaryKeys(params.get(PRIMARY_KEYS).split(","));
        }

        if (params.has(COMPUTED_COLUMN)) {
            action.withComputedColumnArgs(
                    new ArrayList<>(params.getMultiParameter(COMPUTED_COLUMN)));
        }

        if (params.has(METADATA_COLUMN)) {
            List<String> metadataColumns =
                    new ArrayList<>(params.getMultiParameter(METADATA_COLUMN));
            if (metadataColumns.size() == 1) {
                action.withMetadataColumns(Arrays.asList(metadataColumns.get(0).split(",")));
            } else {
                action.withMetadataColumns(metadataColumns);
            }
        }

        if (params.has(TYPE_MAPPING)) {
            String[] options = params.get(TYPE_MAPPING).split(",");
            action.withTypeMapping(TypeMapping.parse(options));
        }
    }
}
