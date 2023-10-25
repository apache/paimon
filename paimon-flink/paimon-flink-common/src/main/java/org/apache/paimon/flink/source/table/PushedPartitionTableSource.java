/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.flink.source.table;

import org.apache.paimon.flink.source.FlinkTableSource;

import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A {@link RichTableSource} with partition push down. */
public class PushedPartitionTableSource extends PushedRichTableSource
        implements SupportsPartitionPushDown {

    public PushedPartitionTableSource(FlinkTableSource source) {
        super(source);
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        return source.listPartitions();
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        source.applyPartition(remainingPartitions);
    }

    @Override
    public PushedRichTableSource copy() {
        return new PushedPartitionTableSource(source);
    }
}
