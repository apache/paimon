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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.store.table.InnerTable;
import org.apache.flink.table.store.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Implementation for {@link WriteBuilder}. */
public class BatchWriteBuilderImpl implements BatchWriteBuilder {

    private static final long serialVersionUID = 1L;

    private final InnerTable table;
    private final String commitUser = UUID.randomUUID().toString();

    private List<Map<String, String>> staticPartitions;

    public BatchWriteBuilderImpl(InnerTable table) {
        this.table = table;
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType rowType() {
        return table.rowType();
    }

    @Override
    public BatchWriteBuilder withOverwrite(@Nullable List<Map<String, String>> staticPartitions) {
        this.staticPartitions = staticPartitions;
        return this;
    }

    @Override
    public BatchTableWrite newWrite() {
        return table.newWrite(commitUser).withOverwrite(staticPartitions != null);
    }

    @Override
    public BatchTableCommit newCommit() {
        InnerTableCommit commit = table.newCommit(commitUser).withOverwrite(staticPartitions);
        commit.ignoreEmptyCommit(true);
        return commit;
    }
}
