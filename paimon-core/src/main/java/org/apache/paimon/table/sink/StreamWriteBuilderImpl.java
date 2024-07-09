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

package org.apache.paimon.table.sink;

import org.apache.paimon.options.Options;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.RowType;

import java.util.Optional;

import static org.apache.paimon.CoreOptions.createCommitUser;

/** Implementation for {@link WriteBuilder}. */
public class StreamWriteBuilderImpl implements StreamWriteBuilder {

    private static final long serialVersionUID = 1L;

    private final InnerTable table;

    private String commitUser;

    public StreamWriteBuilderImpl(InnerTable table) {
        this.table = table;
        this.commitUser = createCommitUser(new Options(table.options()));
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
    public Optional<WriteSelector> newWriteSelector() {
        return table.newWriteSelector();
    }

    @Override
    public String commitUser() {
        return commitUser;
    }

    @Override
    public StreamWriteBuilder withCommitUser(String commitUser) {
        this.commitUser = commitUser;
        return this;
    }

    @Override
    public StreamTableWrite newWrite() {
        return table.newWrite(commitUser);
    }

    @Override
    public StreamTableCommit newCommit() {
        return table.newCommit(commitUser).ignoreEmptyCommit(false);
    }
}
