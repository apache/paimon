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

import org.apache.flink.table.store.annotation.Experimental;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.types.RowType;

import java.io.Serializable;

/**
 * An interface for building the {@link TableWrite} and {@link TableCommit}.
 *
 * @since 0.4.0
 */
@Experimental
public interface WriteBuilder extends Serializable {

    /** A name to identify this table. */
    String tableName();

    /** Returns the row type of this table. */
    RowType rowType();

    /** Create a {@link TableWrite} to write {@link InternalRow}s. */
    TableWrite newWrite();

    /** Create a {@link TableCommit} to commit {@link CommitMessage}s. */
    TableCommit newCommit();
}
