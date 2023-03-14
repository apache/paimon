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

package org.apache.flink.table.store.file.operation;

/**
 * Operations implementing this interface can extract and recover their states between different
 * instances.
 *
 * @param <S> type of state
 */
public interface Recoverable<S> {

    /**
     * Extract state of the current operation instance. After calling this method, the operation
     * instance must be closed and should no longer be used.
     */
    S extractStateAndClose() throws Exception;

    /**
     * Recover state of a previous operation instance into the current operation instance. This
     * method must be called right after the current instance is constructed.
     */
    void recoverFromState(S state);
}
