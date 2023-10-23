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

/** Abstract class for Flink actions. */
public interface Action {

    /** The execution method of the action. */
    void run() throws Exception;

    /**
     * Builds the action within the given Flink Stream Execution Environment.
     *
     * <p>This method is responsible for setting up any necessary configurations or resources needed
     * for the action to run. It is called before the `run` method to prepare the environment for
     * execution.
     *
     * <p>By default, this method is empty and can be overridden by subclasses to provide custom
     * setup logic.
     *
     * @throws Exception If an error occurs during the build process.
     */
    default void build() throws Exception {}
}
