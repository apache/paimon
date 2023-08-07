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

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

package org.apache.paimon.client;

/**
 * Client Pool to accept.
 *
 * <p>Most of the content of this class is referenced from Iceberg's ClientPool.
 */
public interface ClientPool<C, E extends Exception> {
    /** Copied from iceberg. */
    interface Action<R, C, E extends Exception> {
        R run(C client) throws E;
    }

    <R> R run(Action<R, C, E> action) throws E, InterruptedException;

    <R> R run(Action<R, C, E> action, boolean retry) throws E, InterruptedException;
}
