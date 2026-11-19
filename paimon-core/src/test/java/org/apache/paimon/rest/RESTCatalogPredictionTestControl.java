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

package org.apache.paimon.rest;

/** Controls prediction execution phases for deterministic REST integration tests. */
final class RESTCatalogPredictionTestControl {

    private final RESTCatalogPredictionHandler handler;

    RESTCatalogPredictionTestControl(RESTCatalogPredictionHandler handler) {
        this.handler = handler;
    }

    Runnable pauseExecution() {
        handler.pauseExecution();
        return handler::resumeExecution;
    }

    void synchronizeNextSubmissions(int parties) {
        handler.synchronizeNextSubmissions(parties);
    }

    Runnable pauseAfterOutputCreated() {
        handler.pauseAfterOutputCreated();
        return handler::resumeAfterOutputCreated;
    }

    void awaitOutputCreated() throws InterruptedException {
        handler.awaitOutputCreated();
    }

    Runnable pauseAfterOutputCommitted() {
        handler.pauseAfterOutputCommitted();
        return handler::resumeAfterOutputCommitted;
    }

    void awaitOutputCommitted() throws InterruptedException {
        handler.awaitOutputCommitted();
    }
}
