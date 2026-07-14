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

package org.apache.paimon.clone;

import org.apache.paimon.fs.Path;

import java.io.Serializable;

/** Immutable plan for a full-history clone. */
public class FullHistoryClonePlan implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Path sourceRoot;
    private final Path targetRoot;
    private final String sourceFingerprint;
    private final FullHistoryCopyPlan payloadCopyPlan;

    FullHistoryClonePlan(
            Path sourceRoot,
            Path targetRoot,
            String sourceFingerprint,
            FullHistoryCopyPlan payloadCopyPlan) {
        this.sourceRoot = sourceRoot;
        this.targetRoot = targetRoot;
        this.sourceFingerprint = sourceFingerprint;
        this.payloadCopyPlan = payloadCopyPlan;
    }

    public Path sourceRoot() {
        return sourceRoot;
    }

    public Path targetRoot() {
        return targetRoot;
    }

    public String sourceFingerprint() {
        return sourceFingerprint;
    }

    public FullHistoryCopyPlan payloadCopyPlan() {
        return payloadCopyPlan;
    }
}
