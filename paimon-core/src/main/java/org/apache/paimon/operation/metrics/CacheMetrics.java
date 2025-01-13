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

package org.apache.paimon.operation.metrics;

import java.util.concurrent.atomic.AtomicLong;

/** Metrics for manifest file cache of a caching catalog. */
public class CacheMetrics {

    private final AtomicLong hitObject;
    private final AtomicLong missedObject;

    public CacheMetrics() {
        this.hitObject = new AtomicLong(0);
        this.missedObject = new AtomicLong(0);
    }

    public AtomicLong getHitObject() {
        return hitObject;
    }

    public void increaseHitObject() {
        this.hitObject.incrementAndGet();
    }

    public AtomicLong getMissedObject() {
        return missedObject;
    }

    public void increaseMissedObject() {
        this.missedObject.incrementAndGet();
    }
}
