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

package org.apache.paimon.io;

import org.apache.paimon.annotation.Experimental;

/**
 * Opt-in extension for replayable bundles that can preserve their bundle type under projection.
 *
 * <p>This allows projection-aware writers to keep format-specific bundle fast-paths without
 * coupling to concrete bundle implementations from other modules.
 */
@Experimental
public interface ProjectableBundleRecords extends ReplayableBundleRecords {

    /** Returns a replayable bundle that exposes the projected fields in projection order. */
    ReplayableBundleRecords project(int[] projection);
}
