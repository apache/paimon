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

import org.apache.paimon.annotation.Experimental;

/** How source-side table changes are handled when merging database references. */
@Experimental
public enum MergeMode {
    /** Merge changes relative to the common ancestor, failing on conflicting table versions. */
    NORMAL,

    /** Accept source-side changes even when they conflict with the target table version. */
    FORCE,

    /** Skip all source-side changes to the table, preserving its target state. */
    DROP
}
