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

package org.apache.paimon.operation.expire;

import org.apache.paimon.Snapshot;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Immutable protection set for parallel expire snapshots. This class holds all the information
 * needed to protect files that should not be deleted during expiration.
 *
 * <p>This class is broadcast to all workers and must be treated as read-only.
 *
 * <p>Note: Tag data files are loaded on-demand by workers using {@link
 * org.apache.paimon.operation.SnapshotDeletion#createDataFileSkipperForTags}, which has internal
 * caching to avoid repeated reads.
 */
public class ProtectionSet implements Serializable {

    private static final long serialVersionUID = 1L;

    /** All tagged snapshots, sorted by snapshot ID. */
    private final List<Snapshot> taggedSnapshots;

    /** Manifest file names that should be protected (from Tags and endSnapshot). */
    private final Set<String> manifestSkippingSet;

    public ProtectionSet(List<Snapshot> taggedSnapshots, Set<String> manifestSkippingSet) {
        this.taggedSnapshots = Collections.unmodifiableList(taggedSnapshots);
        this.manifestSkippingSet =
                manifestSkippingSet == null
                        ? null
                        : Collections.unmodifiableSet(manifestSkippingSet);
    }

    public List<Snapshot> taggedSnapshots() {
        return taggedSnapshots;
    }

    public Set<String> manifestSkippingSet() {
        return manifestSkippingSet;
    }
}
