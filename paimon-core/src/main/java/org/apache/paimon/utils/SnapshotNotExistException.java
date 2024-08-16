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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;

/** Thrown by TagManager or SnapshotManager when the specified snapshotId does not exist. */
public class SnapshotNotExistException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SnapshotNotExistException(long snapshotId) {
        super("Snapshot " + snapshotId + " does not exist");
    }

    public SnapshotNotExistException(String errMsg) {
        super(errMsg);
    }

    public static void checkNotNull(Snapshot snapshotId, String errMsg) {
        if (snapshotId == null) {
            throw new SnapshotNotExistException(errMsg);
        }
    }
}
