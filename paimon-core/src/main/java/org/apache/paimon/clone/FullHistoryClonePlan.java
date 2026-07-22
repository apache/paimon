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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Immutable plan for a full-history clone. */
public class FullHistoryClonePlan implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Path sourceRoot;
    private final Path targetRoot;
    private final String sourceFingerprint;
    private final List<Path> externalTargetRoots;

    FullHistoryClonePlan(
            Path sourceRoot,
            Path targetRoot,
            String sourceFingerprint,
            List<Path> externalTargetRoots) {
        this.sourceRoot = sourceRoot;
        this.targetRoot = targetRoot;
        this.sourceFingerprint = sourceFingerprint;
        this.externalTargetRoots =
                Collections.unmodifiableList(new ArrayList<>(externalTargetRoots));
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

    public List<Path> externalTargetRoots() {
        return externalTargetRoots;
    }

    public void validatePayloadTarget(Path target) {
        boolean owned = PathMapping.isSameOrDescendant(target.toString(), targetRoot.toString());
        if (!owned) {
            for (Path externalRoot : externalTargetRoots) {
                if (PathMapping.isSameOrDescendant(target.toString(), externalRoot.toString())) {
                    owned = true;
                    break;
                }
            }
        }
        checkArgument(
                owned, "Payload target is outside full-history clone owned roots: %s", target);
        validateNotControlPath(targetRoot, target);
        for (Path externalRoot : externalTargetRoots) {
            validateNotControlPath(externalRoot, target);
        }
    }

    private static void validateNotControlPath(Path root, Path target) {
        Path marker = new Path(root, FullHistoryCloneMarker.FILE_NAME);
        Path success = new Path(root, FullHistoryCloneMarker.SUCCESS_FILE_NAME);
        checkArgument(
                !PathMapping.isSameOrDescendant(target.toString(), marker.toString())
                        && !PathMapping.isSameOrDescendant(target.toString(), success.toString()),
                "Payload target uses a reserved control namespace: %s",
                target);
    }
}
