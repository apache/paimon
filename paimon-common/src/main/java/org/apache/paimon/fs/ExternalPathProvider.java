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

package org.apache.paimon.fs;

import org.apache.paimon.CoreOptions.ExternalPathStrategy;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/** Provider for external data paths. */
public interface ExternalPathProvider extends Serializable {

    Path getNextExternalDataPath(String fileName);

    @Nullable
    static ExternalPathProvider create(
            ExternalPathStrategy strategy, List<Path> externalTablePaths, Path relativeBucketPath) {
        switch (strategy) {
            case ENTROPY_INJECT:
                return new EntropyInjectExternalPathProvider(
                        externalTablePaths, relativeBucketPath);
            case SPECIFIC_FS:
                // specific fs can use round-robin with only one path
            case ROUND_ROBIN:
                return new RoundRobinExternalPathProvider(externalTablePaths, relativeBucketPath);
            case NONE:
                return null;
            default:
                throw new UnsupportedOperationException(
                        "Cannot support create external path provider for: " + strategy);
        }
    }
}
