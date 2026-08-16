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

import org.apache.paimon.data.BinaryRow;

import java.util.Map;

/** Information to read the file. */
public class ChainReadContext {

    private final BinaryRow logicalPartition;
    private final Map<String, String> fileBucketPathMapping;
    private final Map<String, String> fileBranchMapping;

    private ChainReadContext(
            BinaryRow readPartition,
            Map<String, String> fileBucketPathMapping,
            Map<String, String> fileBranchMapping) {
        this.logicalPartition = readPartition;
        this.fileBucketPathMapping = fileBucketPathMapping;
        this.fileBranchMapping = fileBranchMapping;
    }

    public BinaryRow logicalPartition() {
        return logicalPartition;
    }

    public Map<String, String> fileBucketPathMapping() {
        return fileBucketPathMapping;
    }

    public Map<String, String> fileBranchMapping() {
        return fileBranchMapping;
    }

    /** Builder for {@link ChainReadContext}. */
    public static class Builder {
        private BinaryRow logicalPartition;

        private Map<String, String> fileBucketPathMapping;

        private Map<String, String> fileBranchPathMapping;

        public ChainReadContext.Builder withLogicalPartition(BinaryRow logicalPartition) {
            this.logicalPartition = logicalPartition;
            return this;
        }

        public ChainReadContext.Builder withFileBucketPathMapping(
                Map<String, String> fileBucketPathMapping) {
            this.fileBucketPathMapping = fileBucketPathMapping;
            return this;
        }

        public ChainReadContext.Builder withFileBranchPathMapping(
                Map<String, String> fileBranchPathMapping) {
            this.fileBranchPathMapping = fileBranchPathMapping;
            return this;
        }

        public ChainReadContext build() {
            return new ChainReadContext(
                    logicalPartition, fileBucketPathMapping, fileBranchPathMapping);
        }
    }
}
