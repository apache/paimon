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

package org.apache.paimon.options;

import java.time.Duration;

/** Config of snapshot and changelog expiration. */
public class ExpireConfig {
    private final int snapshotRetainMax;
    private final int snapshotRetainMin;
    private final Duration snapshotTimeRetain;
    private final int snapshotMaxDeletes;
    private final int changelogRetainMax;
    private final int changelogRetainMin;
    private final Duration changelogTimeRetain;
    private final int changelogMaxDeletes;
    private final boolean changelogDecoupled;

    public ExpireConfig(
            int snapshotRetainMax,
            int snapshotRetainMin,
            Duration snapshotTimeRetain,
            int snapshotMaxDeletes,
            int changelogRetainMax,
            int changelogRetainMin,
            Duration changelogTimeRetain,
            int changelogMaxDeletes) {
        this.snapshotRetainMax = snapshotRetainMax;
        this.snapshotRetainMin = snapshotRetainMin;
        this.snapshotTimeRetain = snapshotTimeRetain;
        this.snapshotMaxDeletes = snapshotMaxDeletes;
        this.changelogRetainMax = changelogRetainMax;
        this.changelogRetainMin = changelogRetainMin;
        this.changelogTimeRetain = changelogTimeRetain;
        this.changelogMaxDeletes = changelogMaxDeletes;
        this.changelogDecoupled =
                changelogRetainMax > snapshotRetainMax
                        || changelogRetainMin > snapshotRetainMin
                        || changelogTimeRetain.compareTo(snapshotTimeRetain) > 0;
    }

    public int getSnapshotRetainMax() {
        return snapshotRetainMax;
    }

    public int getSnapshotRetainMin() {
        return snapshotRetainMin;
    }

    public Duration getSnapshotTimeRetain() {
        return snapshotTimeRetain;
    }

    public int getChangelogRetainMax() {
        return changelogRetainMax;
    }

    public int getChangelogRetainMin() {
        return changelogRetainMin;
    }

    public Duration getChangelogTimeRetain() {
        return changelogTimeRetain;
    }

    public int getSnapshotMaxDeletes() {
        return snapshotMaxDeletes;
    }

    public int getChangelogMaxDeletes() {
        return changelogMaxDeletes;
    }

    public boolean isChangelogDecoupled() {
        return changelogDecoupled;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The builder for {@link ExpireConfig}. */
    public static final class Builder {
        private int snapshotRetainMax = Integer.MAX_VALUE;
        private int snapshotRetainMin = 1;
        private Duration snapshotTimeRetain = Duration.ofMillis(Long.MAX_VALUE);
        private int snapshotMaxDeletes = Integer.MAX_VALUE;
        // If changelog config is not set, use the snapshot's by default
        private Integer changelogRetainMax = null;
        private Integer changelogRetainMin = null;
        private Duration changelogTimeRetain = null;
        private Integer changelogMaxDeletes = null;

        public static Builder builder() {
            return new Builder();
        }

        public Builder snapshotRetainMax(int snapshotRetainMax) {
            this.snapshotRetainMax = snapshotRetainMax;
            return this;
        }

        public Builder snapshotRetainMin(int snapshotRetainMin) {
            this.snapshotRetainMin = snapshotRetainMin;
            return this;
        }

        public Builder snapshotTimeRetain(Duration snapshotTimeRetain) {
            this.snapshotTimeRetain = snapshotTimeRetain;
            return this;
        }

        public Builder snapshotMaxDeletes(int snapshotMaxDeletes) {
            this.snapshotMaxDeletes = snapshotMaxDeletes;
            return this;
        }

        public Builder changelogRetainMax(Integer changelogRetainMax) {
            this.changelogRetainMax = changelogRetainMax;
            return this;
        }

        public Builder changelogRetainMin(Integer changelogRetainMin) {
            this.changelogRetainMin = changelogRetainMin;
            return this;
        }

        public Builder changelogTimeRetain(Duration changelogTimeRetain) {
            this.changelogTimeRetain = changelogTimeRetain;
            return this;
        }

        public Builder changelogMaxDeletes(Integer changelogMaxDeletes) {
            this.changelogMaxDeletes = changelogMaxDeletes;
            return this;
        }

        public ExpireConfig build() {
            return new ExpireConfig(
                    snapshotRetainMax,
                    snapshotRetainMin,
                    snapshotTimeRetain,
                    snapshotMaxDeletes,
                    changelogRetainMax == null ? snapshotRetainMax : changelogRetainMax,
                    changelogRetainMin == null ? snapshotRetainMin : changelogRetainMin,
                    changelogTimeRetain == null ? snapshotTimeRetain : changelogTimeRetain,
                    changelogMaxDeletes == null ? snapshotMaxDeletes : changelogMaxDeletes);
        }
    }
}
