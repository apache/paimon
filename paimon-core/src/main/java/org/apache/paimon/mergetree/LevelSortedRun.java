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

package org.apache.paimon.mergetree;

import java.util.Objects;

/** {@link SortedRun} with level. */
public class LevelSortedRun {

    private final int level;

    private final SortedRun run;

    public LevelSortedRun(int level, SortedRun run) {
        this.level = level;
        this.run = run;
    }

    public int level() {
        return level;
    }

    public SortedRun run() {
        return run;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LevelSortedRun that = (LevelSortedRun) o;
        return level == that.level && Objects.equals(run, that.run);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, run);
    }

    @Override
    public String toString() {
        return "LevelSortedRun{" + "level=" + level + ", run=" + run + '}';
    }
}
