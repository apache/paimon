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
import org.apache.paimon.annotation.VisibleForTesting;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

/** Provider for external paths. */
public class TableExternalPathProvider implements Serializable {
    private final Map<String, Path> externalPathsMap;
    private final List<Path> externalPathsList;

    private final ExternalPathStrategy externalPathStrategy;
    private final String externalSpecificFS;
    private int currentIndex = 0;
    private boolean externalPathExists;

    public TableExternalPathProvider(
            String externalPaths,
            ExternalPathStrategy externalPathStrategy,
            String externalSpecificFS) {
        this.externalPathsMap = new HashMap<>();
        this.externalPathsList = new ArrayList<>();
        this.externalPathStrategy = externalPathStrategy;
        if (externalSpecificFS != null) {
            this.externalSpecificFS = externalSpecificFS.toLowerCase();
        } else {
            this.externalSpecificFS = null;
        }
        initExternalPaths(externalPaths);
        if (!externalPathsList.isEmpty()) {
            this.currentIndex = new Random().nextInt(externalPathsList.size());
        }
    }

    private void initExternalPaths(String externalPaths) {
        if (externalPaths == null) {
            return;
        }

        String[] tmpArray = externalPaths.split(",");
        for (String s : tmpArray) {
            Path path = new Path(s.trim());
            String scheme = path.toUri().getScheme();
            if (scheme == null) {
                throw new IllegalArgumentException("scheme should not be null: " + path);
            }
            scheme = scheme.toLowerCase();
            externalPathsMap.put(scheme, path);
            externalPathsList.add(path);
        }

        if (externalPathStrategy != null
                && externalPathStrategy.equals(ExternalPathStrategy.SPECIFIC_FS)) {
            if (externalSpecificFS == null) {
                throw new IllegalArgumentException("external specific fs should not be null: ");
            }

            if (!externalPathsMap.containsKey(externalSpecificFS)) {
                throw new IllegalArgumentException(
                        "external specific fs not found: " + externalSpecificFS);
            }
        }

        if (!externalPathsMap.isEmpty()
                && !externalPathsList.isEmpty()
                && externalPathStrategy != ExternalPathStrategy.NONE) {
            externalPathExists = true;
        }
    }

    /**
     * Get the next external path.
     *
     * @return the next external path
     */
    public Optional<Path> getNextExternalPath() {
        if (externalPathsMap == null || externalPathsMap.isEmpty()) {
            return Optional.empty();
        }

        switch (externalPathStrategy) {
            case NONE:
                return Optional.empty();
            case SPECIFIC_FS:
                return getSpecificFSExternalPath();
            case ROUND_ROBIN:
                return getRoundRobinPath();
            default:
                return Optional.empty();
        }
    }

    private Optional<Path> getSpecificFSExternalPath() {
        if (!externalPathsMap.containsKey(externalSpecificFS)) {
            return Optional.empty();
        }
        return Optional.of(externalPathsMap.get(externalSpecificFS));
    }

    private Optional<Path> getRoundRobinPath() {
        currentIndex = (currentIndex + 1) % externalPathsList.size();
        return Optional.of(externalPathsList.get(currentIndex));
    }

    public boolean externalPathExists() {
        return externalPathExists;
    }

    @VisibleForTesting
    public Map<String, Path> getExternalPathsMap() {
        return externalPathsMap;
    }

    @VisibleForTesting
    public List<Path> getExternalPathsList() {
        return externalPathsList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableExternalPathProvider that = (TableExternalPathProvider) o;
        return currentIndex == that.currentIndex
                && externalPathExists == that.externalPathExists
                && externalPathsMap.equals(that.externalPathsMap)
                && externalPathsList.equals(that.externalPathsList)
                && externalPathStrategy == that.externalPathStrategy
                && Objects.equals(externalSpecificFS, that.externalSpecificFS);
    }

    @Override
    public String toString() {
        return "ExternalPathProvider{"
                + " externalPathsMap="
                + externalPathsMap
                + ", externalPathsList="
                + externalPathsList
                + ", externalPathStrategy="
                + externalPathStrategy
                + ", externalSpecificFS='"
                + externalSpecificFS
                + '\''
                + ", currentIndex="
                + currentIndex
                + ", externalPathExists="
                + externalPathExists
                + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                externalPathsMap,
                externalPathsList,
                externalPathStrategy,
                externalSpecificFS,
                currentIndex,
                externalPathExists);
    }
}
