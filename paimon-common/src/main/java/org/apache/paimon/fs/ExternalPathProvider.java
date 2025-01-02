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

import org.apache.paimon.CoreOptions.ExternalFSStrategy;
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.annotation.VisibleForTesting;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Provider for external paths. */
public class ExternalPathProvider implements Serializable {
    private final Map<ExternalFSStrategy, Path> externalPathsMap;
    private final List<Path> externalPathsList;

    private final ExternalPathStrategy externalPathStrategy;
    private final ExternalFSStrategy externalFSStrategy;
    private int currentIndex;
    private boolean externalPathExists;
    private final String dbAndTableRelativePath;

    @VisibleForTesting
    public ExternalPathProvider() {
        this.externalPathsMap = new HashMap<>();
        this.externalPathsList = new ArrayList<>();
        this.externalPathStrategy = ExternalPathStrategy.NONE;
        this.externalFSStrategy = null;
        this.dbAndTableRelativePath = null;
    }

    public ExternalPathProvider(
            String externalPaths,
            ExternalPathStrategy externalPathStrategy,
            ExternalFSStrategy externalFSStrategy,
            String dbAndTableRelativePath) {
        this.externalPathsMap = new HashMap<>();
        this.externalPathsList = new ArrayList<>();
        this.externalPathStrategy = externalPathStrategy;
        this.externalFSStrategy = externalFSStrategy;
        this.dbAndTableRelativePath = dbAndTableRelativePath;
        this.currentIndex = 0;
        initExternalPaths(externalPaths);
    }

    private void initExternalPaths(String externalPaths) {
        if (externalPaths == null) {
            return;
        }
        String[] tmpArray = externalPaths.split(",");
        for (String part : tmpArray) {
            String path = part.trim();
            if (path.toLowerCase().startsWith("oss")) {
                externalPathsMap.put(ExternalFSStrategy.OSS, new Path(path));
                externalPathsList.add(new Path(path));
            } else if (path.toLowerCase().startsWith("s3")) {
                externalPathsMap.put(ExternalFSStrategy.S3, new Path(path));
                externalPathsList.add(new Path(path));
            } else {
                throw new IllegalArgumentException("Unsupported external path: " + path);
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
        switch (externalFSStrategy) {
            case S3:
                return Optional.of(
                        new Path(
                                externalPathsMap.get(ExternalFSStrategy.S3),
                                dbAndTableRelativePath));
            case OSS:
                return Optional.of(
                        new Path(
                                externalPathsMap.get(ExternalFSStrategy.OSS),
                                dbAndTableRelativePath));
            default:
                throw new IllegalArgumentException(
                        "Unsupported external fs strategy: " + externalFSStrategy);
        }
    }

    private Optional<Path> getRoundRobinPath() {
        currentIndex = (currentIndex + 1) % externalPathsList.size();
        return Optional.of(new Path(externalPathsList.get(currentIndex), dbAndTableRelativePath));
    }

    public boolean externalPathExists() {
        return externalPathExists;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExternalPathProvider that = (ExternalPathProvider) o;
        return currentIndex == that.currentIndex
                && externalPathExists == that.externalPathExists
                && externalPathsMap.equals(that.externalPathsMap)
                && externalPathsList.equals(that.externalPathsList)
                && externalPathStrategy == that.externalPathStrategy
                && externalFSStrategy == that.externalFSStrategy
                && Objects.equals(dbAndTableRelativePath, that.dbAndTableRelativePath);
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
                + ", externalFSStrategy="
                + externalFSStrategy
                + ", currentIndex="
                + currentIndex
                + ", externalPathExists="
                + externalPathExists
                + ", dbAndTableRelativePath='"
                + dbAndTableRelativePath
                + '\''
                + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                externalPathsMap,
                externalPathsList,
                externalPathStrategy,
                externalFSStrategy,
                currentIndex,
                externalPathExists,
                dbAndTableRelativePath);
    }
}
