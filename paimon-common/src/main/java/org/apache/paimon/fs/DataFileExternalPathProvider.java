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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/** Provider for external data paths. */
public class DataFileExternalPathProvider implements Serializable {
    @Nullable private final TableExternalPathProvider tableExternalPathProvider;
    private final Path relativeBucketPath;

    public DataFileExternalPathProvider(
            @Nullable TableExternalPathProvider tableExternalPathProvider,
            Path relativeBucketPath) {
        this.tableExternalPathProvider = tableExternalPathProvider;
        this.relativeBucketPath = relativeBucketPath;
    }

    /**
     * Get the next external data path.
     *
     * @return the next external data path
     */
    public Optional<Path> getNextExternalDataPath() {
        return Optional.ofNullable(tableExternalPathProvider)
                .flatMap(TableExternalPathProvider::getNextExternalPath)
                .map(path -> new Path(path, relativeBucketPath));
    }

    public boolean externalPathExists() {
        return tableExternalPathProvider != null && tableExternalPathProvider.externalPathExists();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataFileExternalPathProvider)) {
            return false;
        }

        DataFileExternalPathProvider that = (DataFileExternalPathProvider) o;
        return Objects.equals(tableExternalPathProvider, that.tableExternalPathProvider)
                && Objects.equals(relativeBucketPath, that.relativeBucketPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableExternalPathProvider, relativeBucketPath);
    }

    @Override
    public String toString() {
        return "DataFileExternalPathProvider{"
                + " externalPathProvider="
                + tableExternalPathProvider
                + ", relativeBucketPath="
                + relativeBucketPath
                + "}";
    }
}
